"""
payment_processor.py — Transitions payments through their lifecycle.

Two jobs each tick:

1. Process PENDING payments:
     ~96% → captured (with paid_at, processor_ref)
     ~4%  → refunded WITH a failure reason note, AND order auto-cancels
            (single transaction: payment + order + status event)

2. Process REFUNDS for cancelled orders:
     captured payments belonging to cancelled orders → refunded
     (only handles orders cancelled by other processes — e.g., the customer
      cancellation flow in order_lifecycle.py)

Failure policy: a "failed" payment does not produce a 'failed' status in our
data. Instead, the payment goes straight to 'refunded' (no money charged)
and the order is cancelled in the same transaction, with the failure reason
recorded in order_status_events.notes. This keeps the data consistent —
no orders left hanging with failed payments.

Refund policy: payment_processor handles ALL refunds — both:
  - "payment failed" refunds (immediate, this process)
  - "customer cancelled" refunds (delayed, also this process, separate flow)

Stateless polling. Each payment is evaluated exactly once per transition
because the WHERE clauses (status = 'pending', etc.) stop matching after
the UPDATE.
"""

import os
import time
import random
from datetime import datetime, timezone

from utils.db_utils import setup_logging, connect, transaction, GracefulShutdown


TICK_SECONDS = int(os.getenv("PAYMENT_TICK_SECONDS", "2"))

# Min delay between payment INSERT and processing — simulates the payment
# processor taking a few seconds to respond.
MIN_PROCESSING_DELAY_SECONDS = int(os.getenv("PAYMENT_MIN_DELAY_SEC", "3"))

# Min delay between order cancellation and refund — simulates async refund
# processing for customer-initiated cancellations.
MIN_REFUND_DELAY_SECONDS = int(os.getenv("REFUND_MIN_DELAY_SEC", "30"))

# Probability of payment failure (declined card, etc.)
FAILURE_RATE = float(os.getenv("PAYMENT_FAILURE_RATE", "0.04"))

BATCH_SIZE = int(os.getenv("PAYMENT_BATCH_SIZE", "500"))

FAILURE_REASONS = [
    "Card declined",
    "Insufficient funds",
    "Invalid CVV",
    "Card expired",
    "Bank rejected transaction",
    "Fraud check failed",
]


log = setup_logging("payment_processor")


# =============================================================================
# Pending payment processing
# =============================================================================

def fetch_pending_payments(conn) -> list:
    """Find pending payments past the minimum processing delay."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.payment_id, p.order_id, p.amount, o.status
            FROM payments p
            JOIN orders o ON o.order_id = p.order_id
            WHERE p.status = 'pending'
              AND p.created_at <= NOW() - (%s * INTERVAL '1 second')
            LIMIT %s
        """, (MIN_PROCESSING_DELAY_SECONDS, BATCH_SIZE))
        rows = cur.fetchall()
    conn.commit()
    return rows


def random_processor_ref() -> str:
    """Mimic Stripe-style charge ID."""
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "ch_" + "".join(random.choices(chars, k=24))


def capture_payment(conn, payment_id: int) -> bool:
    """Transition pending → captured. Idempotent."""
    now = datetime.now(timezone.utc)
    ref = random_processor_ref()
    with transaction(conn) as cur:
        cur.execute("""
            UPDATE payments
            SET status = 'captured',
                paid_at = %s,
                processor_ref = %s
            WHERE payment_id = %s AND status = 'pending'
        """, (now, ref, payment_id))
        return cur.rowcount > 0


def fail_payment_and_cancel_order(
    conn, payment_id: int, order_id: int, current_order_status: str
) -> bool:
    """
    "Failure" path: in a SINGLE TRANSACTION:
      1. Mark payment as 'refunded' (no money was actually captured)
      2. Cancel the order (status='cancelled', cancelled_at=NOW)
      3. Insert an order_status_events row with the failure reason

    This keeps payment + order + history consistent and avoids leaving
    orders stuck with failed payments.

    Returns True if the transaction took effect; False if a race meant the
    payment was no longer pending or the order was no longer cancellable.
    """
    reason = random.choice(FAILURE_REASONS)
    note = f"Payment failed: {reason}"
    now = datetime.now(timezone.utc)

    with transaction(conn) as cur:
        # 1. Refund the payment (no money was charged, but the lifecycle
        #    column reflects the final state).
        cur.execute("""
            UPDATE payments
            SET status = 'refunded',
                processor_ref = NULL
            WHERE payment_id = %s AND status = 'pending'
        """, (payment_id,))
        if cur.rowcount == 0:
            return False  # someone else got to it

        # 2. Cancel the order — only if it isn't already terminal.
        cur.execute("""
            UPDATE orders
            SET status = 'cancelled',
                cancelled_at = %s
            WHERE order_id = %s
              AND status NOT IN ('delivered', 'cancelled')
        """, (now, order_id))
        if cur.rowcount == 0:
            log.warning(
                "Payment %s failed but order %s already terminal (%s); "
                "payment marked refunded, order untouched",
                payment_id, order_id, current_order_status,
            )
            return True

        # 3. Append the cancellation event with the failure reason.
        cur.execute("""
            INSERT INTO order_status_events (
                order_id, from_status, to_status, event_ts,
                actor_type, actor_id, notes
            ) VALUES (%s, %s, 'cancelled', %s, 'system', NULL, %s)
        """, (order_id, current_order_status, now, note))

    log.info("Order %s cancelled due to payment failure: %s", order_id, reason)
    return True


def process_one_pending(
    conn, payment_id: int, order_id: int, order_status: str
):
    """
    Roll outcome and apply it. Returns:
      'captured'  — payment captured (happy path)
      'refunded'  — payment failed → refunded + order cancelled
      None        — race; row no longer matched
    """
    if random.random() < FAILURE_RATE:
        return "refunded" if fail_payment_and_cancel_order(
            conn, payment_id, order_id, order_status,
        ) else None
    else:
        return "captured" if capture_payment(conn, payment_id) else None


# =============================================================================
# Customer-initiated refunds (cancelled orders with captured payments)
# =============================================================================

def fetch_payments_to_refund(conn) -> list:
    """
    Find captured payments whose order was cancelled at least
    MIN_REFUND_DELAY_SECONDS ago, and which are not already refunded.

    These are CUSTOMER-initiated cancellations from order_lifecycle.py —
    not the payment-failure cancellations we already handled above.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.payment_id
            FROM payments p
            JOIN orders o ON o.order_id = p.order_id
            WHERE p.status = 'captured'
              AND o.status = 'cancelled'
              AND o.cancelled_at <= NOW() - (%s * INTERVAL '1 second')
            LIMIT %s
        """, (MIN_REFUND_DELAY_SECONDS, BATCH_SIZE))
        rows = cur.fetchall()
    conn.commit()
    return [r[0] for r in rows]


def process_one_refund(conn, payment_id: int) -> bool:
    """Transition a captured payment to refunded. Idempotent."""
    with transaction(conn) as cur:
        cur.execute("""
            UPDATE payments
            SET status = 'refunded'
            WHERE payment_id = %s AND status = 'captured'
        """, (payment_id,))
        return cur.rowcount > 0


# =============================================================================
# Main loop
# =============================================================================

def main():
    log.info(
        "Starting payment_processor (tick=%ds, processing_delay=%ds, "
        "refund_delay=%ds, failure_rate=%.1f%%)",
        TICK_SECONDS, MIN_PROCESSING_DELAY_SECONDS,
        MIN_REFUND_DELAY_SECONDS, FAILURE_RATE * 100,
    )
    shutdown = GracefulShutdown()
    conn = connect()

    while not shutdown.should_stop:
        try:
            # 1. Process pending payments (capture or fail+cancel)
            pending = fetch_pending_payments(conn)
            if pending:
                captured = 0
                refunded_via_failure = 0
                for payment_id, order_id, _amount, order_status in pending:
                    try:
                        result = process_one_pending(
                            conn, payment_id, order_id, order_status,
                        )
                        if result == "captured":
                            captured += 1
                        elif result == "refunded":
                            refunded_via_failure += 1
                    except Exception:
                        log.exception("Failed processing payment %s", payment_id)
                if captured or refunded_via_failure:
                    log.info(
                        "Processed %d pending: %d captured, %d failed->refunded+cancelled",
                        len(pending), captured, refunded_via_failure,
                    )

            # 2. Process customer-cancellation refunds
            to_refund = fetch_payments_to_refund(conn)
            if to_refund:
                refunded = 0
                for payment_id in to_refund:
                    try:
                        if process_one_refund(conn, payment_id):
                            refunded += 1
                    except Exception:
                        log.exception("Failed refunding payment %s", payment_id)
                if refunded:
                    log.info("Refunded %d payments (customer cancellations)",
                             refunded)

            time.sleep(TICK_SECONDS)
        except Exception:
            log.exception("Unexpected error in main loop")
            time.sleep(TICK_SECONDS)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()

    log.info("Stopping payment_processor")
    conn.close()


if __name__ == "__main__":
    main()