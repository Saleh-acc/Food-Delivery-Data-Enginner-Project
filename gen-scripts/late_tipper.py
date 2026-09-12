"""
late_tipper.py — Sets a random tip on delivered orders past the eligibility
window.

Pattern: every TICK_SECONDS, find delivered orders where:
  - tip IS NULL  (not yet evaluated)
  - delivered_at >= MIN_DELAY_MINUTES ago  (delay before tip "arrives")

For each, UPDATE tip to a random uniform amount in [0, 10] SAR.

The tip column is NULL by default (see schema), so each order is
evaluated exactly once: after the UPDATE, tip IS NOT NULL and the row
no longer matches.

Note: we ALSO update the payments.amount and orders.total to reflect
the added tip so the books balance. This is the realistic behavior —
when a tip is added, the captured payment amount changes too.
"""

import os
import time
import random
from decimal import Decimal

from utils.db_utils import setup_logging, connect, transaction, GracefulShutdown


TICK_SECONDS       = int(os.getenv("TIPPER_TICK_SECONDS", "30"))
MIN_DELAY_MINUTES  = int(os.getenv("TIPPER_MIN_DELAY_MIN", "20"))
TIP_MIN_SAR        = 0.0
TIP_MAX_SAR        = 10.0
BATCH_SIZE         = int(os.getenv("TIPPER_BATCH_SIZE", "500"))


log = setup_logging("late_tipper")


def find_eligible_orders(conn) -> list:
    """
    Find delivered orders where tip is still NULL and the eligibility
    window has been reached.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT order_id, total
            FROM orders
            WHERE status = 'delivered'
              AND tip IS NULL
              AND delivered_at <= NOW() - (%s * INTERVAL '1 minute')
            LIMIT %s
        """, (MIN_DELAY_MINUTES, BATCH_SIZE))
        rows = cur.fetchall()
    conn.commit()
    return rows


def apply_tip(conn, order_id: int, current_total: Decimal) -> Decimal:
    """
    Pick a random tip and apply it. Updates orders.tip, orders.total,
    and the captured payment for this order so amounts stay consistent.
    """
    tip = Decimal(str(round(random.uniform(TIP_MIN_SAR, TIP_MAX_SAR), 2)))
    new_total = (current_total + tip).quantize(Decimal("0.01"))

    with transaction(conn) as cur:
        cur.execute("""
            UPDATE orders
            SET tip = %s, total = %s
            WHERE order_id = %s AND tip IS NULL
        """, (tip, new_total, order_id))

        if cur.rowcount == 0:
            # Race: someone else processed this order. Nothing to do.
            return Decimal("0.00")

        # Update the corresponding captured payment amount, if one exists.
        # If multiple payments exist (refunds, split), this updates them all
        # which is technically wrong — but for our generator scope it's fine.
        cur.execute("""
            UPDATE payments
            SET amount = amount + %s
            WHERE order_id = %s AND status IN ('captured', 'authorized')
        """, (tip, order_id))

    return tip


def main():
    log.info("Starting late_tipper (tick=%ds, min_delay=%dmin, range=$%g-$%g)",
             TICK_SECONDS, MIN_DELAY_MINUTES, TIP_MIN_SAR, TIP_MAX_SAR)
    shutdown = GracefulShutdown()
    conn = connect()

    while not shutdown.should_stop:
        try:
            eligible = find_eligible_orders(conn)
            if eligible:
                applied = 0
                for order_id, current_total in eligible:
                    try:
                        tip = apply_tip(conn, order_id, current_total)
                        if tip > 0 or True:  # log even zero tips
                            applied += 1
                    except Exception:
                        log.exception("Failed to apply tip to order %s", order_id)
                log.info("Applied tips to %d orders", applied)

            time.sleep(TICK_SECONDS)
        except Exception:
            log.exception("Unexpected error in main loop")
            time.sleep(TICK_SECONDS)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()

    log.info("Stopping late_tipper")
    conn.close()


if __name__ == "__main__":
    main()