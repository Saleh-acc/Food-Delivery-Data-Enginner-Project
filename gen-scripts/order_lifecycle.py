"""
order_lifecycle.py — Drives orders through their status lifecycle.

Stateless polling pattern: every TICK_SECONDS, query all non-terminal orders,
check whether each is "due" for its next transition based on existing
timestamps, transition + insert a status event for any that are due.

Lifecycle:
  placed -> confirmed -> preparing -> ready -> picked_up -> delivered
                                                          -> (cancelled at any stage)

State lives in the database; no in-memory timers, restart-safe.
"""

import os
import time
import random
from datetime import datetime, timedelta, timezone

from utils.db_utils import (
    setup_logging, connect, transaction, EntityCache, GracefulShutdown,
)


# Tunables — how long an order spends in each pre-transition state.
# Format: (status, (min_seconds, max_seconds_until_next_transition), next_status)
# `placed -> confirmed`: 10s-2min after placed_at
# `confirmed -> preparing`: 30s-2min after confirmed_at
# `preparing -> ready`: 8min-25min after confirmed_at (preparing has no own timestamp)
# `ready -> picked_up`: 1min-10min after ready_at
# `picked_up -> delivered`: 5min-20min after picked_up_at
TRANSITIONS = [
    # (current_status, ts_column_to_check, min_sec, max_sec, next_status, next_ts_col)
    ("placed",     "placed_at",     10,    120,  "confirmed",  "confirmed_at"),
    ("confirmed",  "confirmed_at",  30,    120,  "preparing",  None),         # no preparing_at column
    ("preparing",  "confirmed_at",  480,   1500, "ready",      "ready_at"),
    ("ready",      "ready_at",      60,    600,  "picked_up",  "picked_up_at"),
    ("picked_up",  "picked_up_at",  300,   1200, "delivered",  "delivered_at"),
]

# Probability of cancellation per check, per order in early stages
CANCELLATION_RATE = 0.005    # 0.5% chance per tick per eligible order
CANCEL_ELIGIBLE_STATUSES = ("placed", "confirmed", "preparing")

TICK_SECONDS    = int(os.getenv("LIFECYCLE_TICK_SECONDS", "3"))
LIFECYCLE_BATCH_SIZE = int(os.getenv("LIFECYCLE_BATCH_SIZE", "2000"))
REFRESH_EVERY_N = 50         # refresh cache every N ticks


log = setup_logging("order_lifecycle")


def assign_driver_if_needed(cur, order_id: int, cache: EntityCache,
                             new_status: str) -> int | None:
    """When transitioning to 'picked_up', assign a driver if not already set."""
    if new_status != "picked_up":
        return None
    if not cache.drivers:
        return None
    cur.execute("SELECT driver_id FROM orders WHERE order_id = %s", (order_id,))
    row = cur.fetchone()
    if row and row[0]:
        return row[0]   # already assigned
    driver_id, _city_id = random.choice(cache.drivers)
    cur.execute("UPDATE orders SET driver_id = %s WHERE order_id = %s",
                (driver_id, order_id))
    return driver_id


def transition_order(conn, order: tuple, cache: EntityCache):
    """
    Apply one status transition to one order. `order` is a row tuple as fetched
    by the main query (see fetch_due_orders).
    """
    (order_id, status, placed_at, confirmed_at, ready_at, picked_up_at,
     customer_id, _payment_status) = order

    # Find which transition rule applies
    rule = next((r for r in TRANSITIONS if r[0] == status), None)
    if not rule:
        return False

    _curr, ts_col, _min_s, _max_s, next_status, next_ts_col = rule

    now = datetime.now(timezone.utc)

    with transaction(conn) as cur:
        update_parts = ["status = %s"]
        update_vals = [next_status]
        if next_ts_col:
            update_parts.append(f"{next_ts_col} = %s")
            update_vals.append(now)

        update_vals.append(order_id)

        cur.execute(
            f"UPDATE orders SET {', '.join(update_parts)} WHERE order_id = %s",
            update_vals,
        )

        # Assign a driver when picking up if not already assigned
        actor_type = "system"
        actor_id = None
        if next_status == "picked_up":
            driver_id = assign_driver_if_needed(cur, order_id, cache, next_status)
            if driver_id:
                actor_type = "driver"
                actor_id = driver_id
        elif next_status in ("confirmed", "preparing", "ready"):
            actor_type = "restaurant"

        cur.execute("""
            INSERT INTO order_status_events (
                order_id, from_status, to_status, event_ts, actor_type, actor_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (order_id, status, next_status, now, actor_type, actor_id))

    return True


def cancel_order(conn, order_id: int, current_status: str):
    """Cancel an order: set status='cancelled', cancelled_at=now, append event."""
    now = datetime.now(timezone.utc)
    with transaction(conn) as cur:
        cur.execute("""
            UPDATE orders
            SET status = 'cancelled', cancelled_at = %s
            WHERE order_id = %s AND status NOT IN ('delivered', 'cancelled')
        """, (now, order_id))

        cur.execute("""
            INSERT INTO order_status_events (
                order_id, from_status, to_status, event_ts, actor_type, actor_id, notes
            ) VALUES (%s, %s, 'cancelled', %s, 'customer', NULL, 'Customer cancelled')
        """, (order_id, current_status, now))


def fetch_due_orders(conn) -> list:
    """
    Find non-terminal orders + their relevant timestamps + the order's
    most recent payment status, and decide due-ness in Python.

    Limits the result so this query stays fast even at high order rates.
    Orders are returned oldest-first so nothing gets starved when the
    backlog grows.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                o.order_id, o.status, o.placed_at, o.confirmed_at,
                o.ready_at, o.picked_up_at, o.customer_id,
                p.status AS payment_status
            FROM orders o
            LEFT JOIN LATERAL (
                SELECT status
                FROM payments
                WHERE order_id = o.order_id
                ORDER BY payment_id DESC
                LIMIT 1
            ) p ON TRUE
            WHERE o.status NOT IN ('delivered', 'cancelled')
            ORDER BY o.placed_at
            LIMIT %s
        """, (LIFECYCLE_BATCH_SIZE,))
        rows = cur.fetchall()
    conn.commit()
    return rows


def is_due(order_row: tuple) -> bool:
    """
    Pure-Python decision: given an order's current status, lifecycle
    timestamps, and payment status, is it due for the next transition?

    Special rule: an order at status='confirmed' can ONLY advance to
    'preparing' if its payment is 'captured'. If payment is still 'pending',
    we wait. If payment is 'refunded' (failure path), the payment_processor
    has already cancelled the order — we won't see it here because the
    WHERE clause excludes cancelled orders.
    """
    (_oid, status, placed_at, confirmed_at, ready_at, picked_up_at,
     _customer_id, payment_status) = order_row

    rule = next((r for r in TRANSITIONS if r[0] == status), None)
    if not rule:
        return False

    # Payment gate: don't start preparing until payment is captured.
    if status == "confirmed" and payment_status != "captured":
        return False

    _curr, ts_col, min_s, max_s, _next_status, _next_ts_col = rule
    ts_map = {
        "placed_at":    placed_at,
        "confirmed_at": confirmed_at,
        "ready_at":     ready_at,
        "picked_up_at": picked_up_at,
    }
    ts = ts_map.get(ts_col)
    if ts is None:
        return False

    delay = random.uniform(min_s, max_s)
    elapsed = (datetime.now(timezone.utc) - ts).total_seconds()
    return elapsed >= delay


def main():
    log.info("Starting order_lifecycle (tick=%ds)", TICK_SECONDS)
    shutdown = GracefulShutdown()
    conn = connect()
    cache = EntityCache(conn)

    tick = 0
    while not shutdown.should_stop:
        try:
            tick += 1
            orders = fetch_due_orders(conn)
            log.info("Tick %d: %d in-flight orders", tick, len(orders))

            transitioned = 0
            cancelled = 0

            for order in orders:
                order_id, status, *_ = order

                # Roll cancellation chance for early-stage orders
                if (status in CANCEL_ELIGIBLE_STATUSES
                        and random.random() < CANCELLATION_RATE):
                    try:
                        cancel_order(conn, order_id, status)
                        cancelled += 1
                        continue
                    except Exception:
                        log.exception("Failed to cancel order %s", order_id)
                        continue

                # Otherwise check if due for next transition
                if is_due(order):
                    try:
                        if transition_order(conn, order, cache):
                            transitioned += 1
                    except Exception:
                        log.exception("Failed to transition order %s", order_id)

            if transitioned or cancelled:
                log.info("Tick %d done: %d transitions, %d cancellations",
                         tick, transitioned, cancelled)

            if tick % REFRESH_EVERY_N == 0:
                cache.refresh()

            time.sleep(TICK_SECONDS)
        except Exception:
            log.exception("Unexpected error in main loop")
            time.sleep(TICK_SECONDS)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
            cache = EntityCache(conn)

    log.info("Stopping order_lifecycle")
    conn.close()


if __name__ == "__main__":
    main()