"""
reviewer.py — Inserts reviews on delivered orders 1+ day old.

Pattern: every TICK_SECONDS, find delivered orders where:
  - delivered_at >= MIN_DELAY_HOURS ago  (review "writes itself" some hours later)
  - no review row exists yet

For each, INSERT into reviews with random food/delivery ratings (1-5) and
a randomly-picked comment from the Saudi review pool.

Per the design decision in Step 3.5: every delivered order eventually gets
a review (no probability roll), so the existence of a review row is the
exactly-once sentinel — once inserted, the order won't match the query again.
"""

import os
import time
import random
from datetime import datetime, timezone

from utils.db_utils import setup_logging, connect, transaction, GracefulShutdown
from saudi_data import REVIEW_COMMENTS


TICK_SECONDS     = int(os.getenv("REVIEWER_TICK_SECONDS", "5"))
# How long after delivery before a review "arrives". Tunable down for testing
# so you don't have to wait days to see reviews flow.
MIN_DELAY_HOURS  = int(os.getenv("REVIEWER_MIN_DELAY_HOURS", "0"))
BATCH_SIZE       = int(os.getenv("REVIEWER_BATCH_SIZE", "2000"))


log = setup_logging("reviewer")


def find_orders_due_for_review(conn) -> list:
    """
    Delivered orders without an existing review, past the eligibility window.
    Uses a LEFT JOIN ... WHERE r.review_id IS NULL pattern to find orders
    with no matching review row.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT o.order_id, o.customer_id, o.restaurant_id, o.driver_id
            FROM orders o
            LEFT JOIN reviews r ON r.order_id = o.order_id
            WHERE o.status = 'delivered'
              AND o.delivered_at <= NOW() - (%s * INTERVAL '1 hour')
              AND r.review_id IS NULL
            LIMIT %s
        """, (MIN_DELAY_HOURS, BATCH_SIZE))
        rows = cur.fetchall()
    conn.commit()
    return rows


def insert_review(conn, order_id: int, customer_id: int,
                  restaurant_id: int, driver_id):
    """Insert one review with random ratings + a random Saudi-style comment."""
    food_rating = random.choices(
        [1, 2, 3, 4, 5],
        weights=[0.05, 0.08, 0.15, 0.35, 0.37],   # skewed positive
        k=1,
    )[0]
    delivery_rating = random.choices(
        [1, 2, 3, 4, 5],
        weights=[0.04, 0.07, 0.14, 0.35, 0.40],
        k=1,
    )[0]

    # 75% of reviews include a comment; rest leave it empty
    comment = random.choice(REVIEW_COMMENTS) if random.random() < 0.75 else None
    submitted_at = datetime.now(timezone.utc)

    with transaction(conn) as cur:
        cur.execute("""
            INSERT INTO reviews (
                order_id, customer_id, restaurant_id, driver_id,
                food_rating, delivery_rating, comment, submitted_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
        """, (
            order_id, customer_id, restaurant_id, driver_id,
            food_rating, delivery_rating, comment, submitted_at,
        ))


def main():
    log.info("Starting reviewer (tick=%ds, min_delay=%dh)",
             TICK_SECONDS, MIN_DELAY_HOURS)
    shutdown = GracefulShutdown()
    conn = connect()

    while not shutdown.should_stop:
        try:
            due = find_orders_due_for_review(conn)
            if due:
                inserted = 0
                for order_id, customer_id, restaurant_id, driver_id in due:
                    try:
                        insert_review(conn, order_id, customer_id,
                                      restaurant_id, driver_id)
                        inserted += 1
                    except Exception:
                        log.exception("Failed to insert review for order %s",
                                      order_id)
                log.info("Inserted %d reviews", inserted)

            time.sleep(TICK_SECONDS)
        except Exception:
            log.exception("Unexpected error in main loop")
            time.sleep(TICK_SECONDS)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()

    log.info("Stopping reviewer")
    conn.close()


if __name__ == "__main__":
    main()