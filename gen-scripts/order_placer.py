"""
order_placer.py — Continuously creates new orders.

Each iteration:
  1. Picks a random customer, restaurant, dropoff zone
  2. Picks 1-5 random menu items from that restaurant
  3. INSERTs a single transaction:
       - orders (status='placed')
       - order_items (one per chosen menu item)
       - payments (status='pending')
       - order_status_events (initial 'placed' event)

All in one DB transaction so CDC sees a consistent burst of inserts per order.

Stops gracefully on SIGINT/SIGTERM.
"""

import os
import time
import random
from datetime import datetime, timezone
from decimal import Decimal

from utils.db_utils import (
    setup_logging, connect, transaction,
    EntityCache, random_dropoff_zone, GracefulShutdown,
)


# Tunables
ORDERS_PER_MINUTE = int(os.getenv("ORDERS_PER_MINUTE", "1000"))
SLEEP_SECONDS     = 40.0 / ORDERS_PER_MINUTE
REFRESH_EVERY_N   = 200      # refresh entity cache every N orders

PAYMENT_METHODS = ["card", "cash", "wallet", "bank_transfer"]
PAYMENT_METHOD_WEIGHTS = [0.55, 0.20, 0.20, 0.05]

DELIVERY_FEE_RANGE = (5.0, 20.0)     # SAR
SERVICE_FEE_RANGE  = (2.0, 8.0)      # SAR


log = setup_logging("order_placer")


def place_one_order(conn, cache: EntityCache) -> bool:
    """Build and INSERT one full order (header + items + payment + initial event)."""
    if not cache.customers or not cache.restaurants:
        log.warning("Cache empty; refreshing")
        cache.refresh()
        return False

    customer_id = random.choice(cache.customers)
    rid, pickup_zone_id, city_id = random.choice(cache.restaurants)

    menu = cache.menu_items_by_restaurant.get(rid)
    if not menu:
        return False  # restaurant has no available items right now

    dropoff_zone_id = random_dropoff_zone(cache, city_id) or pickup_zone_id

    # Choose 1-5 distinct menu items
    n_items = random.randint(1, min(5, len(menu)))
    chosen = random.sample(menu, n_items)

    # Build line items
    items = []
    subtotal = Decimal("0.00")
    for menu_item_id, unit_price in chosen:
        quantity = random.randint(1, 3)
        line_total = (Decimal(str(unit_price)) * quantity).quantize(Decimal("0.01"))
        subtotal += line_total
        items.append((menu_item_id, quantity, unit_price, line_total))

    delivery_fee = Decimal(str(round(random.uniform(*DELIVERY_FEE_RANGE), 2)))
    service_fee  = Decimal(str(round(random.uniform(*SERVICE_FEE_RANGE), 2)))
    discount     = Decimal("0.00")
    total        = (subtotal + delivery_fee + service_fee - discount).quantize(Decimal("0.01"))

    payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_METHOD_WEIGHTS, k=1)[0]
    placed_at = datetime.now(timezone.utc)

    with transaction(conn) as cur:
        # Insert the order header
        cur.execute("""
            INSERT INTO orders (
                customer_id, restaurant_id, pickup_zone_id, dropoff_zone_id,
                status, subtotal, delivery_fee, service_fee, discount, tip, total,
                placed_at
            ) VALUES (
                %s, %s, %s, %s,
                'placed', %s, %s, %s, %s, NULL, %s,
                %s
            )
            RETURNING order_id
        """, (
            customer_id, rid, pickup_zone_id, dropoff_zone_id,
            subtotal, delivery_fee, service_fee, discount, total,
            placed_at,
        ))
        order_id = cur.fetchone()[0]

        # Insert all line items
        cur.executemany("""
            INSERT INTO order_items (
                order_id, menu_item_id, quantity, unit_price, line_total
            ) VALUES (%s, %s, %s, %s, %s)
        """, [
            (order_id, mid, qty, up, lt)
            for (mid, qty, up, lt) in items
        ])

        # Insert pending payment
        cur.execute("""
            INSERT INTO payments (
                order_id, payment_method, amount, status
            ) VALUES (%s, %s, %s, 'pending')
        """, (order_id, payment_method, total))

        # Insert initial status event
        cur.execute("""
            INSERT INTO order_status_events (
                order_id, from_status, to_status, event_ts, actor_type, actor_id
            ) VALUES (%s, NULL, 'placed', %s, 'customer', %s)
        """, (order_id, placed_at, customer_id))

    log.debug("Placed order %s: %d items, total %s SAR", order_id, n_items, total)
    return True


def main():
    log.info("Starting order_placer (target: %.1f orders/min, sleep %.2fs)",
             ORDERS_PER_MINUTE, SLEEP_SECONDS)
    shutdown = GracefulShutdown()
    conn = connect()
    cache = EntityCache(conn)

    placed = 0
    while not shutdown.should_stop:
        try:
            if place_one_order(conn, cache):
                placed += 1

            if placed > 0 and placed % REFRESH_EVERY_N == 0:
                log.info("Placed %d orders; refreshing cache", placed)
                cache.refresh()

            time.sleep(SLEEP_SECONDS)
        except Exception:
            log.exception("Error placing order; sleeping before retry")
            time.sleep(5)
            # Reconnect on persistent failure
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()
            cache = EntityCache(conn)

    log.info("Stopping; placed %d orders this session", placed)
    conn.close()


if __name__ == "__main__":
    main()
