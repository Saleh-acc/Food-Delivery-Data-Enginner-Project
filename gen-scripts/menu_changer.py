"""
menu_changer.py — Occasional price changes and availability toggles on menu items.

This is the source of the SCD2 dimension lesson: price changes are rare per
item but business-critical when they happen. The CDC stream captures them;
the warehouse builds a price-history dimension downstream.

Two kinds of changes per tick:
  1. Price tweak on a few menu items (+/- 10% to 20%)
  2. Availability flip on a few menu items (in-stock <-> out-of-stock)

Cadence: slow. Real menus don't change every minute — once every few
minutes is plenty for CDC to see meaningful events.
"""

import os
import time
import random
from decimal import Decimal

from utils.db_utils import (
    setup_logging, connect, transaction, EntityCache, GracefulShutdown,
)


TICK_SECONDS         = int(os.getenv("MENU_TICK_SECONDS", "120"))
PRICE_CHANGES_PER_TICK    = int(os.getenv("PRICE_CHANGES_PER_TICK", "3"))
AVAILABILITY_FLIPS_PER_TICK = int(os.getenv("AVAILABILITY_FLIPS_PER_TICK", "2"))

# Bounds for percentage price change (multiplicative)
PRICE_DELTA_MIN = -0.20      # -20%
PRICE_DELTA_MAX =  0.20      # +20%

# Don't drop below this floor (in SAR) — avoids absurd $0.01 burgers
PRICE_FLOOR = Decimal("3.00")


log = setup_logging("menu_changer")


def fetch_random_menu_items(conn, n: int) -> list:
    """Pull n random menu_items (id, current_price, current_is_available)."""
    if n <= 0:
        return []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT menu_item_id, price, is_available
            FROM menu_items
            ORDER BY random()
            LIMIT %s
        """, (n,))
        rows = cur.fetchall()
    conn.commit()
    return rows


def change_price(conn, menu_item_id: int, current_price) -> Decimal:
    """Tweak price by random multiplier; never below PRICE_FLOOR."""
    delta = random.uniform(PRICE_DELTA_MIN, PRICE_DELTA_MAX)
    new_price = (Decimal(str(current_price)) * Decimal(str(1 + delta))).quantize(Decimal("0.01"))
    if new_price < PRICE_FLOOR:
        new_price = PRICE_FLOOR

    with transaction(conn) as cur:
        cur.execute("""
            UPDATE menu_items
            SET price = %s
            WHERE menu_item_id = %s
        """, (new_price, menu_item_id))
    return new_price


def flip_availability(conn, menu_item_id: int, current: bool) -> bool:
    """Toggle is_available."""
    new_value = not current
    with transaction(conn) as cur:
        cur.execute("""
            UPDATE menu_items
            SET is_available = %s
            WHERE menu_item_id = %s AND is_available = %s
        """, (new_value, menu_item_id, current))
    return new_value


def main():
    log.info("Starting menu_changer (tick=%ds, %d price changes + %d availability flips/tick)",
             TICK_SECONDS, PRICE_CHANGES_PER_TICK, AVAILABILITY_FLIPS_PER_TICK)
    shutdown = GracefulShutdown()
    conn = connect()

    while not shutdown.should_stop:
        try:
            # Price changes
            price_targets = fetch_random_menu_items(conn, PRICE_CHANGES_PER_TICK)
            for mid, current_price, _avail in price_targets:
                try:
                    new = change_price(conn, mid, current_price)
                    log.info("menu_item %d: price %s -> %s SAR",
                             mid, current_price, new)
                except Exception:
                    log.exception("Failed to change price on menu_item %s", mid)

            # Availability flips
            flip_targets = fetch_random_menu_items(conn, AVAILABILITY_FLIPS_PER_TICK)
            for mid, _price, current_avail in flip_targets:
                try:
                    new = flip_availability(conn, mid, current_avail)
                    log.info("menu_item %d: available %s -> %s",
                             mid, current_avail, new)
                except Exception:
                    log.exception("Failed to flip availability on menu_item %s", mid)

            time.sleep(TICK_SECONDS)
        except Exception:
            log.exception("Unexpected error in main loop")
            time.sleep(TICK_SECONDS)
            try:
                conn.close()
            except Exception:
                pass
            conn = connect()

    log.info("Stopping menu_changer")
    conn.close()


if __name__ == "__main__":
    main()
