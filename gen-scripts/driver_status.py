"""
driver_status.py — Flips driver status to simulate drivers going online,
busy, offline, etc.

Every TICK_SECONDS, picks a small random sample of active drivers and
randomly transitions their status. State machine:

    inactive --(login)-->  available
    available <-(busy)--> busy   (in our generator: random; in reality, busy
                                  would be set when an order is picked_up)
    available --(logout)--> offline
    offline --(login)--> available
    available --(end shift)--> inactive

For learning purposes the transitions are random — we just want CDC
to see status churn so the speed layer has something to react to.
"""

import os
import time
import random

from utils.db_utils import (
    setup_logging, connect, transaction, EntityCache, GracefulShutdown,
)


TICK_SECONDS    = int(os.getenv("DRIVER_TICK_SECONDS", "5"))
DRIVERS_PER_TICK = int(os.getenv("DRIVERS_PER_TICK", "10"))
REFRESH_EVERY_N  = 200

# Allowed transitions (current_status -> list of next_statuses with weights).
# Probabilities reflect plausibility, not correctness.
TRANSITIONS = {
    "inactive":  [("available", 1.0)],                                  # come online
    "available": [("busy", 0.40), ("offline", 0.30), ("inactive", 0.10),
                  ("available", 0.20)],                                  # mostly stay
    "busy":      [("available", 0.85), ("offline", 0.15)],
    "offline":   [("available", 0.70), ("inactive", 0.30)],
}


log = setup_logging("driver_status")


def pick_next_status(current: str) -> str | None:
    rules = TRANSITIONS.get(current)
    if not rules:
        return None
    statuses, weights = zip(*rules)
    next_status = random.choices(statuses, weights=weights, k=1)[0]
    return next_status if next_status != current else None


def flip_drivers(conn, driver_ids: list[int]):
    """Flip status for the given drivers. Reads current status, picks a new one."""
    if not driver_ids:
        return 0

    flipped = 0
    with conn.cursor() as cur:
        cur.execute("""
            SELECT driver_id, status
            FROM drivers
            WHERE driver_id = ANY(%s)
            AND is_active = TRUE 
        """, (driver_ids,))
        rows = cur.fetchall()

    conn.commit()

    for driver_id, current in rows:
        next_status = pick_next_status(current)
        if not next_status:
            continue
        try:
            with transaction(conn) as cur:
                cur.execute("""
                    UPDATE drivers
                    SET status = %s
                    WHERE driver_id = %s AND status = %s
                """, (next_status, driver_id, current))
                if cur.rowcount > 0:
                    flipped += 1
        except Exception:
            log.exception("Failed to flip driver %s", driver_id)
    return flipped


def main():
    log.info("Starting driver_status (tick=%ds, %d drivers/tick)",
             TICK_SECONDS, DRIVERS_PER_TICK)
    shutdown = GracefulShutdown()
    conn = connect()
    cache = EntityCache(conn)

    tick = 0
    while not shutdown.should_stop:
        try:
            tick += 1
            if not cache.drivers:
                log.warning("No drivers in cache; refreshing")
                cache.refresh()
                time.sleep(TICK_SECONDS)
                continue

            sample_size = min(DRIVERS_PER_TICK, len(cache.drivers))
            sample = random.sample(cache.drivers, sample_size)
            ids = [d[0] for d in sample]

            n = flip_drivers(conn, ids)
            if n:
                log.info("Tick %d: flipped %d drivers", tick, n)

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

    log.info("Stopping driver_status")
    conn.close()


if __name__ == "__main__":
    main()
