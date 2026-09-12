"""
db_utils.py — Shared utilities for all generator processes.

Connection management, cached entity ID lookups, and a graceful-shutdown
helper. Imported by order_placer.py, order_lifecycle.py, late_tipper.py,
reviewer.py, driver_status.py, menu_changer.py.
"""

import os
import signal
import logging
import random
from contextlib import contextmanager

import psycopg2
import psycopg2.extras


DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",   "master_db"),
    "user":     os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}


# =============================================================================
# Logging
# =============================================================================

def setup_logging(process_name: str) -> logging.Logger:
    """Standard log format with the process name baked in."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format=f"%(asctime)s [%(levelname)s] [{process_name}] %(message)s",
    )
    return logging.getLogger(process_name)


# =============================================================================
# Connection management
# =============================================================================

def connect():
    """Open a Postgres connection with autocommit OFF (we manage transactions)."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


@contextmanager
def transaction(conn):
    """
    Context manager: commit on success, rollback on exception.

    Usage:
        with transaction(conn) as cur:
            cur.execute(...)
    """
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# =============================================================================
# Entity ID caches
#
# All processes need to pick random customers, restaurants, drivers, etc.
# Loading IDs once at startup and caching them in memory is far faster than
# querying every iteration. The caches go slightly stale as new entities are
# added (Airflow DAGs add slow-changing batch data over time), but each
# process refreshes its caches every REFRESH_EVERY_N_TICKS iterations to
# pick up additions.
# =============================================================================

class EntityCache:
    """
    Caches IDs of frequently-referenced entities. Refreshes on demand.
    """

    def __init__(self, conn):
        self.conn = conn
        self.customers = []
        self.restaurants = []          # list of (rid, zone_id, city_id)
        self.drivers = []              # list of (driver_id, city_id)
        self.menu_items_by_restaurant = {}  # rid -> list of (mid, price)
        self.zones_by_city = {}        # city_id -> list of zone_ids
        self.refresh()

    def refresh(self):
        """Reload all caches from the database."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM customers WHERE is_active = true")
            self.customers = [row[0] for row in cur.fetchall()]

            cur.execute("""
                SELECT restaurant_id, zone_id, city_id
                FROM restaurants
                WHERE is_active = true
            """)
            self.restaurants = [(rid, zid, cid) for rid, zid, cid in cur.fetchall()]

            cur.execute("""
                SELECT driver_id, city_id
                FROM drivers
                WHERE is_active = true
            """)
            self.drivers = [(did, cid) for did, cid in cur.fetchall()]

            cur.execute("""
                SELECT menu_item_id, restaurant_id, price
                FROM menu_items
                WHERE is_available = true
            """)
            self.menu_items_by_restaurant = {}
            for mid, rid, price in cur.fetchall():
                self.menu_items_by_restaurant.setdefault(rid, []).append((mid, price))

            cur.execute("""
                SELECT zone_id, city_id
                FROM zones
                WHERE is_active = true
            """)
            self.zones_by_city = {}
            for zid, cid in cur.fetchall():
                self.zones_by_city.setdefault(cid, []).append(zid)

        self.conn.commit()  # SELECTs leave a transaction open in non-autocommit mode


def random_dropoff_zone(cache: EntityCache, city_id: int) -> int| None:
    """Pick a random delivery zone in the same city as the restaurant."""
    candidates = cache.zones_by_city.get(city_id, [])
    if not candidates:
        return None
    return random.choice(candidates)


# =============================================================================
# Graceful shutdown
#
# Each generator process is a long-running while True loop. We want them to
# stop cleanly on SIGINT (Ctrl+C) or SIGTERM (Docker / kill) so any
# in-progress transaction commits or rolls back properly.
# =============================================================================

class GracefulShutdown:
    """
    Sets a flag when SIGINT or SIGTERM is received. Loops should check
    `should_stop` each iteration and exit cleanly.

    Usage:
        shutdown = GracefulShutdown()
        while not shutdown.should_stop:
            ...
    """

    def __init__(self):
        self.should_stop = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame):
        self.should_stop = True
