"""
seeder.py — One-shot seeder for the food delivery OLTP database.

Loads reference + entity tables into Postgres in dependency order:
    cities -> zones -> customers (depends on cities)
                    -> restaurants (depends on cities, zones)
                            -> menu_items (depends on restaurants)
                    -> drivers (depends on cities)

This script is idempotent at the table level: it checks if cities table is
already populated and skips if so. To re-seed from scratch, TRUNCATE the
tables first (be aware of FK cascades).

Usage:
    pip install psycopg2-binary faker
    python seeder.py
"""

import os
import random
import logging
from datetime import datetime, timedelta, timezone
from tqdm import tqdm

import psycopg2
import psycopg2.extras
from faker import Faker

from saudi_data import (
    CITIES,
    ZONES_BY_CITY,
    CUISINE_TYPES,
    REAL_CHAINS,
    MENU_ITEMS_BY_CUISINE,
    FIRST_NAMES_MALE,
    FIRST_NAMES_FEMALE,
    SURNAMES,
    VEHICLE_TYPES,
    DRIVER_INITIAL_STATUSES,
)


# =============================================================================
# Configuration
# =============================================================================

DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", "5432")),
    "dbname":   os.getenv("PG_DB",   "master_db"),
    "user":     os.getenv("PG_USER", "admin"),
    "password": os.getenv("PG_PASSWORD", "admin"),
}

TARGET_COUNTS = {
    "customers":   30000,
    "restaurants": 3000,
    "drivers":     15000,
}

fake = Faker()
Faker.seed(42)
random.seed(42)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# =============================================================================
# Helpers
# =============================================================================

def saudi_phone_number() -> str:
    """Generate a Saudi mobile number in +9665XXXXXXXX format."""
    return f"+9665{random.randint(10_000_000, 99_999_999)}"


def saudi_full_name() -> str:
    """Generate a transliterated Saudi name: first + surname."""
    if random.random() < 0.65:
        first = random.choice(FIRST_NAMES_MALE)
    else:
        first = random.choice(FIRST_NAMES_FEMALE)
    return f"{first} {random.choice(SURNAMES)}"


def random_past_timestamp(days_ago_max: int = 365) -> datetime:
    """Random timestamp between [now - days_ago_max, now]."""
    seconds_ago = random.randint(0, days_ago_max * 86400)
    return datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)


def bulk_insert(cur, sql: str, rows: list, page_size: int = 500) -> None:
    """
    Bulk INSERT using execute_values. No RETURNING — we re-query for IDs
    after insert using natural keys (or just SELECT *), which is more
    reliable across psycopg2 versions than RETURNING+fetch.
    """
    psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)


def rows_needed(cur, table: str, target: int) -> int:
    """
    Returns how many rows are still needed to reach the target count.
    Used for top-up mode: insert only the difference between target
    and what's already in the table.
    """
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    existing = cur.fetchone()[0]
    return max(0, target - existing)


def table_is_empty(cur, table: str) -> bool:
    cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
    return cur.fetchone() is None


# =============================================================================
# Seed functions
# =============================================================================

def seed_cities(cur) -> dict:
    """
    Insert cities, then SELECT to build name -> id map.
    Returns: {city_name: city_id}
    """
    if table_is_empty(cur, "cities"):
        rows = [
            (c["city_name"], c["country_code"], c["timezone"])
            for c in CITIES
        ]
        sql = """
            INSERT INTO cities (city_name, country_code, timezone)
            VALUES %s
        """
        bulk_insert(cur, sql, rows)
        log.info("Inserted %d cities", len(rows))
    else:
        log.info("cities already populated; loading existing IDs")

    cur.execute("SELECT city_id, city_name FROM cities")
    name_to_id = {city_name: city_id for city_id, city_name in cur.fetchall()}
    return name_to_id


def seed_zones(cur, city_name_to_id: dict) -> dict:
    """
    Insert zones (one row per (city, zone_name)), then SELECT to build map.
    Returns: {(city_name, zone_name): zone_id}
    """
    if table_is_empty(cur, "zones"):
        rows = []
        for city_name, zone_list in ZONES_BY_CITY.items():
            if city_name not in city_name_to_id:
                raise RuntimeError(
                    f"City {city_name!r} not in city_name_to_id map. "
                    f"Available: {list(city_name_to_id.keys())}"
                )
            city_id = city_name_to_id[city_name]
            for zone_name in zone_list:
                rows.append((city_id, zone_name, True))

        sql = """
            INSERT INTO zones (city_id, zone_name, is_active)
            VALUES %s
        """
        bulk_insert(cur, sql, rows)
        log.info("Inserted %d zones across %d cities",
                 len(rows), len(ZONES_BY_CITY))
    else:
        log.info("zones already populated; loading existing IDs")

    cur.execute("""
        SELECT z.zone_id, z.zone_name, c.city_name
        FROM zones z
        JOIN cities c ON c.city_id = z.city_id
    """)
    zone_lookup = {
        (city_name, zone_name): zone_id
        for zone_id, zone_name, city_name in cur.fetchall()
    }
    return zone_lookup


def seed_customers(cur, city_name_to_id: dict, count: int) -> list:
    """Insert customers up to the target count; skip if already at target."""
    needed = rows_needed(cur, "customers", count)
    if needed == 0:
        log.info("customers already at target (%d); skipping", count)
    else:
        log.info("customers: inserting %d more to reach target %d", needed, count)
        city_weights = {
            "Riyadh": 0.40, "Jeddah": 0.25, "Dammam": 0.15,
            "Mecca":  0.12, "Medina": 0.08,
        }
        cities_for_choice = list(city_weights.keys())
        weights = list(city_weights.values())

        # Generate emails using a counter suffix to guarantee uniqueness
        # across runs. Cheaper and more reliable than fake.unique.email()
        # which doesn't know about already-seeded data.
        # We over-generate by 10% to absorb any DB collisions.
        rows = []
        for i in tqdm(range(int(needed * 1.1))):
            city_name = random.choices(cities_for_choice, weights=weights, k=1)[0]
            city_id = city_name_to_id[city_name]
            full_name = saudi_full_name()

            # Synthesize an email based on the name + a unique numeric suffix.
            # The suffix uses a large random range to make collisions vanishingly rare.
            slug = full_name.lower().replace(" ", ".").replace("-", "")
            email = f"{slug}.{random.randint(100_000, 9_999_999)}@example.sa"

            phone = saudi_phone_number()
            signup_date = random_past_timestamp(days_ago_max=730).date()
            is_active = random.random() > 0.05

            rows.append((
                email, full_name, phone, city_id,
                f"{city_name}, Saudi Arabia",
                signup_date, is_active,
            ))

        sql = """
            INSERT INTO customers
                (email, full_name, phone, city_id, default_address,
                 signup_date, is_active)
            VALUES %s
            ON CONFLICT (email) DO NOTHING
        """
        bulk_insert(cur, sql, rows)

        # Report actual count after insert
        cur.execute("SELECT COUNT(*) FROM customers")
        final = cur.fetchone()[0]
        log.info("customers: now %d (target %d)", final, count)

    cur.execute("SELECT customer_id FROM customers")
    return [row[0] for row in cur.fetchall()]

    cur.execute("SELECT customer_id FROM customers")
    return [row[0] for row in cur.fetchall()]


def seed_restaurants(
    cur, city_name_to_id: dict, zone_lookup: dict, count: int
) -> list:
    """Insert restaurants up to the target count; skip if already at target."""
    needed = rows_needed(cur, "restaurants", count)
    if needed == 0:
        log.info("restaurants already at target (%d); skipping", count)
    else:
        log.info("restaurants: inserting %d more to reach target %d", needed, count)
        city_weights = {
            "Riyadh": 0.40, "Jeddah": 0.30, "Dammam": 0.15,
            "Mecca":  0.10, "Medina": 0.05,
        }

        zones_by_city = {city: [] for city in city_weights}
        for (city, _zname), zid in tqdm(zone_lookup.items()):
            if city in zones_by_city:
                zones_by_city[city].append(zid)

        rows = []
        chains = list(REAL_CHAINS)
        random.shuffle(chains)

        for i in range(needed):
            if random.random() < 0.70:
                name, cuisine = chains[i % len(chains)]
                if random.random() < 0.4:
                    name = f"{name} - Branch {random.randint(1, 50)}"
            else:
                cuisine = random.choice(CUISINE_TYPES)
                prefix = random.choice(['Royal', 'Golden', 'Al Sultan', 'Bayt', 'Shams'])
                suffix = random.choice(['Kitchen', 'Restaurant', 'House', 'Cafe', 'Grill'])
                name = f"{prefix} {suffix}"

            city_name = random.choices(
                list(city_weights.keys()),
                weights=list(city_weights.values()),
                k=1,
            )[0]
            city_id = city_name_to_id[city_name]
            zone_id = random.choice(zones_by_city[city_name])
            address = f"{fake.street_address()}, {city_name}, Saudi Arabia"
            rating_avg = round(random.uniform(3.5, 4.8), 2)
            is_active = random.random() > 0.03
            onboarded_at = random_past_timestamp(days_ago_max=900)

            rows.append((
                name, cuisine, city_id, zone_id, address,
                rating_avg, is_active, onboarded_at,
            ))

        sql = """
            INSERT INTO restaurants
                (name, cuisine_type, city_id, zone_id, address,
                 rating_avg, is_active, onboarded_at)
            VALUES %s
        """
        bulk_insert(cur, sql, rows)
        log.info("Inserted %d restaurants", len(rows))

        # New restaurants need menu items too. Pull just the newly-added ones
        # and seed their menus.
        cur.execute("""
            SELECT restaurant_id, cuisine_type
            FROM restaurants
            ORDER BY restaurant_id DESC
            LIMIT %s
        """, (needed,))
        new_restaurants = [(rid, ct) for rid, ct in cur.fetchall()]
        seed_menu_items_for(cur, new_restaurants)

    cur.execute("SELECT restaurant_id, cuisine_type FROM restaurants")
    return [(rid, ct) for rid, ct in cur.fetchall()]


def seed_menu_items_for(cur, restaurants: list):
    """Insert menu items for the given restaurants (used when topping up)."""
    rows = []
    for rid, cuisine in tqdm(restaurants):
        menu_pool = MENU_ITEMS_BY_CUISINE.get(
            cuisine, MENU_ITEMS_BY_CUISINE["american"]
        )
        n_items = random.randint(5, min(15, len(menu_pool)))
        chosen = random.sample(menu_pool, n_items)

        for name, category, price_min, price_max in chosen:
            price = round(random.uniform(price_min, price_max), 2)
            is_available = random.random() > 0.05
            rows.append((rid, name, category, price, is_available))

    if rows:
        sql = """
            INSERT INTO menu_items
                (restaurant_id, name, category, price, is_available)
            VALUES %s
        """
        bulk_insert(cur, sql, rows)
        log.info("Inserted %d menu_items for %d new restaurants",
                 len(rows), len(restaurants))


def seed_menu_items(cur, restaurants: list) -> list:
    """
    On the initial run: each restaurant gets 5-15 menu items.
    On subsequent runs: only seeds menus for restaurants that currently
    have none (e.g., if the previous run failed mid-way).
    Newly-added restaurants get their menus seeded inside seed_restaurants.
    """
    # Find restaurants that have no menu items at all
    cur.execute("""
        SELECT r.restaurant_id, r.cuisine_type
        FROM restaurants r
        LEFT JOIN menu_items mi ON mi.restaurant_id = r.restaurant_id
        WHERE mi.menu_item_id IS NULL
    """)
    missing = [(rid, ct) for rid, ct in cur.fetchall()]

    if not missing:
        log.info("menu_items: all restaurants have menus; skipping")
    else:
        log.info("menu_items: seeding menus for %d restaurants without menus",
                 len(missing))
        seed_menu_items_for(cur, missing)

    cur.execute("SELECT menu_item_id FROM menu_items")
    return [row[0] for row in cur.fetchall()]


def seed_drivers(cur, city_name_to_id: dict, count: int) -> list:
    """Insert drivers up to the target count; skip if already at target."""
    needed = rows_needed(cur, "drivers", count)
    if needed == 0:
        log.info("drivers already at target (%d); skipping", count)
    else:
        log.info("drivers: inserting %d more to reach target %d", needed, count)
        city_weights = {
            "Riyadh": 0.40, "Jeddah": 0.30, "Dammam": 0.15,
            "Mecca":  0.10, "Medina": 0.05,
        }

        # Load existing phones to avoid collisions with already-seeded drivers
        cur.execute("SELECT phone FROM drivers")
        seen_phones = {row[0] for row in cur.fetchall()}

        rows = []
        for _ in tqdm(range(needed)):
            while True:
                phone = saudi_phone_number()
                if phone not in seen_phones:
                    seen_phones.add(phone)
                    break

            full_name = saudi_full_name()
            vehicle_type = random.choice(VEHICLE_TYPES)
            city_name = random.choices(
                list(city_weights.keys()),
                weights=list(city_weights.values()),
                k=1,
            )[0]
            city_id = city_name_to_id[city_name]
            status = random.choice(DRIVER_INITIAL_STATUSES)
            onboarded_at = random_past_timestamp(days_ago_max=600)
            is_active = random.random() > 0.05

            if not is_active:
                status = "offline"

            rows.append((
                full_name, phone, vehicle_type, city_id,
                status, onboarded_at, is_active,
            ))

        sql = """
            INSERT INTO drivers
                (full_name, phone, vehicle_type, city_id, status,
                 onboarded_at, is_active)
            VALUES %s
        """
        bulk_insert(cur, sql, rows)
        log.info("Inserted %d drivers", len(rows))

    cur.execute("SELECT driver_id FROM drivers")
    return [row[0] for row in cur.fetchall()]


# =============================================================================
# Main
# =============================================================================

def main():
    log.info("Connecting to Postgres at %s:%s/%s",
             DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            log.info("=== Starting seed ===")

            city_map = seed_cities(cur)
            log.info("city_map ready (%d cities): %s",
                     len(city_map), list(city_map.keys()))

            zone_map = seed_zones(cur, city_map)
            log.info("zone_map ready (%d zones)", len(zone_map))

            seed_customers(cur, city_map, TARGET_COUNTS["customers"])
            restaurants = seed_restaurants(
                cur, city_map, zone_map, TARGET_COUNTS["restaurants"]
            )
            seed_menu_items(cur, restaurants)
            seed_drivers(cur, city_map, TARGET_COUNTS["drivers"])

        conn.commit()
        log.info("=== Seed complete (committed) ===")
    except Exception:
        conn.rollback()
        log.exception("Seed failed; rolled back transaction")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()