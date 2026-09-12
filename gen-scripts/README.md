# Food Delivery Data Generator

Saudi-localized data generator for the food delivery Lambda + medallion project.

## Files

### Reference data + seeder (one-shot)
- `saudi_data.py` — reference data (cities, zones, restaurant chains, menu items, names, review comments)
- `seeder.py` — one-shot seeder: cities, zones, customers, restaurants, menu_items, drivers

### 6 continuous generator processes
- `order_placer.py` — INSERTs new orders + items + initial payment + initial status_event
- `order_lifecycle.py` — UPDATEs orders through statuses; appends status events; assigns drivers; cancels some
- `late_tipper.py` — UPDATEs `tip` on delivered orders past 20-min window
- `reviewer.py` — INSERTs reviews on delivered orders 24h+ old
- `driver_status.py` — UPDATEs driver status (inactive/available/busy/offline) over time
- `menu_changer.py` — Occasional UPDATEs to menu_item price + availability (SCD2 source)

### Shared module + run scripts
- `db_utils.py` — connection helpers, entity ID cache, graceful shutdown
- `run_all.sh` — launches all 6 processes in the background
- `stop_all.sh` — sends SIGTERM for clean shutdown
- `migration_001_tip_nullable.sql` — makes `orders.tip` nullable (required)

## Prerequisites

```bash
pip install psycopg2-binary faker
```

Postgres must be running and the schema from Step 2 must already exist.

## Setup order (do this once)

```bash
# 1. Apply the tip-nullable migration
psql -U postgres -d food_delivery -f migration_001_tip_nullable.sql

# 2. Set DB connection env vars
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=food_delivery
export PG_USER=postgres
export PG_PASSWORD=postgres

# 3. Run the seeder (one shot)
python seeder.py
```

## Running the generators

### Option A: launch all 6 in the background
```bash
chmod +x run_all.sh stop_all.sh
./run_all.sh

# watch combined logs
tail -f logs/*.log

# stop them
./stop_all.sh
```

### Option B: run one at a time (for debugging)
```bash
python order_placer.py     # in terminal 1
python order_lifecycle.py  # in terminal 2
# ... etc
```

Ctrl+C stops a single process cleanly.

## Tunables (env vars)

| Env var | Default | Effect |
|---|---|---|
| `ORDERS_PER_MINUTE` | 30 | Rate of order_placer |
| `LIFECYCLE_TICK_SECONDS` | 10 | How often order_lifecycle polls |
| `TIPPER_TICK_SECONDS` | 30 | How often late_tipper polls |
| `TIPPER_MIN_DELAY_MIN` | 20 | Minutes after delivery before tip can arrive |
| `REVIEWER_TICK_SECONDS` | 60 | How often reviewer polls |
| `REVIEWER_MIN_DELAY_HOURS` | 24 | Hours after delivery before review can arrive (set to 0 for testing) |
| `DRIVER_TICK_SECONDS` | 5 | How often driver_status flips drivers |
| `DRIVERS_PER_TICK` | 10 | How many drivers flip per tick |
| `MENU_TICK_SECONDS` | 120 | How often menu_changer runs |

For fast iteration during testing, set short delays:
```bash
export REVIEWER_MIN_DELAY_HOURS=0   # reviews appear immediately after delivery
export TIPPER_MIN_DELAY_MIN=1       # tips appear 1 min after delivery
./run_all.sh
```

## What the data looks like after a few hours of running

Roughly per hour at default rates (30 orders/min):
- ~1,800 new orders placed
- Most progress through full lifecycle: 6 status events each → ~10,800 events
- ~5% cancelled mid-lifecycle
- All delivered orders eventually get a tip + a review
- ~50 menu price changes
- ~25 menu availability flips
- ~700 driver status flips

This is enough volume to see CDC events flowing meaningfully and Lambda
reconciliation patterns play out.

## Idempotency / restart behavior

Every process is designed to be safely restartable:
- State lives in the database, not in memory
- All updates use idempotent WHERE clauses (e.g., `WHERE status = 'placed'`)
- Reviews use `ON CONFLICT (order_id) DO NOTHING`
- The tip uses `WHERE tip IS NULL` as exactly-once guard

Just kill and restart. No data corruption.

## Where this fits in the bigger pipeline

This module produces the **OLTP traffic** that downstream layers consume:

```
[generators here]
       │
       ▼
   Postgres
       │
       ├──► Debezium → Kafka → Spark Streaming → MinIO bronze (CDC tables)
       │
       └──► Airflow snapshot → MinIO bronze (batch tables: cities, zones, customers, restaurants)
```

Next up: the Debezium / Kafka / Spark Streaming / Iceberg / Trino stack
that reads from this Postgres.
