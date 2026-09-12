from pyspark.sql.types import *

batch_tracker_schema = StructType([
        StructField("id", StringType(), False),
        StructField("job_name", StringType(), False),
        StructField("batch_number", IntegerType(), False),
        StructField("lower_bound", IntegerType(), False),
        StructField("upper_bound", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("rows_processed", IntegerType(), True),
        StructField("started_at_ts", TimestampType(), True),
        StructField("completed_at_ts", TimestampType(), True),
        StructField("error_message", StringType(), True),
        StructField("generated_at_ts", TimestampType(), False),

    ])

orders_schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("restaurant_id", LongType(), False),
    StructField("driver_id", LongType(), True),
    StructField("pickup_zone_id", LongType(), False),
    StructField("dropoff_zone_id", LongType(), False),
    StructField("status", StringType(), False),
    StructField("subtotal", FloatType(), False),
    StructField("delivery_fee", FloatType(), False),
    StructField("service_fee", FloatType(), False),
    StructField("discount", FloatType(), False),
    StructField("tip", FloatType(), False),
    StructField("total", FloatType(), False),
    StructField("placed_at", TimestampType(), False),
    StructField("confirmed_at", TimestampType(), True),
    StructField("ready_at", TimestampType(), True),
    StructField("picked_up_at", TimestampType(), True),
    StructField("delivered_at", TimestampType(), True),
    StructField("cancelled_at", TimestampType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),

])

drivers_items_schema = StructType([
   StructField("driver_id", StringType(), False),
    StructField("full_name", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("vehicle_type", StringType(), False),
    StructField("city_id", LongType(), False),
    StructField("status", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("onboarded_at", TimestampType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)

])

order_itmes_schema = StructType([
    StructField("order_item_id", LongType(), False),
    StructField("order_id", LongType(), False),
    StructField("menu_item_id", LongType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", FloatType(), False),
    StructField("line_total", FloatType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),

])


payments_schema = StructType([
    StructField("payment_id", LongType(), False),
    StructField("order_id", LongType(), False),
    StructField("payment_method", StringType(), False),
    StructField("amount", FloatType(), False),
    StructField("status", StringType(), False),
    StructField("processor_ref", StringType(), True),
    StructField("paid_at", TimestampType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),

])

order_status_events_schema = StructType([
    StructField("event_id", LongType(), False),
    StructField("order_id", LongType(), False),
    StructField("from_status", StringType(), True),
    StructField("to_status", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("actor_type", StringType(), True),
    StructField("actor_id", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("created_at", TimestampType(), False),

])



reviews_schema = StructType([
    StructField("review_id", LongType(), False),
    StructField("order_id", LongType(), False),
    StructField("customer_id", StringType(), False),
    StructField("restaurant_id", StringType(), False),
    StructField("driver_id", StringType(), False),
    StructField("food_rating", FloatType(), False),
    StructField("delivery_rating", FloatType(), False),
    StructField("comment", StringType(), True),
    StructField("submitted_at", TimestampType(), False),
    StructField("last_refresh_date_ts", TimestampType(), False),
])

menu_items_schema = StructType([
    StructField("menu_item_id", LongType(), False),
    StructField("restaurant_id", LongType(), False),
    StructField("name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DecimalType(8,2), False),
    StructField("is_available", BooleanType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)
])

# batch schemas 
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("email", StringType(), False),
    StructField("full_name", StringType(), False),
    StructField("prev_phone", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("prev_city_id", StringType(), False),
    StructField("city_id", StringType(), False),
    StructField("default_address", TimestampType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("signup_date", TimestampType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)

])


restaurants_schema = StructType([
    StructField("restaurant_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("cuisine_type", StringType(), False),
    StructField("city_id", StringType(), False),
    StructField("zone_id", StringType(), False),
    StructField("address", StringType(), False),
    StructField("rating_avg", DecimalType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("is_current", BooleanType(), False),
    StructField("eff_start", TimestampType(), False),
    StructField("eff_end", TimestampType(), False),
    StructField("onboarded_at", TimestampType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)

])

zones_schema = StructType([
    StructField("zone_id", StringType(), False),
    StructField("city_id", StringType(), False),
    StructField("zone_name", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)

])

cities_schema = StructType([
    StructField("city_id", StringType(), False),
    StructField("city_name", StringType(), False),
    StructField("country_code", StringType(), False),
    StructField("timezone", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False)

])