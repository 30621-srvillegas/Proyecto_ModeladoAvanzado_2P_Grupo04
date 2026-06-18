import os
import logging
from datetime import datetime
from pyspark.sql import functions as F
from src.config_loader import *

logger = logging.getLogger(__name__)




def build_gold(spark, services):
    all_silver_dfs = []
    for service in services:
        silver_service_path = os.path.join(SILVER_PATH, service)
        if os.path.exists(silver_service_path):
            df_svc = spark.read.parquet(silver_service_path)
            if df_svc.count() > 0:
                all_silver_dfs.append(df_svc)
    
    if not all_silver_dfs:
        logger.warning("No silver data found")
        return None
    
    df_union = all_silver_dfs[0]
    for df in all_silver_dfs[1:]:
        df_union = df_union.unionByName(df, allowMissingColumns=True)
    
    logger.info(f"Union all silver: {df_union.count()} total records")
    
    df_gold_trips_clean = df_union \
        .filter(F.col("quality_status") == "VALID") \
        .select(
            "trip_id", "service_type", "pickup_datetime", "dropoff_datetime",
            "trip_duration_minutes", "trip_distance", "pickup_location_id", "dropoff_location_id",
            "payment_type", "fare_amount", "tip_amount", "total_amount",
            "tip_percentage", "average_speed_mph", "fare_per_mile", "is_airport_trip",
            "year", "month", "source_file"
        )
    
    df_gold_trips_clean.write.mode("overwrite").parquet(os.path.join(GOLD_PATH, "gold_trips_clean"))
    logger.info(f"gold_trips_clean: {df_gold_trips_clean.count()} records")
    
    df_gold_daily_revenue = df_gold_trips_clean \
        .withColumn("trip_date", F.to_date("pickup_datetime")) \
        .groupBy("service_type", "trip_date") \
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("average_fare"),
            F.avg("tip_amount").alias("average_tip"),
            F.avg("trip_distance").alias("average_trip_distance"),
            F.avg("trip_duration_minutes").alias("average_trip_duration")
        ) \
        .orderBy("service_type", "trip_date")
    
    df_gold_daily_revenue.write.mode("overwrite").parquet(os.path.join(GOLD_PATH, "gold_daily_revenue"))
    logger.info(f"gold_daily_revenue: {df_gold_daily_revenue.count()} records")
    
    df_gold_location_performance = df_gold_trips_clean \
        .groupBy("service_type", "pickup_location_id", "dropoff_location_id") \
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("average_fare"),
            F.avg("trip_distance").alias("average_distance"),
            F.avg("trip_duration_minutes").alias("average_duration"),
            F.sum(F.when(F.col("is_airport_trip") == True, 1).otherwise(0)).alias("suspicious_trip_count")
        ) \
        .orderBy("service_type", "pickup_location_id", "dropoff_location_id")
    
    df_gold_location_performance.write.mode("overwrite").parquet(os.path.join(GOLD_PATH, "gold_location_performance"))
    logger.info(f"gold_location_performance: {df_gold_location_performance.count()} records")
    
    return df_gold_trips_clean, df_gold_daily_revenue, df_gold_location_performance


def load_to_sqlite(spark):
    from sqlalchemy import create_engine, text
    import pandas as pd
    
    engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
    
    # Tablas pequeñas (< 5 MB) → pandas (inspección auxiliar, permitida por especificación)
    small_tables = [
        ("gold_daily_revenue", os.path.join(GOLD_PATH, "gold_daily_revenue")),
        ("gold_location_performance", os.path.join(GOLD_PATH, "gold_location_performance")),
        ("quality_rejected_records", os.path.join(METADATA_PATH, "quality_rejected_records")),
        ("quality_metrics_summary", os.path.join(METADATA_PATH, "quality_metrics_summary")),
        ("audit_file_inventory", os.path.join(METADATA_PATH, "audit_file_inventory")),
    ]
    
    for table_name, parquet_path in small_tables:
        try:
            if os.path.exists(parquet_path):
                df_spark = spark.read.parquet(parquet_path)
                pd_df = df_spark.toPandas()
                pd_df.to_sql(table_name, engine, if_exists="replace", index=False)
                logger.info(f"Loaded {table_name}: {len(pd_df)} rows")
            else:
                logger.warning(f"Path not found: {parquet_path}")
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
    
    # gold_trips_clean (54 GB) → pandas read_parquet directo archivo por archivo (sin Spark)
    gold_trips_path = os.path.join(GOLD_PATH, "gold_trips_clean")
    if os.path.exists(gold_trips_path):
        try:
            import pandas as pd
            parquet_files = sorted([f for f in os.listdir(gold_trips_path) if f.endswith(".parquet")])
            total_rows = 0
            for i, pf in enumerate(parquet_files):
                fpath = os.path.join(gold_trips_path, pf)
                pdf = pd.read_parquet(fpath)
                pdf = pdf.sample(frac=0.1, random_state=42)
                mode = "replace" if i == 0 else "append"
                pdf.to_sql("gold_trips_clean", engine, if_exists=mode, index=False)
                total_rows += len(pdf)
                logger.info(f"Loaded chunk {i+1}/{len(parquet_files)}: {pf} ({len(pdf)} rows)")
            logger.info(f"Loaded gold_trips_clean: {total_rows} total rows")
        except Exception as e:
            logger.error(f"Failed to load gold_trips_clean: {e}")
    
    logger.info("SQLite loading complete")
    return engine


def run_verification_queries(engine):
    from sqlalchemy import text
    import pandas as pd
    
    verify_queries = [
        ("Q1 - Revenue por servicio", """
            SELECT service_type, COUNT(*) AS total_trips, SUM(total_amount) AS total_revenue
            FROM gold_trips_clean
            GROUP BY service_type
            ORDER BY total_revenue DESC
        """),
        ("Q2 - Metricas de calidad", """
            SELECT service_type, year, month, total_records, valid_records,
                   rejected_records, quality_percentage
            FROM quality_metrics_summary
            ORDER BY year, month, service_type
        """),
        ("Q3 - Top 20 rutas por revenue", """
            SELECT pickup_location_id, dropoff_location_id,
                   COUNT(*) AS total_trips, SUM(total_amount) AS total_revenue,
                   AVG(trip_duration_minutes) AS avg_duration
            FROM gold_trips_clean
            GROUP BY pickup_location_id, dropoff_location_id
            ORDER BY total_revenue DESC
            LIMIT 20
        """)
    ]
    
    with engine.connect() as conn:
        for q_name, q_sql in verify_queries:
            print(f"\n{'='*60}")
            print(f"  {q_name}")
            print(f"{'='*60}")
            result = conn.execute(text(q_sql))
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            print(result_df.to_string(index=False))
    
    logger.info("Verification queries completed")
