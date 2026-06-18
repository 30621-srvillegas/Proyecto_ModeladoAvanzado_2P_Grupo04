import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config_loader import *

logger = logging.getLogger(__name__)


def transform_to_silver(spark, services, years):
    all_rejected = []
    silver_paths = []
    
    for service in services:
        bronze_service_path = os.path.join(BRONZE_PATH, service)
        if not os.path.exists(bronze_service_path):
            continue
        
        silver_service_path = os.path.join(SILVER_PATH, service)
        
        for year in years:
            months = get_months(year)
            for month in months:
                bronze_partition = os.path.join(bronze_service_path, f"year={year}", f"month={month:02d}")
                if not os.path.exists(bronze_partition):
                    continue
                
                try:
                    df_bronze = spark.read.parquet(bronze_partition)
                    
                    df_transformed = df_bronze \
                        .withColumn("pickup_datetime",
                            F.when(F.col("pickup_datetime").isNull(), F.lit(None).cast("timestamp"))
                            .otherwise(F.to_timestamp(F.col("pickup_datetime")))) \
                        .withColumn("dropoff_datetime",
                            F.when(F.col("dropoff_datetime").isNull(), F.lit(None).cast("timestamp"))
                            .otherwise(F.to_timestamp(F.col("dropoff_datetime")))) \
                        .withColumn("trip_duration_minutes",
                            F.when(F.col("pickup_datetime").isNotNull() & F.col("dropoff_datetime").isNotNull(),
                                (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60)
                            .otherwise(F.lit(None))) \
                        .withColumn("average_speed_mph",
                            F.when(F.col("trip_duration_minutes").isNotNull() & (F.col("trip_duration_minutes") > 0),
                                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60))
                            .otherwise(F.lit(None))) \
                        .withColumn("fare_per_mile",
                            F.when(F.col("trip_distance").isNotNull() & (F.col("trip_distance") > 0),
                                F.col("fare_amount") / F.col("trip_distance"))
                            .otherwise(F.lit(None))) \
                        .withColumn("tip_percentage",
                            F.when(F.col("fare_amount").isNotNull() & (F.col("fare_amount") > 0),
                                (F.col("tip_amount") / F.col("fare_amount")) * 100)
                            .otherwise(F.lit(None))) \
                        .withColumn("total_amount", F.round("total_amount", 2)) \
                        .withColumn("fare_amount", F.round("fare_amount", 2)) \
                        .withColumn("tip_amount", F.round("tip_amount", 2)) \
                        .withColumn("is_airport_trip",
                            F.when(F.col("airport_fee").isNotNull() & (F.col("airport_fee") > 0), F.lit(True))
                            .otherwise(F.lit(False))) \
                        .withColumn("trip_id",
                            F.sha2(F.concat_ws("|",
                                F.col("service_type"),
                                F.col("pickup_datetime").cast("string"),
                                F.col("dropoff_datetime").cast("string"),
                                F.col("pickup_location_id").cast("string"),
                                F.col("dropoff_location_id").cast("string"),
                                F.col("total_amount").cast("string")
                            ), 256))
                    
                    total_count = df_transformed.count()
                    
                    window_spec = Window.partitionBy("trip_id").orderBy("ingestion_timestamp")
                    df_dedup = df_transformed \
                        .withColumn("rn", F.row_number().over(window_spec)) \
                        .filter(F.col("rn") == 1) \
                        .drop("rn")
                    
                    dedup_count = df_dedup.count()
                    duplicate_count = total_count - dedup_count
                    
                    is_suspicious = (
                        (F.col("trip_distance") <= 0) |
                        (F.col("total_amount") <= 0) |
                        (F.col("fare_amount") < 0) |
                        (F.col("trip_duration_minutes") <= 0) |
                        (F.col("trip_duration_minutes") > 480) |
                        (F.col("average_speed_mph") > 100) |
                        (F.col("tip_percentage") > 100) |
                        (F.col("pickup_datetime") > F.col("dropoff_datetime")) |
                        (F.col("pickup_datetime") > F.current_timestamp())
                    )
                    
                    df_dedup = df_dedup \
                        .withColumn("is_suspicious_trip", is_suspicious) \
                        .withColumn("quality_status",
                            F.when(is_suspicious, F.lit("SUSPICIOUS")).otherwise(F.lit("VALID"))) \
                        .withColumn("processing_date", F.current_date())
                    
                    source_file = f"{service}_tripdata_{year}-{month:02d}.parquet"
                    
                    silver_output = os.path.join(silver_service_path, f"year={year}", f"month={month:02d}")
                    df_dedup.write.mode("overwrite").parquet(silver_output)
                    silver_paths.append(silver_output)
                    
                    metrics = df_dedup.groupBy("quality_status").count().collect()
                    metrics_map = {row["quality_status"]: row["count"] for row in metrics}
                    valid_count = metrics_map.get("VALID", 0)
                    suspicious_count = metrics_map.get("SUSPICIOUS", 0)
                    
                    logger.info(f"Silver: {source_file} - valid:{valid_count} susp:{suspicious_count} dups:{duplicate_count}")
                    
                except Exception as e:
                    logger.error(f"Transformation error for {bronze_partition}: {e}")
    
    logger.info(f"Silver layer: {len(silver_paths)} partitions")
    return silver_paths
