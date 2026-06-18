import os
import logging
from datetime import datetime
from pyspark.sql import functions as F
from src.config_loader import *

logger = logging.getLogger(__name__)

CRITICAL_COLUMNS = ["pickup_datetime", "dropoff_datetime", "pickup_location_id",
                    "dropoff_location_id", "trip_distance", "total_amount"]


def validate_quality(spark, services, years, process_id):
    quality_rejected = []
    quality_metrics = []
    
    for service in services:
        silver_service_path = os.path.join(SILVER_PATH, service)
        if not os.path.exists(silver_service_path):
            continue
        
        for year in years:
            months = get_months(year)
            for month in months:
                silver_partition = os.path.join(silver_service_path, f"year={year}", f"month={month:02d}")
                if not os.path.exists(silver_partition):
                    continue
                
                try:
                    df_silver = spark.read.parquet(silver_partition)
                    total_records = df_silver.count()
                    if total_records == 0:
                        continue
                    
                    null_critical = 0
                    for col in CRITICAL_COLUMNS:
                        if col in df_silver.columns:
                            null_critical += df_silver.filter(F.col(col).isNull()).count()
                    
                    valid_records = df_silver.filter(F.col("quality_status") == "VALID").count()
                    rejected_records = df_silver.filter(F.col("quality_status") == "SUSPICIOUS").count()
                    
                    source_file = f"{service}_tripdata_{year}-{month:02d}.parquet"
                    
                    suspicious_df = df_silver.filter(F.col("quality_status") == "SUSPICIOUS")
                    if suspicious_df.count() > 0:
                        rejection_rules = [
                            ("trip_distance <= 0", "trip_distance", F.col("trip_distance") <= 0),
                            ("total_amount <= 0", "total_amount", F.col("total_amount") <= 0),
                            ("fare_amount < 0", "fare_amount", F.col("fare_amount") < 0),
                            ("duration <= 0", "trip_duration_minutes", F.col("trip_duration_minutes") <= 0),
                            ("duration > 480", "trip_duration_minutes", F.col("trip_duration_minutes") > 480),
                            ("speed > 100 mph", "average_speed_mph", F.col("average_speed_mph") > 100),
                            ("tip > 100%", "tip_percentage", F.col("tip_percentage") > 100),
                            ("pickup > dropoff", "pickup_datetime", F.col("pickup_datetime") > F.col("dropoff_datetime")),
                            ("future pickup", "pickup_datetime", F.col("pickup_datetime") > F.current_timestamp())
                        ]
                        
                        for rule_name, column, condition in rejection_rules:
                            if column in df_silver.columns:
                                violated = suspicious_df.filter(condition)
                                if violated.count() > 0:
                                    sample = violated.select("trip_id", column).limit(5).collect()
                                    for row in sample:
                                        quality_rejected.append({
                                            "process_id": process_id,
                                            "trip_id": row["trip_id"],
                                            "service_type": service,
                                            "source_file": source_file,
                                            "rejection_stage": "phase5_quality",
                                            "rejection_rule": rule_name,
                                            "rejection_column": column,
                                            "original_value": str(row[column]) if row[column] is not None else None,
                                            "technical_reason": f"Rule '{rule_name}' violated on column {column}",
                                            "business_reason": "Data does not meet quality standards",
                                            "rejected_at": datetime.now().isoformat()
                                        })
                    
                    duplicate_count = df_silver.groupBy("trip_id").count().filter("count > 1").count()
                    quality_pct = round((valid_records / total_records) * 100, 2) if total_records > 0 else 0
                    
                    quality_metrics.append({
                        "process_id": process_id,
                        "service_type": service,
                        "year": year,
                        "month": month,
                        "total_records": total_records,
                        "valid_records": valid_records,
                        "rejected_records": rejected_records,
                        "duplicate_records": duplicate_count,
                        "null_critical_records": null_critical,
                        "suspicious_records": rejected_records,
                        "quality_percentage": quality_pct,
                        "processed_at": datetime.now().isoformat()
                    })
                    
                except Exception as e:
                    logger.error(f"Quality error for {silver_partition}: {e}")
    
    return quality_metrics, quality_rejected
