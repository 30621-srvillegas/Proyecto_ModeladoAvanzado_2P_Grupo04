import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from src.config_loader import *

logger = logging.getLogger(__name__)


def diagnose_schema(spark, inventory):
    canonical_cols = list(CANONICAL_SCHEMA.keys())
    diagnosis = []
    
    for item in inventory:
        if item["read_status"] != "SUCCESS":
            continue
        
        service = item["service_type"]
        fpath = item["file_path"]
        
        try:
            df_raw = spark.read.parquet(fpath)
            real_cols = set(df_raw.columns)
            
            expected_cols = set()
            for canon_col, mapping in HOMOLOGATION_MAP.items():
                src_col = mapping.get(service, None)
                if src_col:
                    expected_cols.add(src_col)
            
            missing = expected_cols - real_cols
            extra = real_cols - expected_cols
            
            diagnosis.append({
                "file_name": item["file_name"],
                "service_type": service,
                "real_columns": len(real_cols),
                "expected_columns": len(expected_cols),
                "missing_columns": list(missing) if missing else None,
                "extra_columns": list(extra) if extra else None,
                "diagnosis_status": "COMPLETE" if not missing else "MISSING_COLUMNS"
            })
        except Exception as e:
            logger.warning(f"Diagnosis error for {fpath}: {e}")
    
    return diagnosis


def build_bronze(spark, inventory):
    canonical_cols = list(CANONICAL_SCHEMA.keys())
    metadata_cols = {"year", "month", "source_file", "ingestion_timestamp", "quality_status", "trip_id", "service_type"}
    bronze_records = []
    
    for item in inventory:
        if item["read_status"] != "SUCCESS":
            continue
        
        service = item["service_type"]
        year = item["partition_year"]
        month = item["partition_month"]
        fpath = item["file_path"]
        fname = item["file_name"]
        
        bronze_service_path = os.path.join(BRONZE_PATH, service)
        
        try:
            df_raw = spark.read.parquet(fpath)
            
            select_exprs = []
            for canon_col in canonical_cols:
                if canon_col in metadata_cols:
                    continue
                mapping = HOMOLOGATION_MAP.get(canon_col, {})
                src_col = mapping.get(service)
                if src_col and src_col in df_raw.columns:
                    select_exprs.append(F.col(src_col).alias(canon_col))
                else:
                    col_type = CANONICAL_SCHEMA.get(canon_col)
                    if col_type == "double":
                        select_exprs.append(F.lit(None).cast("double").alias(canon_col))
                    elif col_type == "int":
                        select_exprs.append(F.lit(None).cast("int").alias(canon_col))
                    elif col_type == "timestamp":
                        select_exprs.append(F.lit(None).cast("timestamp").alias(canon_col))
                    else:
                        select_exprs.append(F.lit(None).alias(canon_col))
            
            if select_exprs:
                df_canonical = df_raw.select(select_exprs)
            
                df_canonical = df_canonical \
                    .withColumn("service_type", F.lit(service)) \
                    .withColumn("year", F.lit(year)) \
                    .withColumn("month", F.lit(month)) \
                    .withColumn("source_file", F.lit(fname)) \
                    .withColumn("ingestion_timestamp", F.current_timestamp()) \
                    .withColumn("quality_status", F.lit("PENDING"))
                
                bronze_output = os.path.join(bronze_service_path, f"year={year}", f"month={month:02d}")
                df_canonical.write.mode("overwrite").parquet(bronze_output)
                
                bronze_records.append({
                    "service_type": service,
                    "year": year,
                    "month": month,
                    "file_name": fname,
                    "records": df_canonical.count(),
                    "status": "BRONZE_WRITTEN"
                })
                logger.info(f"Bronze written: {fname}")
        except Exception as e:
            logger.error(f"Failed to process {fpath} to bronze: {e}")
    
    return bronze_records
