import os
import requests
import time
import logging
from pathlib import Path
from requests.exceptions import ConnectionError, ChunkedEncodingError, Timeout
from src.config_loader import *
from src.utils import get_headers, classify_error

logger = logging.getLogger(__name__)


def download_trip_data(service, year, month):
    file_name = f"{service}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{file_name}"
    target_dir = os.path.join(RAW_PATH, service, f"year={year}", f"month={month:02d}")
    target_path = os.path.join(target_dir, file_name)
    
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    
    record = {
        "process_id": "",
        "source_system": "NYC_TLC",
        "service_type": service,
        "file_name": file_name,
        "file_path": target_path,
        "file_size_mb": None,
        "partition_year": year,
        "partition_month": month,
        "read_status": "SUCCESS",
        "record_count": None,
        "column_count": None,
        "schema_hash": None,
        "error_message": None,
        "processed_at": datetime.now().isoformat()
    }
    
    if not os.path.exists(target_path):
        headers = get_headers()
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Downloading {url}...")
                resp = requests.get(url, timeout=300, headers=headers)
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning(f"Rate limited for {file_name}, retrying in {wait}s (attempt {attempt}/{max_retries})...")
                    time.sleep(wait)
                    continue
                if resp.status_code == 403:
                    logger.warning(f"HTTP 403 for {file_name} - file may not exist or rate limited")
                    record["read_status"] = "FAILED"
                    record["error_message"] = "HTTP 403: File not available or rate limited"
                    return record
                if resp.status_code == 200:
                    with open(target_path, 'wb') as f:
                        f.write(resp.content)
                    record["file_size_mb"] = round(len(resp.content) / (1024 * 1024), 2)
                    logger.info(f"Downloaded {file_name} ({record['file_size_mb']} MB)")
                    break
                else:
                    record["read_status"] = "FAILED"
                    record["error_message"] = f"HTTP {resp.status_code}: File not available"
                    return record
            except (ConnectionError, ChunkedEncodingError, Timeout) as e:
                if attempt < max_retries:
                    wait = 2 ** attempt
                    logger.warning(f"Connection error for {file_name}, retrying in {wait}s (attempt {attempt}/{max_retries}): {e}")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to download {file_name} after {max_retries} attempts: {e}")
                    record["read_status"] = "FAILED"
                    record["error_message"] = f"Connection error after {max_retries} retries: {e}"
                    return record
        time.sleep(0.5)
    else:
        record["file_size_mb"] = round(os.path.getsize(target_path) / (1024 * 1024), 2)
    
    return record


def read_parquet_metadata(spark, record):
    from datetime import datetime
    import hashlib
    
    target_path = record["file_path"]
    file_name = record["file_name"]
    
    try:
        df = spark.read.parquet(target_path)
        record["record_count"] = df.count()
        record["column_count"] = len(df.columns)
        record["schema_hash"] = hashlib.md5(str(df.schema).encode()).hexdigest()[:16]
        record["processed_at"] = datetime.now().isoformat()
        logger.info(f"Read {file_name}: {record['record_count']} records, {record['column_count']} columns")
    except Exception as e:
        record["read_status"] = "FAILED"
        record["error_message"] = str(e)[:500]
        logger.warning(f"Failed to read {file_name}: {e}")
    
    return record


def download_and_inventory(spark, services, years):
    from datetime import datetime
    import os
    
    inventory = []
    total = 0
    success = 0
    failed = 0
    
    for service in services:
        service_path = os.path.join(RAW_PATH, service)
        Path(service_path).mkdir(parents=True, exist_ok=True)
        
        for year in years:
            months = get_months(year)
            for month in months:
                rec = download_trip_data(service, year, month)
                rec = read_parquet_metadata(spark, rec)
                rec["process_id"] = rec.get("process_id") or ""
                rec["error_message"] = rec.get("error_message") or ""
                rec["schema_hash"] = rec.get("schema_hash") or ""
                rec["file_size_mb"] = rec.get("file_size_mb") or 0
                rec["record_count"] = rec.get("record_count") or 0
                rec["column_count"] = rec.get("column_count") or 0
                
                if rec["read_status"] == "FAILED":
                    failed += 1
                else:
                    success += 1
                total += 1
                
                inventory.append(rec)
    
    logger.info(f"Inventory: {total} files, {success} successful, {failed} failed")
    return inventory


def process_bad_parquet(spark):
    bad_dir = os.path.join(RAW_PATH, "bad_parquet")
    if not os.path.exists(bad_dir):
        logger.warning("bad_parquet directory not found")
        return [], []
    
    inventory = []
    quarantine_records = []
    
    for fname in os.listdir(bad_dir):
        if not fname.endswith(".parquet"):
            continue
        
        fpath = os.path.join(bad_dir, fname)
        record = {
            "process_id": "",
            "source_system": "NYC_TLC_TEST",
            "service_type": "bad_parquet",
            "file_name": fname,
            "file_path": fpath,
            "file_size_mb": round(os.path.getsize(fpath) / (1024 * 1024), 2),
            "partition_year": None,
            "partition_month": None,
            "read_status": "FAILED",
            "record_count": None,
            "column_count": None,
            "schema_hash": None,
            "error_message": None,
            "processed_at": datetime.now().isoformat()
        }
        
        try:
            df = spark.read.parquet(fpath)
            record["read_status"] = "SUCCESS"
            record["record_count"] = df.count()
            record["column_count"] = len(df.columns)
            logger.info(f"Bad parquet {fname} was actually readable!")
        except Exception as e:
            err_msg = str(e)[:500]
            record["error_message"] = err_msg
            logger.warning(f"Bad parquet {fname} failed as expected: {err_msg[:100]}")
            
            category = classify_error(err_msg)
            quarantine_records.append({
                "process_id": "",
                "file_name": fname,
                "service_type": "bad_parquet",
                "rejection_category": category,
                "error_message": err_msg,
                "stage": "bad_parquet_test",
                "recommended_action": "Redownload from source" if category.startswith("NOT") else "Attempt schema recovery",
                "quarantined_at": datetime.now().isoformat()
            })
        
        record["process_id"] = record.get("process_id") or ""
        record["partition_year"] = record.get("partition_year") or 0
        record["partition_month"] = record.get("partition_month") or 0
        record["record_count"] = record.get("record_count") or 0
        record["column_count"] = record.get("column_count") or 0
        record["schema_hash"] = record.get("schema_hash") or ""
        record["error_message"] = record.get("error_message") or ""
        record["file_size_mb"] = record.get("file_size_mb") or 0.0
        inventory.append(record)
    
    return inventory, quarantine_records
