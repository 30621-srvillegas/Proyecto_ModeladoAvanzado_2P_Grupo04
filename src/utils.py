import os
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from src.config_loader import *

logger = logging.getLogger(__name__)

def ensure_dirs():
    for path in [RAW_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH,
                 QUARANTINE_PATH, AUDIT_PATH, METADATA_PATH,
                 os.path.join(RAW_PATH, "bad_parquet")]:
        Path(path).mkdir(parents=True, exist_ok=True)
    logger.info("Directory structure verified")


def get_headers():
    return {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def generate_process_id():
    return hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()[:12]


def classify_error(error_message):
    err = (error_message or "").lower()
    if "empty" in err or "length 0" in err:
        return "NOT_RECOVERABLE_EMPTY_FILE"
    if "magic number" in err or "corrupt" in err:
        return "NOT_RECOVERABLE_CORRUPT_METADATA"
    if "not a parquet" in err or "format" in err:
        return "NOT_RECOVERABLE_UNSUPPORTED_FORMAT"
    if "schema" in err or "column" in err:
        return "RECUPERABLE_SCHEMA_MISMATCH"
    if "type" in err or "cast" in err:
        return "RECUPERABLE_TYPE_CASTING"
    return "PARTIALLY_RECOVERABLE"
