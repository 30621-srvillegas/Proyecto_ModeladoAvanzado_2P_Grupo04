import os
import yaml
import hashlib
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

YAML_PATH = os.path.join(BASE_DIR, "config", "etl_config.yaml")

with open(YAML_PATH, "r", encoding="utf-8") as f:
    _cfg = yaml.safe_load(f)

_p = _cfg["paths"]
_ds = _cfg["data_sources"]["tlc_nyc"]

RAW_PATH = os.path.join(BASE_DIR, _p["raw"])
BRONZE_PATH = os.path.join(BASE_DIR, _p["bronze"])
SILVER_PATH = os.path.join(BASE_DIR, _p["silver"])
GOLD_PATH = os.path.join(BASE_DIR, _p["gold"])
QUARANTINE_PATH = os.path.join(BASE_DIR, _p["quarantine"])
AUDIT_PATH = os.path.join(BASE_DIR, _p["audit"])
METADATA_PATH = os.path.join(BASE_DIR, _p["metadata"])
METADATA_JSON_PATH = os.path.join(BASE_DIR, "metadata")
DB_PATH = os.path.join(BASE_DIR, _p["db"])

BASE_URL = _ds["base_url"]
SERVICES = _ds["services"]
YEARS = _ds["years"]
MONTHS = list(range(1, 13))

def get_months(year):
    mp = _ds.get("months_per_year", {})
    if str(year) in mp:
        return mp[str(year)]
    return MONTHS

SERVICE_TYPE_MAP = _cfg.get("service_type_map", {})

_schema = _cfg.get("canonical_schema", {})
CANONICAL_SCHEMA = {col["name"]: col["type"] for col in _schema.get("columns", [])}

_hom = _schema.get("homologation", {})
HOMOLOGATION_MAP = {k: v for k, v in _hom.items()}

SUSPICIOUS_RULES = {}
for rule in _cfg.get("quality", {}).get("suspicious_rules", []):
    if isinstance(rule, dict):
        SUSPICIOUS_RULES[rule["name"]] = rule.get("column", rule.get("description", ""))
    elif isinstance(rule, str):
        SUSPICIOUS_RULES[rule] = rule

RECOVERY_CATEGORIES = _cfg.get("recovery_categories", [])

PROCESS_ID = hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()[:12]
