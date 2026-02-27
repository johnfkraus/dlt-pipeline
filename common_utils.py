# common_utils.py
import re
from datetime import datetime

UNKNOWN = "Unknown"

def normalize_empty(value: str | None) -> str:
    if value is None:
        return UNKNOWN
    v = str(value).strip()
    if v == "" or v.lower() in ("none", "null"):
        return UNKNOWN
    return v

def normalize_date(value: str | None) -> str:
    v = normalize_empty(value)
    if v == UNKNOWN:
        return v
    candidates = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y%m%d",
    ]
    for fmt in candidates:
        try:
            dt = datetime.strptime(v, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return UNKNOWN

def normalize_name(value: str | None) -> str:
    v = normalize_empty(value)
    if v == UNKNOWN:
        return v
    return v.strip()

def normalize_phone_cn(value: str | None) -> str:
    v = normalize_empty(value)
    if v == UNKNOWN:
        return v
    digits = re.sub(r"\D", "", v)
    if digits.startswith("86") and len(digits) > 11:
        digits = digits[2:]
    if len(digits) == 11:
        return "+86" + digits
    return UNKNOWN
