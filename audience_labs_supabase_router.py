#!/usr/bin/env python3
"""
Fetch one hardcoded Audience Labs list, clean/validate the leads, and route them
into Supabase tables by region.

Required environment variables:
  AUDIENCE_LABS_API_KEY
  SUPABASE_URL
  SUPABASE_KEY

Optional environment variables:
  TYPE_SUFFIX          Defaults to "hvac"
  AUDIENCE_PAGE_SIZE   Defaults to 500

The Supabase table name is built as:
  <region_slug>_<TYPE_SUFFIX>

Example:
  murrieta_hvac
  temecula_hvac
"""

from __future__ import annotations

from dataclasses import dataclass
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests


AUDIENCE_LABS_API_KEY = os.environ.get("AUDIENCE_LABS_API_KEY")
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

AUDIENCE_ID = "690932ed-86d3-4348-9851-fdec475a1db9"
TYPE_SUFFIX = os.environ.get("TYPE_SUFFIX", "hvac")
PAGE_SIZE = int(os.environ.get("AUDIENCE_PAGE_SIZE", "500"))
GEOCODE_SLEEP_SECONDS = float(os.environ.get("GEOCODE_SLEEP_SECONDS", "0.15"))
MIN_SKIPTRACE_MATCH_SCORE = int(os.environ.get("MIN_SKIPTRACE_MATCH_SCORE", "5"))

REGION_ZIPS = {
    "murrieta": ["92562", "92563", "92564", "92595"],
    "temecula": ["92589", "92590", "92591", "92592", "92593", "92028", "92596", "92536"],
    "menifee": ["92584", "92585", "92586", "92587", "92548", "92567"],
    "perris": ["92570", "92571", "92572", "92599"],
    "riverside": [
        "92501",
        "92503",
        "92504",
        "92505",
        "92506",
        "92507",
        "92508",
        "92518",
        "92324",
        "92313",
        "91752",
        "92860",
    ],
    "oceanside": ["92054", "92056", "92057", "92058", "92081", "92083", "92084", "92008", "92010", "92003"],
    "corona": ["92877", "92878", "92879", "92880", "92881", "92882", "92883", "92870"],
    "lake_elsinore": ["92530", "92531", "92532"],
    "moreno_valley": ["92551", "92552", "92553", "92554", "92555", "92556", "92557", "92373", "92223"],
}

ZIP_TO_REGION = {zip_code: region for region, zips in REGION_ZIPS.items() for zip_code in zips}
GEOCODE_CACHE: dict[str, tuple[float | None, float | None]] = {}

ALLOWED_COLUMNS = [
    "FIRST_NAME",
    "LAST_NAME",
    "PERSONAL_VERIFIED_EMAIL",
    "SKIPTRACE_WIRELESS_NUMBERS",
    "PERSONAL_ADDRESS",
    "PERSONAL_CITY",
    "PERSONAL_STATE",
    "PERSONAL_ZIP",
    "LATITUDE",
    "LONGITUDE",
    "NET_WORTH",
    "INCOME_RANGE",
    "time_stamp",
]


@dataclass(frozen=True)
class PhoneCandidate:
    number: str
    source: str
    dnc_status: str
    match_score: int
    match_quality: str


def require_env() -> None:
    missing = [
        name
        for name, value in {
            "AUDIENCE_LABS_API_KEY": AUDIENCE_LABS_API_KEY,
            "SUPABASE_URL": SUPABASE_URL,
            "SUPABASE_KEY": SUPABASE_KEY,
        }.items()
        if not value
    ]
    if missing:
        sys.exit(f"Missing required environment variable(s): {', '.join(missing)}")


def is_blank(value: Any) -> bool:
    return value is None or str(value).strip().lower() in {"", "nan", "none", "null"}


def first_present(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if not is_blank(value):
            return str(value).strip()
    return ""


def normalize_zip(value: Any) -> str:
    match = re.search(r"\d{5}", str(value or ""))
    return match.group(0) if match else ""


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else ""


def normalize_words(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", str(value or "").lower())).strip()


def numeric_prefix(value: Any) -> str:
    match = re.search(r"\d+", str(value or ""))
    return match.group(0) if match else ""


def numeric_score(value: Any) -> int:
    match = re.search(r"\d+", str(value or ""))
    return int(match.group(0)) if match else 0


def skiptrace_identity_matches(row: dict[str, Any]) -> bool:
    score = numeric_score(first_present(row, "SKIPTRACE_MATCH_SCORE"))
    if score < MIN_SKIPTRACE_MATCH_SCORE:
        return False

    first = normalize_words(first_present(row, "FIRST_NAME"))
    last = normalize_words(first_present(row, "LAST_NAME"))
    skip_name = normalize_words(first_present(row, "SKIPTRACE_NAME"))
    if not first or not last or first not in skip_name or last not in skip_name:
        return False

    personal_zip = normalize_zip(first_present(row, "PERSONAL_ZIP"))
    skip_zip = normalize_zip(first_present(row, "SKIPTRACE_ZIP"))
    if personal_zip and skip_zip and personal_zip != skip_zip:
        return False

    personal_number = numeric_prefix(first_present(row, "PERSONAL_ADDRESS"))
    skip_number = numeric_prefix(first_present(row, "SKIPTRACE_ADDRESS"))
    if personal_number and skip_number and personal_number != skip_number:
        return False

    return True


def dnc_flag_for_index(row: dict[str, Any], dnc_col: str, index: int) -> str:
    if is_blank(row.get(dnc_col)):
        return ""
    flags = str(row.get(dnc_col)).split(",")
    return flags[index].strip().upper() if index < len(flags) else ""


def normalize_coordinate(value: Any, *, kind: str) -> float | None:
    if is_blank(value):
        return None

    try:
        number = float(str(value).strip())
    except ValueError:
        return None

    if kind == "lat" and 32 <= number <= 35:
        return round(number, 7)
    if kind == "lng" and -119 <= number <= -116:
        return round(number, 7)
    return None


def geocode_address(address: str, city: str, state: str, zip_code: str) -> tuple[float | None, float | None]:
