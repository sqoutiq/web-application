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

ALLOWED_COLUMNS = [
    "FIRST_NAME",
    "LAST_NAME",
    "PERSONAL_VERIFIED_EMAIL",
    "SKIPTRACE_WIRELESS_NUMBERS",
    "PERSONAL_ADDRESS",
    "PERSONAL_CITY",
    "PERSONAL_STATE",
    "PERSONAL_ZIP",
    "NET_WORTH",
    "INCOME_RANGE",
    "time_stamp",
]


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


def get_safe_phone(row: dict[str, Any]) -> str:
    phone_columns = [
        ("VALID_PHONES", None),
        ("MOBILE_PHONE", "MOBILE_PHONE_DNC"),
        ("PERSONAL_PHONE", "PERSONAL_PHONE_DNC"),
        ("SKIPTRACE_WIRELESS_NUMBERS", None),
    ]

    for phone_col, dnc_col in phone_columns:
        phone_value = row.get(phone_col)
        if is_blank(phone_value):
            continue

        phones = str(phone_value).split(",")
        dnc_flags = str(row.get(dnc_col)).split(",") if dnc_col and not is_blank(row.get(dnc_col)) else []

        for index, raw_phone in enumerate(phones):
            phone = normalize_phone(raw_phone)
            if not phone:
                continue

            if dnc_col:
                # Missing DNC flags are treated as unsafe.
                flag = dnc_flags[index].strip().upper() if index < len(dnc_flags) else "Y"
                if flag != "N":
                    continue

            return phone

    return ""


def process_lead(row: dict[str, Any]) -> dict[str, Any] | None:
    name = first_present(row, "SKIPTRACE_NAME")
    if not name:
        name = f"{first_present(row, 'FIRST_NAME')} {first_present(row, 'LAST_NAME')}".strip()

    address = first_present(row, "SKIPTRACE_ADDRESS", "PERSONAL_ADDRESS")
    city = first_present(row, "SKIPTRACE_CITY", "PERSONAL_CITY")
    state = first_present(row, "SKIPTRACE_STATE", "PERSONAL_STATE")
    zip_code = normalize_zip(first_present(row, "SKIPTRACE_ZIP", "PERSONAL_ZIP"))
    phone = get_safe_phone(row)

    if not name or not address or not city or not state or not zip_code or not phone:
        return None

    if zip_code not in ZIP_TO_REGION:
        return None

    if state.upper() != "CA":
        return None

    commercial_or_unit_keywords = r"\b(ste|ste\.|suite|apt|apt\.|unit|bldg|building|floor|fl|lot|spc|space)\b|#"
    if re.search(commercial_or_unit_keywords, address.lower()):
        return None

    name_parts = name.split()
    first_name = name_parts[0] if name_parts else ""
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

    if not first_name or not last_name:
        return None

    email = first_present(row, "PERSONAL_VERIFIED_EMAILS", "PERSONAL_VERIFIED_EMAIL", "PERSONAL_EMAILS")
    timestamp = first_present(row, "time_stamp", "created_at", "updated_at") or datetime.now(timezone.utc).isoformat()

    return {
        "FIRST_NAME": first_name,
        "LAST_NAME": last_name,
        "PERSONAL_ADDRESS": address,
        "PERSONAL_CITY": city,
        "PERSONAL_STATE": state.upper(),
        "PERSONAL_ZIP": zip_code,
        "SKIPTRACE_WIRELESS_NUMBERS": phone,
        "PERSONAL_VERIFIED_EMAIL": email,
        "NET_WORTH": first_present(row, "NET_WORTH"),
        "INCOME_RANGE": first_present(row, "INCOME_RANGE"),
        "time_stamp": timestamp,
    }


def fetch_audience_rows() -> list[dict[str, Any]]:
    headers = {"X-Api-Key": AUDIENCE_LABS_API_KEY}
    rows: list[dict[str, Any]] = []
    page = 1
    retries = 0
    max_retries = 20

    print(f"Fetching Audience Labs list: {AUDIENCE_ID}")

    while True:
        url = f"https://api.audiencelab.io/audiences/{AUDIENCE_ID}?page={page}&page_size={PAGE_SIZE}"
        response = requests.get(url, headers=headers, timeout=90)

        if response.status_code in {429, 500, 502, 503, 504}:
            retries += 1
            if retries > max_retries:
                raise RuntimeError(f"Audience Labs kept failing with HTTP {response.status_code}")
            print(f"Audience Labs returned HTTP {response.status_code}. Waiting 30 seconds ({retries}/{max_retries})...")
            time.sleep(30)
            continue

        if response.status_code != 200:
            raise RuntimeError(f"Audience Labs HTTP {response.status_code}: {response.text}")

        retries = 0
        payload = response.json()
        data = payload.get("data", []) if isinstance(payload, dict) else payload

        if not data:
            print(f"Finished fetching at page {page - 1}.")
            break

        rows.extend(data)
        if page % 10 == 0:
            print(f"Downloaded {len(rows)} raw rows...")

        page += 1
        time.sleep(1.5)

    print(f"Extracted {len(rows)} raw rows from Audience Labs.")
    return rows


def clean_and_dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clean_rows = [lead for row in rows if (lead := process_lead(row)) is not None]
    print(f"Kept {len(clean_rows)} valid residential CA leads in your target ZIPs.")

    clean_rows.sort(key=lambda row: str(row.get("time_stamp") or ""), reverse=True)

    unique_by_phone: dict[str, dict[str, Any]] = {}
    for row in clean_rows:
        phone = row["SKIPTRACE_WIRELESS_NUMBERS"]
        if phone not in unique_by_phone:
            unique_by_phone[phone] = row

    deduped = list(unique_by_phone.values())
    print(f"Removed duplicates inside this pull. {len(deduped)} unique leads remain.")
    return deduped


def supabase_headers(prefer: str | None = None) -> dict[str, str]:
    headers = {
        "apikey": SUPABASE_KEY or "",
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


def delete_existing_phone(table_name: str, phone: str) -> None:
    encoded_phone = quote(phone, safe="")
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?SKIPTRACE_WIRELESS_NUMBERS=eq.{encoded_phone}"
    response = requests.delete(url, headers=supabase_headers(), timeout=60)
    if response.status_code not in {200, 204}:
        raise RuntimeError(f"Supabase delete failed for {table_name}/{phone}: HTTP {response.status_code} {response.text}")


def delete_existing_phone_from_all_route_tables(phone: str) -> None:
    for region in REGION_ZIPS:
        delete_existing_phone(f"{region}_{TYPE_SUFFIX}", phone)


def insert_rows(table_name: str, rows: list[dict[str, Any]]) -> int:
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    total = 0

    for start in range(0, len(rows), 500):
        chunk = rows[start : start + 500]
        response = requests.post(url, headers=supabase_headers("return=minimal"), json=chunk, timeout=90)
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"Supabase insert failed for {table_name}: HTTP {response.status_code} {response.text}")
        total += len(chunk)

    return total


def route_to_supabase(rows: list[dict[str, Any]]) -> None:
    routed: dict[str, list[dict[str, Any]]] = {region: [] for region in REGION_ZIPS}

    for row in rows:
        region = ZIP_TO_REGION[row["PERSONAL_ZIP"]]
        routed[region].append({key: row.get(key, "") for key in ALLOWED_COLUMNS})

    phones = sorted({row["SKIPTRACE_WIRELESS_NUMBERS"] for row in rows})
    print(f"Deleting existing Supabase duplicates for {len(phones)} phones across all route tables...")
    for index, phone in enumerate(phones, start=1):
        delete_existing_phone_from_all_route_tables(phone)
        if index % 100 == 0:
            print(f"Checked/deleted duplicates for {index}/{len(phones)} phones...")

    for region, region_rows in routed.items():
        if not region_rows:
            continue

        table_name = f"{region}_{TYPE_SUFFIX}"
        print(f"Refreshing {len(region_rows)} leads in {table_name}...")

        inserted = insert_rows(table_name, region_rows)
        print(f"Sent {inserted} fresh leads to {table_name}.")


def main() -> int:
    require_env()
    raw_rows = fetch_audience_rows()
    if not raw_rows:
        print("No Audience Labs rows found. Nothing to push.")
        return 0

    clean_rows = clean_and_dedupe(raw_rows)
    if not clean_rows:
        print("No valid leads remained after filtering. Nothing to push.")
        return 0

    route_to_supabase(clean_rows)
    print("Routing complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
