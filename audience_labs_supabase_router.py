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
  AUDIENCE_PAGE_DELAY  Defaults to 1.5 seconds
  AUDIENCE_RETRY_WAIT_SECONDS Defaults to 30 seconds
  GEOCODE_ENABLED      Defaults to false

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
import requests


AUDIENCE_LABS_API_KEY = os.environ.get("AUDIENCE_LABS_API_KEY")
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

AUDIENCE_ID = "690932ed-86d3-4348-9851-fdec475a1db9"
TYPE_SUFFIX = os.environ.get("TYPE_SUFFIX", "hvac")
PAGE_SIZE = int(os.environ.get("AUDIENCE_PAGE_SIZE", "500"))
AUDIENCE_REQUEST_TIMEOUT = int(os.environ.get("AUDIENCE_REQUEST_TIMEOUT", "180"))
AUDIENCE_PAGE_DELAY = float(os.environ.get("AUDIENCE_PAGE_DELAY", "1.5"))
AUDIENCE_RETRY_WAIT_SECONDS = float(os.environ.get("AUDIENCE_RETRY_WAIT_SECONDS", "30"))
AUDIENCE_MAX_RETRY_WAIT_SECONDS = float(os.environ.get("AUDIENCE_MAX_RETRY_WAIT_SECONDS", "180"))
AUDIENCE_MAX_RETRIES = int(os.environ.get("AUDIENCE_MAX_RETRIES", "20"))
GEOCODE_ENABLED = os.environ.get("GEOCODE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "y"}
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
    "opelika": ["36801", "36804", "36830"],
    "san_antonio": ["78249", "78258", "78260"],
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

    if kind == "lat" and 24 <= number <= 50:
        return round(number, 7)
    if kind == "lng" and -125 <= number <= -66:
        return round(number, 7)
    return None


def geocode_address(address: str, city: str, state: str, zip_code: str) -> tuple[float | None, float | None]:
    cache_key = f"{address}|{city}|{state}|{zip_code}".lower()
    if cache_key in GEOCODE_CACHE:
        return GEOCODE_CACHE[cache_key]

    params = {
        "street": address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "benchmark": "Public_AR_Current",
        "format": "json",
    }

    try:
        response = requests.get(
            "https://geocoding.geo.census.gov/geocoder/locations/address",
            params=params,
            timeout=20,
        )
        if response.status_code != 200:
            print(f"Geocode failed for {address}, {city} {zip_code}: HTTP {response.status_code}")
            GEOCODE_CACHE[cache_key] = (None, None)
            return GEOCODE_CACHE[cache_key]

        matches = response.json().get("result", {}).get("addressMatches", [])
        if not matches:
            GEOCODE_CACHE[cache_key] = (None, None)
            return GEOCODE_CACHE[cache_key]

        coordinates = matches[0].get("coordinates", {})
        lat = normalize_coordinate(coordinates.get("y"), kind="lat")
        lng = normalize_coordinate(coordinates.get("x"), kind="lng")
        GEOCODE_CACHE[cache_key] = (lat, lng) if lat is not None and lng is not None else (None, None)
        time.sleep(GEOCODE_SLEEP_SECONDS)
        return GEOCODE_CACHE[cache_key]
    except Exception as exc:
        print(f"Geocode error for {address}, {city} {zip_code}: {exc}")
        GEOCODE_CACHE[cache_key] = (None, None)
        return GEOCODE_CACHE[cache_key]


def get_best_phone(row: dict[str, Any]) -> PhoneCandidate | None:
    score = numeric_score(first_present(row, "SKIPTRACE_MATCH_SCORE"))
    if score < MIN_SKIPTRACE_MATCH_SCORE:
        return None

    if str(first_present(row, "SKIPTRACE_DNC")).strip().upper() in {"Y", "YES", "TRUE", "1"}:
        return None

    phone_value = row.get("SKIPTRACE_WIRELESS_NUMBERS")
    if is_blank(phone_value):
        return None

    phones = str(phone_value).split(",")

    for index, raw_phone in enumerate(phones):
        phone = normalize_phone(raw_phone)
        if phone:
            return PhoneCandidate(
                number=phone,
                source="skiptrace_wireless",
                dnc_status=first_present(row, "SKIPTRACE_DNC"),
                match_score=score,
                match_quality=f"skiptrace_wireless_score_{score}",
            )

    return None


def get_safe_phone(row: dict[str, Any]) -> str:
    candidate = get_best_phone(row)
    return candidate.number if candidate else ""


def router_run_timestamp() -> str:
    run_date = datetime.now(timezone.utc).date()
    return f"{run_date.isoformat()}T09:30:00+00:00"


def process_lead(row: dict[str, Any]) -> dict[str, Any] | None:
    name = f"{first_present(row, 'FIRST_NAME')} {first_present(row, 'LAST_NAME')}".strip()
    if not name:
        name = first_present(row, "SKIPTRACE_NAME")

    address = first_present(row, "PERSONAL_ADDRESS", "SKIPTRACE_ADDRESS")
    city = first_present(row, "PERSONAL_CITY", "SKIPTRACE_CITY")
    state = first_present(row, "PERSONAL_STATE", "SKIPTRACE_STATE")
    zip_code = normalize_zip(first_present(row, "PERSONAL_ZIP", "SKIPTRACE_ZIP"))
    phone_candidate = get_best_phone(row)
    phone = phone_candidate.number if phone_candidate else None

    if not name or not address or not city or not state or not zip_code:
        return None

    if zip_code not in ZIP_TO_REGION:
        return None

    if state.upper() not in {"CA", "AL", "TX"}:
        return None

    commercial_keywords = r"\b(commercial|business|office|industrial|warehouse|retail|storefront|shop|factory|plant|mall|plaza|center|centre)\b"
    if re.search(commercial_keywords, address.lower()):
        return None

    name_parts = name.split()
    first_name = name_parts[0] if name_parts else ""
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

    if not first_name or not last_name:
        return None

    email = first_present(row, "PERSONAL_VERIFIED_EMAILS", "PERSONAL_VERIFIED_EMAIL", "PERSONAL_EMAILS")
    timestamp = router_run_timestamp()
    lat = normalize_coordinate(
        first_present(row, "LATITUDE", "PROPERTY_LATITUDE", "PERSONAL_LATITUDE", "SKIPTRACE_LATITUDE", "lat"),
        kind="lat",
    )
    lng = normalize_coordinate(
        first_present(row, "LONGITUDE", "PROPERTY_LONGITUDE", "PROPERTY_LON", "PERSONAL_LONGITUDE", "SKIPTRACE_LONGITUDE", "lng", "lon"),
        kind="lng",
    )

    if GEOCODE_ENABLED and (lat is None or lng is None):
        lat, lng = geocode_address(address, city, state.upper(), zip_code)

    return {
        "FIRST_NAME": first_name,
        "LAST_NAME": last_name,
        "PERSONAL_ADDRESS": address,
        "PERSONAL_CITY": city,
        "PERSONAL_STATE": state.upper(),
        "PERSONAL_ZIP": zip_code,
        "LATITUDE": lat,
        "LONGITUDE": lng,
        "SKIPTRACE_WIRELESS_NUMBERS": phone,
        "PERSONAL_VERIFIED_EMAIL": email,
        "NET_WORTH": first_present(row, "NET_WORTH"),
        "INCOME_RANGE": first_present(row, "INCOME_RANGE"),
        "PHONE_SOURCE": phone_candidate.source if phone_candidate else "",
        "PHONE_DNC_STATUS": phone_candidate.dnc_status if phone_candidate else "",
        "PHONE_MATCH_SCORE": phone_candidate.match_score if phone_candidate else "",
        "PHONE_MATCH_QUALITY": phone_candidate.match_quality if phone_candidate else "",
        "time_stamp": timestamp,
    }


def fetch_audience_rows() -> list[dict[str, Any]]:
    headers = {"X-Api-Key": AUDIENCE_LABS_API_KEY}
    rows: list[dict[str, Any]] = []
    page = 1
    retries = 0
    max_retries = AUDIENCE_MAX_RETRIES
    current_page_delay = max(AUDIENCE_PAGE_DELAY, 0.0)
    last_batch_size: int | None = None
    session = requests.Session()
    session.headers.update(headers)

    print(
        f"Fetching Audience Labs list: {AUDIENCE_ID} "
        f"(page_size={PAGE_SIZE}, page_delay={current_page_delay}s, retry_wait={AUDIENCE_RETRY_WAIT_SECONDS}s, geocode_enabled={GEOCODE_ENABLED})"
    )

    while True:
        try:
            response = session.get(
                f"https://api.audiencelab.io/audiences/{AUDIENCE_ID}",
                params={"page": page, "page_size": PAGE_SIZE},
                timeout=(10, AUDIENCE_REQUEST_TIMEOUT),
            )
        except requests.RequestException as exc:
            retries += 1
            if retries > max_retries:
                raise RuntimeError(f"Audience Labs kept failing on page {page}: {exc}") from exc

            wait_seconds = min(AUDIENCE_RETRY_WAIT_SECONDS * retries, AUDIENCE_MAX_RETRY_WAIT_SECONDS)
            current_page_delay = min(max(current_page_delay, AUDIENCE_PAGE_DELAY) + 0.25, 3.0)
            print(
                f"Audience Labs request failed on page {page}: {exc}. "
                f"Waiting {wait_seconds} seconds ({retries}/{max_retries})..."
            )
            time.sleep(wait_seconds)
            continue

        if response.status_code in {429, 500, 502, 503, 504}:
            retries += 1
            if retries > max_retries:
                raise RuntimeError(f"Audience Labs kept failing with HTTP {response.status_code}")

            wait_seconds = min(AUDIENCE_RETRY_WAIT_SECONDS * retries, AUDIENCE_MAX_RETRY_WAIT_SECONDS)
            current_page_delay = min(max(current_page_delay, AUDIENCE_PAGE_DELAY) + 0.25, 3.0)
            print(
                f"Audience Labs returned HTTP {response.status_code} on page {page}. "
                f"Waiting {wait_seconds} seconds ({retries}/{max_retries})..."
            )
            time.sleep(wait_seconds)
            continue

        if response.status_code != 200:
            raise RuntimeError(f"Audience Labs HTTP {response.status_code}: {response.text}")

        retries = 0
        payload = response.json()
        data = payload.get("data", []) if isinstance(payload, dict) else payload
        batch_size = len(data) if isinstance(data, list) else 0

        if not data:
            print(f"Finished fetching at page {page - 1}.")
            break

        if last_batch_size is None:
            last_batch_size = batch_size
            if batch_size and batch_size < PAGE_SIZE:
                print(
                    f"Audience Labs is effectively returning {batch_size} rows per page "
                    f"even though page_size={PAGE_SIZE} was requested."
                )

        rows.extend(data)
        if page % 10 == 0:
            print(f"Downloaded {len(rows)} raw rows...")

        if current_page_delay > 0 and page % 25 == 0:
            current_page_delay = max(AUDIENCE_PAGE_DELAY, round(current_page_delay - 0.05, 2))

        page += 1
        if current_page_delay > 0:
            time.sleep(current_page_delay)

    print(f"Extracted {len(rows)} raw rows from Audience Labs.")
    return rows


def clean_and_dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clean_rows = [lead for row in rows if (lead := process_lead(row)) is not None]
    print(
        f"Kept {len(clean_rows)} valid residential opportunities in your target ZIPs "
        f"with skiptraced wireless phones and match score >= {MIN_SKIPTRACE_MATCH_SCORE}."
    )

    clean_rows.sort(key=lambda row: str(row.get("time_stamp") or ""), reverse=True)

    unique_by_phone: dict[str, dict[str, Any]] = {}
    for row in clean_rows:
        phone = row.get("SKIPTRACE_WIRELESS_NUMBERS")
        if phone and phone not in unique_by_phone:
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


def insert_rows(table_name: str, rows: list[dict[str, Any]]) -> int:
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=SKIPTRACE_WIRELESS_NUMBERS,time_stamp"
    total = 0

    for start in range(0, len(rows), 500):
        chunk = rows[start : start + 500]
        response = requests.post(
            url,
            headers=supabase_headers("resolution=ignore-duplicates,return=minimal"),
            json=chunk,
            timeout=90,
        )
        if response.status_code not in {200, 201}:
            raise RuntimeError(f"Supabase insert failed for {table_name}: HTTP {response.status_code} {response.text}")
        total += len(chunk)

    return total


def route_to_supabase(rows: list[dict[str, Any]]) -> None:
    routed: dict[str, list[dict[str, Any]]] = {region: [] for region in REGION_ZIPS}

    for row in rows:
        region = ZIP_TO_REGION[row["PERSONAL_ZIP"]]
        routed[region].append({key: row.get(key, "") for key in ALLOWED_COLUMNS})

    for region, region_rows in routed.items():
        if not region_rows:
            continue

        table_name = f"{region}_{TYPE_SUFFIX}"
        print(f"Inserting {len(region_rows)} opportunities into {table_name}; same phone can reappear in a new dated batch...")

        inserted = insert_rows(table_name, region_rows)
        print(f"Sent {inserted} candidate opportunities to {table_name}; duplicates were ignored only inside the same batch date.")


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
