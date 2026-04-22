#!/usr/bin/env python3
"""
Export the public lead payload used by the static GitHub Pages frontend.

This runs in GitHub Actions after the Audience Labs router. It uses Supabase
secrets server-side, then writes leads-data.json for the browser to fetch.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TYPE_SUFFIX = os.environ.get("TYPE_SUFFIX", "hvac")
OUTPUT_PATH = Path(os.environ.get("LEADS_OUTPUT_PATH", "leads-data.json"))
MAX_ROWS_PER_CITY = int(os.environ.get("LEADS_EXPORT_LIMIT", "1000"))

CITY_TABLES = [
    {"slug": "murrieta", "label": "Murrieta", "table": f"murrieta_{TYPE_SUFFIX}"},
    {"slug": "temecula", "label": "Temecula", "table": f"temecula_{TYPE_SUFFIX}"},
    {"slug": "menifee", "label": "Menifee", "table": f"menifee_{TYPE_SUFFIX}"},
    {"slug": "perris", "label": "Perris", "table": f"perris_{TYPE_SUFFIX}"},
    {"slug": "riverside", "label": "Riverside", "table": f"riverside_{TYPE_SUFFIX}"},
    {"slug": "oceanside", "label": "Oceanside", "table": f"oceanside_{TYPE_SUFFIX}"},
    {"slug": "corona", "label": "Corona", "table": f"corona_{TYPE_SUFFIX}"},
    {"slug": "lake_elsinore", "label": "Lake Elsinore", "table": f"lake_elsinore_{TYPE_SUFFIX}"},
    {"slug": "moreno_valley", "label": "Moreno Valley", "table": f"moreno_valley_{TYPE_SUFFIX}"},
    {"slug": "opelika", "label": "Opelika", "table": f"opelika_{TYPE_SUFFIX}"},
    {"slug": "san_antonio", "label": "San Antonio", "table": f"san_antonio_{TYPE_SUFFIX}"},
]

CORE_COLUMNS = [
        "FIRST_NAME",
        "LAST_NAME",
        "PERSONAL_VERIFIED_EMAIL",
        "SKIPTRACE_WIRELESS_NUMBERS",
        "PERSONAL_ADDRESS",
        "PERSONAL_CITY",
        "PERSONAL_STATE",
        "PERSONAL_ZIP",
        "time_stamp",
        "created_at",
]

ENRICH_COLUMNS = [
        "LATITUDE",
        "LONGITUDE",
        "NET_WORTH",
        "INCOME_RANGE",
]

SELECT_COLUMNS = ",".join(CORE_COLUMNS + ENRICH_COLUMNS)


def require_env() -> None:
    missing = [name for name, value in {"SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY}.items() if not value]
    if missing:
        sys.exit(f"Missing required environment variable(s): {', '.join(missing)}")


def headers() -> dict[str, str]:
    return {
        "apikey": SUPABASE_KEY or "",
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    }


def parse_money_value(value: Any) -> float:
    text = str(value or "").lower()
    if not text:
        return 0
    matches = re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?", text)
    amounts: list[float] = []
    for match in matches:
        amount = float(match.replace(",", ""))
        if "k" in text and amount < 1000:
            amount *= 1000
        if "m" in text and amount < 1_000_000:
            amount *= 1_000_000
        amounts.append(amount)
    return max(amounts) if amounts else 0


def income_points(income_range: Any) -> int:
    income = parse_money_value(income_range)
    if income >= 250_000:
        return 8
    if income >= 200_000:
        return 7
    if income >= 150_000:
        return 6
    if income >= 100_000:
        return 4
    if income >= 75_000:
        return 2
    return 0


def net_worth_points(net_worth: Any) -> int:
    worth = parse_money_value(net_worth)
    if worth >= 1_000_000:
        return 2
    if worth >= 500_000:
        return 1
    return 0


def lead_score(row: dict[str, Any]) -> int:
    score = 75
    if row.get("FIRST_NAME") and row.get("LAST_NAME"):
        score += 3
    if row.get("SKIPTRACE_WIRELESS_NUMBERS"):
        score += 4
    if row.get("PERSONAL_VERIFIED_EMAIL"):
        score += 3
    if row.get("PERSONAL_ADDRESS") and row.get("PERSONAL_CITY") and row.get("PERSONAL_STATE") and row.get("PERSONAL_ZIP"):
        score += 5
    score += income_points(row.get("INCOME_RANGE"))
    score += net_worth_points(row.get("NET_WORTH"))
    return min(100, max(75, score))


def signal_strength(score: int) -> int:
    if score >= 94:
        return 4
    if score >= 86:
        return 3
    if score >= 80:
        return 2
    return 1


def clean_zip(value: Any) -> str:
    match = re.search(r"\d{5}", str(value or ""))
    return match.group(0) if match else ""


def clean_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return str(value or "")


def clean_coordinate(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def normalize_lead(row: dict[str, Any], city: dict[str, str]) -> dict[str, Any]:
    first = str(row.get("FIRST_NAME") or "").strip()
    last = str(row.get("LAST_NAME") or "").strip()
    score = lead_score(row)
    return {
        "name": f"{first} {last}".strip() or "Unknown Contact",
        "phone": clean_phone(row.get("SKIPTRACE_WIRELESS_NUMBERS")),
        "address": row.get("PERSONAL_ADDRESS") or "",
        "city": city["label"],
        "region": city["slug"],
        "table": city["table"],
        "zip": clean_zip(row.get("PERSONAL_ZIP")),
        "email": row.get("PERSONAL_VERIFIED_EMAIL") or "",
        "lat": clean_coordinate(row.get("LATITUDE")),
        "lng": clean_coordinate(row.get("LONGITUDE")),
        "incomeRange": row.get("INCOME_RANGE") or "",
        "netWorth": row.get("NET_WORTH") or "",
        "score": score,
        "sig": signal_strength(score),
        "type": "HVAC",
        "timeStamp": row.get("time_stamp") or row.get("created_at") or "",
    }


def fetch_city(city: dict[str, str]) -> list[dict[str, Any]]:
    url = (
        f"{SUPABASE_URL}/rest/v1/{city['table']}"
        f"?select={SELECT_COLUMNS}&order=created_at.desc&limit={MAX_ROWS_PER_CITY}"
    )
    response = requests.get(url, headers=headers(), timeout=90)
    if response.status_code == 400:
        fallback_columns = ",".join(CORE_COLUMNS)
        url = (
            f"{SUPABASE_URL}/rest/v1/{city['table']}"
            f"?select={fallback_columns}&order=created_at.desc&limit={MAX_ROWS_PER_CITY}"
        )
        response = requests.get(url, headers=headers(), timeout=90)
    if response.status_code != 200:
        raise RuntimeError(f"{city['table']} failed: HTTP {response.status_code} {response.text}")
    return [normalize_lead(row, city) for row in response.json()]


def main() -> int:
    require_env()
    leads: list[dict[str, Any]] = []
    errors: list[str] = []

    for city in CITY_TABLES:
        try:
            city_leads = fetch_city(city)
            leads.extend(city_leads)
            print(f"Exported {len(city_leads)} leads from {city['table']}.")
        except Exception as exc:
            errors.append(str(exc))
            print(f"WARNING: {exc}", file=sys.stderr)

    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "type": TYPE_SUFFIX,
        "cityTables": CITY_TABLES,
        "count": len(leads),
        "errors": errors,
        "leads": leads,
    }
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(leads)} leads to {OUTPUT_PATH}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
