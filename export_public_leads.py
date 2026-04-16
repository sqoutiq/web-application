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
]

SELECT_COLUMNS = ",".join(
    [
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
)


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


def hash_text(text: Any) -> int:
    value = str(text or "")
    result = 0
    for char in value:
        result = ((result << 5) - result) + ord(char)
        result &= 0xFFFFFFFF
    return abs(result if result < 0x80000000 else result - 0x100000000)


def lead_score(row: dict[str, Any]) -> int:
    key = row.get("SKIPTRACE_WIRELESS_NUMBERS") or row.get("PERSONAL_ADDRESS") or row.get("PERSONAL_ZIP")
    return 75 + (hash_text(key) % 26)


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
