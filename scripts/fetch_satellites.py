#!/usr/bin/env python3
"""
XTROBE - Nightly Active Satellite JSON Generator
Source: Celestrak SATCAT (satellite catalog)
Filters: Only operational satellites (OPS_STATUS = '+')
Output: data/active_satellites.json + data/active_satellites.json.gz
"""

import requests
import json
import csv
import gzip
import io
import os
from datetime import datetime, timezone

OUTPUT_DIR = "./data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {"User-Agent": "XTROBE-Satellite-Tracker/1.0"}
SATCAT_URL = "https://celestrak.org/pub/satcat.csv"

OWNER_MAP = {
    "US":   "United States",   "PRC":  "China",
    "CIS":  "Russia",          "ESA":  "European Space Agency",
    "ISRO": "India",           "JAXA": "Japan",
    "JPN":  "Japan",           "UK":   "United Kingdom",
    "FR":   "France",          "GER":  "Germany",
    "CA":   "Canada",          "AUS":  "Australia",
    "ISR":  "Israel",          "ITSO": "Intelsat",
    "EUTE": "Eutelsat",        "NATO": "NATO",
    "AB":   "Arab Sat",        "SKOR": "South Korea",
    "BRAZ": "Brazil",          "SPN":  "Spain",
}

ORBIT_MAP = {
    "LEO": "Low Earth Orbit",       "MEO": "Medium Earth Orbit",
    "GEO": "Geostationary Orbit",   "HEO": "Highly Elliptical Orbit",
    "IGO": "Inclined Geosync",      "EGO": "Eccentric Geosync",
    "NSO": "Near-Synchronous",      "DSO": "Deep Space",
}


def fetch_satcat():
    print("📡 Downloading Celestrak SATCAT...")
    r = requests.get(SATCAT_URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    print(f"   Raw size: {len(r.content)/1024/1024:.2f} MB")
    return r.text


def parse_and_filter(csv_text):
    print("🔍 Filtering active payloads only...")
    reader = csv.DictReader(io.StringIO(csv_text))
    satellites = []
    total = skipped = 0

    for row in reader:
        total += 1
        status   = row.get("OPS_STATUS_CODE", "").strip()
        obj_type = row.get("OBJECT_TYPE", "").strip()

        # Keep only operational/partial payloads (no rocket bodies, no debris)
        if status not in ("+", "P") or obj_type != "PAY":
            skipped += 1
            continue

        owner_code = row.get("OWNER", "").strip()
        orbit_code = row.get("ORBIT_TYPE", "").strip()

        satellites.append({
            "name":        row.get("OBJECT_NAME", "").strip(),
            "norad_id":    row.get("NORAD_CAT_ID", "").strip(),
            "status":      "Operational" if status == "+" else "Partial",
            "owner_code":  owner_code,
            "owner":       OWNER_MAP.get(owner_code, owner_code),
            "launch_date": row.get("LAUNCH_DATE", "").strip(),
            "orbit_code":  orbit_code,
            "orbit":       ORBIT_MAP.get(orbit_code, orbit_code),
            "period_min":  row.get("PERIOD", "").strip(),
            "inclination": row.get("INCLINATION", "").strip(),
            "apogee_km":   row.get("APOGEE", "").strip(),
            "perigee_km":  row.get("PERIGEE", "").strip(),
            "size":        row.get("RCS_SIZE", "").strip(),
        })

    print(f"   Total rows in catalog : {total}")
    print(f"   Skipped (inactive/debris/rockets): {skipped}")
    print(f"   ✅ Active payloads kept: {len(satellites)}")
    return satellites


def build_stats(satellites):
    orbit_counts = {}
    owner_counts = {}
    for s in satellites:
        o = s["orbit_code"] or "Unknown"
        orbit_counts[o] = orbit_counts.get(o, 0) + 1
        own = s["owner"] or "Unknown"
        owner_counts[own] = owner_counts.get(own, 0) + 1

    top_owners = dict(sorted(owner_counts.items(), key=lambda x: x[1], reverse=True)[:15])
    return {"total_active": len(satellites), "by_orbit": orbit_counts, "top_owners": top_owners}


def main():
    print("🛰️  XTROBE Satellite Fetch Started\n")

    raw_csv   = fetch_satcat()
    sats      = parse_and_filter(raw_csv)
    stats     = build_stats(sats)

    output = {
        "schema":       "xtrobe-satellites-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stats":        stats,
        "satellites":   sats,
    }

    json_str  = json.dumps(output, separators=(",", ":"), ensure_ascii=False)

    # Plain JSON
    json_path = os.path.join(OUTPUT_DIR, "active_satellites.json")
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(json_str)
    json_kb = os.path.getsize(json_path) / 1024

    # Gzip compressed (for frontend)
    gz_path = os.path.join(OUTPUT_DIR, "active_satellites.json.gz")
    with gzip.open(gz_path, "wb") as gz:
        gz.write(json_str.encode("utf-8"))
    gz_kb = os.path.getsize(gz_path) / 1024

    print(f"\n📁 JSON  → {json_path}  ({json_kb:.0f} KB)")
    print(f"📦 GZIP  → {gz_path}  ({gz_kb:.0f} KB)  ({100-gz_kb/json_kb*100:.0f}% smaller)")
    print(f"\n✅ Done!  {stats['total_active']} active satellites")
    print(f"   Orbit breakdown: {stats['by_orbit']}")


if __name__ == "__main__":
    main()
