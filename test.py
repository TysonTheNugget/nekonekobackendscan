# dump_entries.py
# Reads all tx:* hashes from Upstash and prints ONLY:
# inscriptionId, buyerAddr, watchedAddr, status, seenAt, confirmedAt, serial, pngText (as dict)
#
# Outputs:
# - prints each entry to console
# - writes entries.jsonl (JSON-lines)
# - writes entries-pretty.json (pretty JSON array)

import os
import json
import requests

# ---- Configure your Upstash credentials ----
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL", "https://game-raptor-60247.upstash.io")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "AutXAAIgcDGxqqCgE7YcdsvGaa7tTmGYQn-EXlTpJAizKum1zEr2FQ")

HEADERS = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}

FIELDS = [
    "inscriptionId",
    "buyerAddr",
    "watchedAddr",
    "status",
    "seenAt",
    "confirmedAt",
    "serial",
    "pngText",
]

def scan(pattern=None, count=1000):
    """Full SCAN across keyspace, optional match pattern."""
    cursor = "0"
    keys = []
    while True:
        url = f"{UPSTASH_REDIS_REST_URL}/scan/{cursor}?count={count}"
        if pattern:
            url += f"&match={pattern}"
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        # Upstash may return dict or list depending on plan/version
        if isinstance(data, dict):
            cursor = data.get("cursor", "0")
            page = data.get("keys", [])
        else:
            cursor, page = data[0], data[1]
        keys.extend(page)
        if cursor == "0":
            break
    return keys

def hgetall(key):
    r = requests.get(f"{UPSTASH_REDIS_REST_URL}/hgetall/{key}", headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def normalize_entry(raw):
    """Keep only desired fields, coerce types, parse pngText JSON if present."""
    out = {}
    for f in FIELDS:
        out[f] = raw.get(f, None)

    # ints if strings
    for k in ("seenAt", "confirmedAt"):
        if out.get(k) in ("", None):
            out[k] = None
        else:
            try:
                out[k] = int(out[k])
            except Exception:
                pass

    # pngText may be a JSON string — parse when possible
    if isinstance(out.get("pngText"), str) and out["pngText"]:
        try:
            out["pngText"] = json.loads(out["pngText"])
        except Exception:
            # leave as raw string if not valid JSON
            pass

    return out

def main():
    tx_keys = scan(pattern="tx:*")
    entries = []

    for k in tx_keys:
        try:
            raw = hgetall(k)
            entry = normalize_entry(raw)
            entries.append(entry)
            # Print each entry nicely
            print(json.dumps(entry, ensure_ascii=False))
        except Exception as e:
            print(f"Error reading {k}: {e}")

    # Save to files
    # JSON Lines
    with open("entries.jsonl", "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    # Pretty JSON array
    with open("entries-pretty.json", "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2, ensure_ascii=False)

    print(f"\nDone. Wrote {len(entries)} entries to entries.jsonl and entries-pretty.json")

if __name__ == "__main__":
    main()
