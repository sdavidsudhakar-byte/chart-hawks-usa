"""
wipe_neon.py — Truncate all tables in Neon before a clean migration.

Uses TRUNCATE ... CASCADE so foreign-key dependencies are handled automatically.
Resets all SERIAL sequences back to 1.

USAGE:
  python wipe_neon.py --dry-run    # show what would be wiped, no changes
  python wipe_neon.py              # wipe all tables
"""

import argparse
import os
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true", help="Show counts only, no truncation")
args = parser.parse_args()

sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

if not os.getenv("DATABASE_URL"):
    print("ERROR: DATABASE_URL not set in .env")
    sys.exit(1)

import psycopg2

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
conn.autocommit = False
cur = conn.cursor()

# Get all user tables in public schema
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    ORDER BY table_name
""")
tables = [r[0] for r in cur.fetchall()]

if not tables:
    log("No tables found in Neon — nothing to wipe.")
    conn.close()
    sys.exit(0)

log(f"Found {len(tables)} tables in Neon:")
total_rows = 0
for t in tables:
    cur.execute(f'SELECT COUNT(*) FROM "{t}"')
    cnt = cur.fetchone()[0]
    total_rows += cnt
    log(f"  {t}: {cnt:,} rows")

print()
log(f"Total rows to wipe: {total_rows:,}")

if args.dry_run:
    log("DRY RUN — no changes made. Run without --dry-run to wipe.")
    conn.close()
    sys.exit(0)

# Safety confirmation
print()
print("WARNING: This will permanently DELETE ALL DATA from Neon.")
print("Type YES to confirm: ", end="", flush=True)
answer = input().strip()
if answer != "YES":
    print("Aborted.")
    conn.close()
    sys.exit(0)

print()
log("Truncating all tables...")

# TRUNCATE all at once with CASCADE — handles any FK deps, resets sequences
tables_sql = ", ".join(f'"{t}"' for t in tables)
cur.execute(f"TRUNCATE {tables_sql} RESTART IDENTITY CASCADE")
conn.commit()

log("All tables truncated and sequences reset.")

# Verify
log("Verifying row counts after wipe:")
all_zero = True
for t in tables:
    cur.execute(f'SELECT COUNT(*) FROM "{t}"')
    cnt = cur.fetchone()[0]
    status = "OK" if cnt == 0 else "ERROR - not empty!"
    log(f"  {t}: {cnt} rows  [{status}]")
    if cnt != 0:
        all_zero = False

print()
if all_zero:
    log("Neon is clean. Ready to run: python migrate_to_neon.py")
else:
    log("WARNING: Some tables are not empty. Check above.")

conn.close()
