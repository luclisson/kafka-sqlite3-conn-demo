#!/usr/bin/env python3
import random
import sqlite3
import time

DB_PATH = "users.db"
CYCLE_SECONDS = 120
MIN_UPDATES_PER_CYCLE = 1
MAX_UPDATES_PER_CYCLE = 3

FIRST_NAMES = ["anna", "ben", "clara", "david", "emil", "frauke", "gustav", "hana", "ida", "jonas"]
LAST_NAMES = ["fischer", "gruber", "huber", "keller", "lang", "maier", "novak", "richter", "schmidt", "wagner"]


def random_user():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first.capitalize()} {last.capitalize()}"
    email = f"{first}.{last}{random.randint(1, 9999)}@example.com"
    return name, email


def insert_row(conn) -> None:
    name, email = random_user()
    conn.execute(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        (name, email),
    )
    conn.commit()
    row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    print(f"inserted id={row_id} name={name} email={email}", flush=True)


def update_rows(conn) -> None:
    count = random.randint(MIN_UPDATES_PER_CYCLE, MAX_UPDATES_PER_CYCLE)
    ids = [row[0] for row in conn.execute(
        "SELECT id FROM users ORDER BY RANDOM() LIMIT ?", (count,)
    ).fetchall()]
    if not ids:
        print("no rows to update yet", flush=True)
        return
    for row_id in ids:
        name, email = random_user()
        conn.execute(
            "UPDATE users SET name = ?, email = ? WHERE id = ?",
            (name, email, row_id),
        )
        conn.commit()
        print(f"updated id={row_id} name={name} email={email}", flush=True)


def main():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=10000;")
    print(
        f"Every {CYCLE_SECONDS}s: 1 insert + {MIN_UPDATES_PER_CYCLE}-{MAX_UPDATES_PER_CYCLE} "
        f"updates on {DB_PATH} (Ctrl+C to stop)",
        flush=True,
    )
    try:
        while True:
            insert_row(conn)
            update_rows(conn)
            time.sleep(CYCLE_SECONDS)
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
