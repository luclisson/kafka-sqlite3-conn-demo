#!/usr/bin/env python3
import random
import sqlite3
import time

DB_PATH = "users.db"
INTERVAL_SECONDS = 5

FIRST_NAMES = ["anna", "ben", "clara", "david", "emil", "frauke", "gustav", "hana", "ida", "jonas"]
LAST_NAMES = ["fischer", "gruber", "huber", "keller", "lang", "maier", "novak", "richter", "schmidt", "wagner"]


def random_user():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    name = f"{first.capitalize()} {last.capitalize()}"
    email = f"{first}.{last}{random.randint(1, 9999)}@example.com"
    return name, email


def main():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=10000;")
    print(f"Writing one user row every {INTERVAL_SECONDS}s to {DB_PATH} (Ctrl+C to stop)")
    try:
        while True:
            name, email = random_user()
            conn.execute(
                "INSERT INTO users (name, email) VALUES (?, ?)",
                (name, email),
            )
            conn.commit()
            row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            print(f"inserted id={row_id} name={name} email={email}", flush=True)
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
