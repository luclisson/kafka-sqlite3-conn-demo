# SQLite-Datenquelle (`users.db` + `insert_users.py`)

## Datenbank `users.db`

Einfache SQLite-Datei im Projekt-Root, **WAL-Modus** (`PRAGMA journal_mode=WAL`):

```sql
CREATE TABLE users (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  name       TEXT NOT NULL,
  email      TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

Prüfen:

```bash
sqlite3 users.db ".schema users" "PRAGMA journal_mode;"   # → wal
sqlite3 users.db "SELECT * FROM users;"
```

Durch WAL entstehen zwei Zusatzdateien (`users.db-wal`, `users.db-shm`), solange Verbindungen offen sind — normal, nicht löschen.

### Zusatzobjekte des CDC-Connectors

Sobald der Debezium-Connector einmal gelaufen ist, enthält die DB zusätzlich:

- Tabelle **`_debezium_cdc_log`** — CDC-Log des Connectors (siehe [kafka-connect-cdc.md](kafka-connect-cdc.md))
- Trigger **`_debezium_cdc_users_insert`**, **`_debezium_cdc_users_update`**, **`_debezium_cdc_users_delete`** — schreiben bei jeder Änderung an `users` eine Zeile in das CDC-Log

Das ist gewollt: Es ist der Capture-Mechanismus des Connectors. Wer die DB „clean“ haben will, muss Trigger + Log-Tabelle nach Connectoren-Stopp manuell droppen.

## Writer-Script `insert_users.py`

Verhalten (2-Minuten-Zyklus, erster Zyklus direkt beim Start):

- Öffnet `users.db` (relativer Pfad → **aus dem Projekt-Root starten**), setzt `journal_mode=WAL` und `busy_timeout=10000`
- Pro Zyklus:
  1. **1 INSERT** neuer Row mit Zufallsname/E-Mail (`Vorname Nachname`, `vorname.nachname<n>@example.com`) → `c`-Event
  2. **1–3 zufällige existierende Rows** (`ORDER BY RANDOM()`) bekommen neue Zufallswerte → `u`-Events
- Loggt jede Aktion auf stdout; Beenden mit `Ctrl+C`
- Konstanten oben anpassbar: `CYCLE_SECONDS`, `MIN_UPDATES_PER_CYCLE`, `MAX_UPDATES_PER_CYCLE`

```bash
cd ~/Documents/repos/kafka-demo
python3 insert_users.py
```

Beispiel-Ausgabe:

```
Every 120s: 1 insert + 1-3 updates on users.db (Ctrl+C to stop)
inserted id=67 name=Ida Schmidt email=ida.schmidt4362@example.com
updated id=53 name=Anna Huber email=anna.huber7303@example.com
```

### Warum nur echte Änderungen im Topic landen

Der Debezium-Connector published ein Event **pro DML-Statement** — ein `UPDATE` mit identischen Werten würde
also trotzdem ein `u`-Event erzeugen. Das Script schreibt Updates deshalb immer mit **neuen Zufallswerten**:
Rows, die im Zyklus nicht gewählt werden, erzeugen gar kein Event; geänderte Rows ein `u`-Event; neue Rows
ein `c`-Event. Das Topic `sqlite.users` enthält damit exakt die neuen und geänderten Rows.

### Warum WAL?

Der Debezium-SQLite-Connector liest die DB-Datei neben dem Writer in kurzen Transaktionen. WAL erlaubt gleichzeitige Reader/Writer ohne Table-Locks — der Connector blockiert den Writer nicht und umgekehrt. Für CDC-Tests der Debezium-Community ist WAL die empfohlene Einstellung.

### Manuelle Update-/Delete-Tests

Der Writer erzeugt inzwischen selbst Insert- **und** Update-Events; Deletes nur manuell:

```bash
sqlite3 users.db "DELETE FROM users WHERE id=2;"
```

Jede Änderung erscheint (innerhalb weniger Sekunden) als Event im Topic `sqlite.users`.
