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

Verhalten:

- Öffnet `users.db` (relativer Pfad → **aus dem Projekt-Root starten**), setzt `journal_mode=WAL` und `busy_timeout=10000`
- Endlosschleife: alle **5 Sekunden** ein INSERT mit Zufallsname/E-Mail (`Vorname Nachname`, `vorname.nachname<n>@example.com`)
- Loggt jede Row auf stdout; Beenden mit `Ctrl+C`

```bash
cd ~/Documents/repos/kafka-demo
python3 insert_users.py
```

Beispiel-Ausgabe:

```
Writing one user row every 5s to users.db (Ctrl+C to stop)
inserted id=1 name=Hana Lang email=hana.lang7221@example.com
```

### Warum WAL?

Der Debezium-SQLite-Connector liest die DB-Datei neben dem Writer in kurzen Transaktionen. WAL erlaubt gleichzeitige Reader/Writer ohne Table-Locks — der Connector blockiert den Writer nicht und umgekehrt. Für CDC-Tests der Debezium-Community ist WAL die empfohlene Einstellung.

### Manuelle Update-/Delete-Tests

Um CDC-Events vom Typ `u` und `d` zu erzeugen:

```bash
sqlite3 users.db "UPDATE users SET name='CDC Test' WHERE id=1;"
sqlite3 users.db "DELETE FROM users WHERE id=2;"
```

Jede Änderung erscheint (innerhalb weniger Sekunden) als Event im Topic `sqlite.users`.
