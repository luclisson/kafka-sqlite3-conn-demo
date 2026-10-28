# CDC-Pipeline: Kafka Connect + Debezium SQLite

## Wie der Connector funktioniert

Der offizielle [Debezium SQLite Connector](https://github.com/debezium/debezium-connector-sqlite) (Stand: **3.8.0-SNAPSHOT, incubating**) nutzt **Trigger-basiertes CDC**, da SQLite keinen replizierbaren Log-Stream nach außen anbietet:

1. Beim Start: DB-Datei in WAL-Modus versetzen, `_debezium_cdc_log` anlegen, falls fehlend
2. Auf jeder gecapturten Tabelle: `AFTER INSERT`/`AFTER UPDATE`/`AFTER DELETE`-Trigger installieren
3. Trigger schreiben in derselben Transaktion wie die Änderung eine Zeile ins CDC-Log (`change_id` = Commit-Order)
4. Connector polllt das Log in Batches, emittiert Change-Events, rückt seinen Offset (`change_id`) vor und löscht verarbeitete Log-Zeilen periodisch

Konsequenzen:

- **Inserts, Updates und Deletes** werden vollständig erfasst, mit vollem `before`- und `after`-Image
- Der Connector braucht **Schreibrechte** auf `users.db` (Trigger, Log-Tabelle, Kompaktierung)
- Connector, Writer und DB-Datei müssen **auf demselben Host** mit lokalem Dateisystem liegen (WAL-Datei-Locks)
- Tabellen brauchen einen **Primary Key** (`users.id` ✓), sonst NULL-Keys
- Genau **1 Task** pro Connector pro DB-Datei; kein Incremental-/Blocking-Snapshot

## Plugin bauen & installieren

Einmalig, siehe [setup.md](setup.md). Ergebnis: `kafka/connect-plugins/debezium-connector-sqlite/` mit Connector-JAR + Dependencies (u. a. `sqlite-jdbc-3.47.1.0.jar`).

## Worker-Konfiguration: `connect-standalone-sqlite.properties`

```properties
bootstrap.servers=localhost:9092

key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true

offset.storage.file.filename=/home/lisslu/Documents/repos/kafka-demo/connect.offsets
offset.flush.interval.ms=5000

plugin.path=/home/lisslu/Documents/repos/kafka-demo/kafka/connect-plugins

listeners=http://localhost:8083
```

| Einstellung | Bedeutung |
|---|---|
| `bootstrap.servers` | Broker-Endpunkt |
| `JsonConverter` + `schemas.enable=true` | Events als JSON **mit eingebettetem Schema** — kein Schema Registry nötig |
| `offset.storage.file.filename` | Standalone-Worker persistiert Offsets in dieser Datei (nicht in Topics — das ist Distributed-Modus) |
| `offset.flush.interval.ms` | Offsets alle 5 s committen |
| `plugin.path` | Wo der Worker nach Connector-Plugins sucht |
| `listeners` | REST-API auf Port 8083 |

## Connector-Registrierung: `sqlite-users-connector.json`

```json
{
  "name": "sqlite-users",
  "config": {
    "connector.class": "io.debezium.connector.sqlite.SQLiteSourceConnector",
    "topic.prefix": "sqlite",
    "database.file.path": "/home/lisslu/Documents/repos/kafka-demo/users.db",
    "table.include.list": "users",
    "snapshot.mode": "initial",
    "tasks.max": "1"
  }
}
```

| Property | Bedeutung |
|---|---|
| `topic.prefix` | Namespace für Topics: `<prefix>.<tabelle>` → `sqlite.users`; Schema-Change-Topic heißt nur `sqlite` |
| `database.file.path` | Absoluter Pfad zur DB-Datei |
| `table.include.list` | Nur diese Tabellen capturen (Regex, kommagetrennt); weglassen = alle Nicht-System-Tabellen |
| `snapshot.mode` | `initial` (Default): erste Starts lesen Bestand als `op=r`-Events; Alternativen: `always`, `no_data`, `initial_only` |
| `tombstones.on.delete` | Default `true`: nach `d`-Event folgt ein Tombstone (Value = null) |

Weitere Optionen (Doku der nightly-Version): `cdc.log.batch.size` (1000), `log.compaction.threshold` (10000), `column.include/exclude.list`, `event.converting.failure.handling.mode`.

## Starten & Registrieren

```bash
cd ~/Documents/repos/kafka-demo

# Worker mit Connector-Config direkt starten (empfohlen):
kafka/bin/connect-standalone.sh \
    connect-standalone-sqlite.properties sqlite-users-connector.json

# Alternativ: Worker ohne Connector starten und per REST registrieren:
kafka/bin/connect-standalone.sh connect-standalone-sqlite.properties
curl -X POST localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @sqlite-users-connector.json

# Status prüfen:
curl -s localhost:8083/connectors/sqlite-users/status | python3 -m json.tool
# connector.state + tasks[0].state sollten "RUNNING" sein

# Registrierte Plugin-Klassen prüfen:
curl -s localhost:8083/connector-plugins | python3 -m json.tool
```

Deregistrieren: `curl -X DELETE localhost:8083/connectors/sqlite-users`

## Events: Topic `sqlite.users`

Nach dem Start streamt der Connector erst den Snapshot (`op=r` für jede existierende Row), dann live alle Änderungen. Empfangenen Operations-Typen:

| `op` | Bedeutung | `before` | `after` |
|---|---|---|---|
| `r` | Snapshot-Read | null | Zeile |
| `c` | Insert | null | Zeile |
| `u` | Update | alte Zeile | neue Zeile |
| `d` | Delete | alte Zeile | null (+ Tombstone) |

Beispiel-Insert (aus dem echten Test, Schema-Teil gekürzt):

```json
{"schema": { "...": "Key-Schema: id INT64 / Value-Schema: Envelope" },
 "payload": {
   "before": null,
   "after": {"id": 6, "name": "Jonas Gruber",
             "email": "jonas.gruber6370@example.com",
             "created_at": "2026-10-28 19:16:48"},
   "source": {"version": "3.8.0-SNAPSHOT", "connector": "sqlite",
              "name": "sqlite", "table": "users", "change_id": 6, "...": "..."},
   "op": "c",
   "ts_ms": 1793215009064}}
```

Der Message-**Key** ist der Primary Key: `{"schema": {...}, "payload": {"id": 6}}`.

## Verifikation

```bash
# Alle Events inkl. Typ zählen:
kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic sqlite.users --from-beginning --timeout-ms 10000 \
  | grep -o '"op":"[a-z]"' | sort | uniq -c
# Erwartung z. B.:  7 "op":"c" / 1 "op":"u" / 1 "op":"d"

# CDC-Mechanismus in der DB direkt prüfen:
sqlite3 users.db "SELECT change_id, table_name, operation FROM _debezium_cdc_log LIMIT 5;"
sqlite3 users.db "SELECT name FROM sqlite_master WHERE type='trigger';"
```

Test-Ablauf in diesem Setup: Writer startete bei leerer Tabelle (kein Snapshot-Inhalt), 7 Inserts als `c`, ein manuelles `UPDATE` als `u`, ein manuelles `DELETE` als `d` plus Tombstone — alle wenige Sekunden nach der Änderung im Topic.

## Offsets & Neustart

- Offsets liegen in `connect.offsets` (Datei). Beim Neustart mit derselben Connector-Config resumed der Connector am letzten `change_id` — keine Duplikate, keine Lücken.
- `snapshot.mode=initial` snapshotet nur, wenn noch keine Offsets existieren.
- Offsets verlieren (Datei löschen) ⇒ vollständiger neuer Snapshot mit `op=r` für den Bestand.

## Bekannte Grenzen (Stand 3.8.0-SNAPSHOT)

- **Incubating**: Nachrichtenformat kann sich ändern; nicht für Produktion empfohlen
- Kein Incremental-/Ad-hoc-/Blocking-Snapshot
- `ddl`-Feld in Schema-Change-Events ist immer `null` (SQLite liefert keinen DDL-Text)
- Kein Schema-History-Topic; Schema wird direkt aus der DB-Datei gelesen
- Typ-Mapping über SQLite-Type-Affinity: `INTEGER→INT64`, `TEXT→STRING`, `REAL/NUMERIC→FLOAT64`, `BLOB→BYTES`
