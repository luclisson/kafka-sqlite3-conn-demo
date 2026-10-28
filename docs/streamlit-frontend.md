# Streamlit-Frontend (`dashboard.py`)

Einfaches Live-Dashboard mit zwei Boxen, die die Nachrichtenströme der beiden Topics anzeigen:

| Box | Topic | Darstellung |
|---|---|---|
| Links | `sqlite.users` | Farbiges Op-Badge (`c` INSERT grün, `u` UPDATE orange, `d` DELETE rot, `r` SNAPSHOT blau), Row-Daten (`id`, `name`, `email`), `change_id` + Offset, Raw-JSON aufklappbar |
| Rechts | `spring-topic` | `text` + `createdAt`, Offset, Raw-JSON aufklappbar |

## Setup (einmalig)

Python-Venv mit den beiden Abhängigkeiten (Streamlit, confluent-kafka):

```bash
cd ~/Documents/repos/kafka-demo
python3 -m venv .venv
.venv/bin/pip install streamlit confluent-kafka
```

`.venv/` ist via `.gitignore` aus dem Repo gehalten.

## Start

Broker muss laufen (Worker und Spring-App optional, je nachdem welche Topics du sehen willst):

```bash
cd ~/Documents/repos/kafka-demo
.venv/bin/streamlit run dashboard.py
```

→ öffnet automatisch den Browser auf <http://localhost:8501>.

## Verhalten

- **Nur Live-Stream**: Jeder Consumer startet mit `auto.offset.reset=latest` und einer einmaligen Group-ID
  (`streamlit-ui-<zufall>`). Beim Öffnen der Seite erscheinen nur neue Nachrichten; Alte siehst du bei
  Bedarf per `kafka-console-consumer.sh --from-beginning` (siehe [kafka-connect-cdc.md](kafka-connect-cdc.md)).
- **Fragment-Refresh**: Die Boxen werden per Streamlit-Fragment jede Sekunde aktualisiert; der Sidebar-Button
  „Alle Boxen leeren" bleibt dabei bedienbar und leert die Anzeige (die Topics werden weiter konsumiert).
- **Puffer**: max. 20 Nachrichten pro Box (neueste oben), ältere fallen heraus.
- **Tombstones** (leere Values nach Deletes) werden gefiltert.
- Debezium-Events (Envelope mit `schema`/`payload`) werden automatisch ausgepackt; `spring-topic`-Messages
  (plain JSON) werden direkt dargestellt.
- Bei Broker-Problemen erscheint eine Warnung in der jeweiligen Box statt eines Absturzes.

## Testen

```bash
# CDC-Box: Writer starten → alle 2 min ein c-Event (grün) + 1-3 u-Events (orange)
python3 insert_users.py

# Spring-Box: App läuft? Dann:
curl -X POST localhost:8080/messages -H "Content-Type: text/plain" -d "hello dashboard"

# Spring-Box ohne App — direkt per console-producer:
kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic spring-topic
# >>> {"text":"hallo","createdAt":"2026-10-28T21:00:00Z"}
```
