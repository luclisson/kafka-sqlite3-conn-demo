# Kafka-Installation (`kafka/`)

> **Hinweis:** Dieses Verzeichnis ist **nicht Teil des Repos** (siehe `.gitignore`). Es handelt sich um die lokal
> entpackte Apache-Kafka-Binary-Distribution — Download: <https://kafka.apache.org/downloads> (Details: [setup.md](setup.md)).
> Die folgende Beschreibung dokumentiert die vorliegende Installation und ihren Betrieb.

Apache Kafka **4.4.0** (Scala 2.13-Build, `kafka_2.13-4.4.0.jar`), Binary-Distribution, entpackt ohne Installer. Läuft im **KRaft-Modus** (ohne ZooKeeper).

## Verzeichnisinhalte

| Pfad | Zweck |
|---|---|
| `bin/` | 45 ausführbare Scripts: Server, Connect, CLI-Tools |
| `bin/windows/` | Windows-Varianten (hier ungenutzt) |
| `config/` | Alle Properties-Dateien (Broker, Connect, Tools) |
| `connect-plugins/` | **Projektspezifisch**: Debezium-SQLite-Connector (siehe unten) |
| `libs/` | Alle JARs (`kafka_2.13-4.4.0.jar`, kafka-clients, Connect-API, …) |
| `licenses/`, `LICENSE`, `NOTICE` | Lizenzmaterial |
| `logs/` | Broker-Logfiles (`server.log`, `log-cleaner.log`, `state-change.log`) |
| `site-docs/` | Mitgelieferte HTML-Dokumentation (`kafka`-Ordner öffnen im Browser) |

## Broker: `config/server.properties` (KRaft Combined-Node)

Die relevante Konfiguration (Werte aus diesem Setup):

```properties
process.roles=broker,controller          # Combined-Mode: ein Prozess macht beides
node.id=1
controller.quorum.bootstrap.servers=localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
log.dirs=/tmp/kraft-combined-logs
```

⚠️ `log.dirs` liegt unter `/tmp` — bei Systemreinigung gehen die Topics verloren (danach einfach Broker neu starten, Topics werden von der App bzw. dem Connector neu angelegt).

### Broker betreiben

```bash
# Format des Storage-Verzeichnisses (nur beim allerersten Start nötig,
# Cluster-UUID wird dabei erzeugt):
kafka/bin/kafka-storage.sh format \
  --standalone \
  --config kafka/config/server.properties

# Start / Stop
kafka/bin/kafka-server-start.sh kafka/config/server.properties
kafka/bin/kafka-server-stop.sh
```

Log-Ausgabe geht aufs Terminal; zusätzliche File-Logs in `kafka/logs/server.log`.

## `config/` im Überblick

| Datei | Zweck | Status hier |
|---|---|---|
| `server.properties` | Broker im KRaft-Broker-Modus | **aktiv genutzt** (s. o.) |
| `broker.properties` | Reiner Broker (separate Rolle) | ungenutzt |
| `controller.properties` | Reiner Controller (separate Rolle) | ungenutzt |
| `connect-standalone.properties` | Connect-Worker-Vorlage (Standalone) | Vorlage; wir nutzen `../connect-standalone-sqlite.properties` |
| `connect-distributed.properties` | Connect-Worker-Vorlage (Distributed, Topic-basierte Offsets) | ungenutzt |
| `connect-console-source.properties` | stdin → Kafka | ungenutzt |
| `connect-console-sink.properties` | Kafka → stdout | ungenutzt |
| `connect-file-source.properties` | Datei → Topic (Demo-Connector) | ungenutzt |
| `connect-file-sink.properties` | Topic → Datei (Demo-Connector) | ungenutzt |
| `connect-mirror-maker.properties` | MM2-Replikation zwischen Clustern | ungenutzt |
| `connect-log4j2.yaml` | Log-Config der Connect-Worker | default |
| `log4j2.yaml` | Log-Config des Brokers | default |
| `tools-log4j2.yaml` | Log-Config der CLI-Tools | default |
| `producer.properties` / `consumer.properties` | Defaults für console-producer/-consumer | default |
| `trogdor.conf` | Test-Framework Trogdor | ungenutzt |

## `bin/` — wichtigste Scripts

### Server & Connect
| Script | Zweck |
|---|---|
| `kafka-server-start.sh` / `kafka-server-stop.sh` | Broker starten/stoppen |
| `kafka-storage.sh` | Storage formatieren, Cluster-UUID, Metadatum-Quorum-Tools |
| `connect-standalone.sh` | Connect-Worker im Standalone-Modus (+ Props + Connector-JSON als Args) |
| `connect-distributed.sh` | Connect-Worker im Distributed-Modus |
| `connect-plugin-path.sh` | Plugins in einem plugin.path prüfen/listen |
| `connect-internal-topics.sh` | Interne Connect-Topics anlegen/prüfen (v. a. für Distributed) |
| `connect-mirror-maker.sh` | MirrorMaker 2 |

### Topic- & Cluster-Verwaltung
| Script | Zweck (Beispiel) |
|---|---|
| `kafka-topics.sh` | `--list`, `--describe`, `--create`, `--alter`, `--delete` |
| `kafka-console-consumer.sh` | Topic lesen (`--from-beginning`, `--timeout-ms`) |
| `kafka-console-producer.sh` | Interaktiv Nachrichten senden |
| `kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell` | Offsets pro Partition abfragen |
| `kafka-consumer-groups.sh` | Consumer-Groups: `--list`, `--describe`, `--reset-offsets` |
| `kafka-broker-api-versions.sh` | Broker-Erreichbarkeit testen |
| `kafka-configs.sh` | Dynamic Broker-/Topic-Configs ändern |
| `kafka-dump-log.sh` | Segment-Files inspizieren (Debug) |

Voller Überblick: `ls kafka/bin/` oder die Doku unter `kafka/site-docs/`.

## Debezium-SQLite-Plugin (`connect-plugins/`)

```
connect-plugins/debezium-connector-sqlite/
├── debezium-connector-sqlite-3.8.0-SNAPSHOT.jar   # der eigentliche Connector
├── debezium-connector-common / -api / -config / -util / -openlineage-api (3.8.0-*)
├── sqlite-jdbc-3.47.1.0.jar                       # xerial SQLite-JDBC-Treiber
└── sketches-java-0.8.2.jar
```

Gebaut aus `github.com/debezium/debezium-connector-sqlite` (Details: [kafka-connect-cdc.md](kafka-connect-cdc.md)). Das Verzeichnis ist über `plugin.path` in `connect-standalone-sqlite.properties` registriert.
