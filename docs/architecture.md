# Architektur

## Datenflüsse

```mermaid
flowchart TD
    subgraph CDC[CDC-Pfad]
        W[insert_users.py] -->|INSERT alle 5 s| UDB[(users.db)]
        UDB -->|Trigger schreiben| LOG[(_debezium_cdc_log)]
        LOG -->|Poll| DBZ[Debezium SQLite Connector]
        DBZ -->|Change Events op=r/c/u/d| SU[(Topic sqlite.users)]
    end

    subgraph App[Pfad der Spring-Boot-App]
        POST[curl POST /messages] --> CTRL[MessageController]
        CTRL --> MSG[DemoMessage record]
        MSG --> PROD[MessageProducer]
        PROD -->|JacksonJsonSerializer| ST[(Topic spring-topic)]
        ST -->|JacksonJsonDeserializer| CONS[MessageConsumer<br/>@KafkaListener]
        CONS --> LOGOUT[(Log-Ausgabe)]
    end

    K[Broker :9092 KRaft :9093] --- SU
    K --- ST
    CW[Connect-Worker :8083] --- DBZ
```

Beide Pfade sind unabhängig; sie teilen sich nur den Broker. Ein Konsument kann beide Topics gleichzeitig lesen (z. B. console-consumer oder ein eigener `@KafkaListener`).

## Ports

| Port | Prozess | Zweck |
|---|---|---|
| 9092 | Kafka Broker | PLAINTEXT-Listener für Clients (App, Connect, CLI-Tools) |
| 9093 | Kafka Controller | KRaft-Quorum (Broker ist Combined-Node: Broker + Controller) |
| 8083 | Connect-Worker | REST-API: Connector registrieren/status abfragen |
| 8080 | Spring Boot | Tomcat, `POST /messages` |

## Verzeichnisstruktur des Projekt-Roots

```
~/Documents/repos/kafka-demo/                # Git-Repo (siehe .gitignore)
├── README.md                                # Projekt-Übersicht + Agentic-AI-Disclaimer
├── kafka/                                   # ⚠️ NICHT im Repo — lokal entpackte Apache-Kafka-Distribution 4.4.0
│   ├── bin/                                 # 45 CLI-Scripts (Broker, Connect, Topic-Tools, ...)
│   ├── config/                              # server.properties, connect-*.properties, ...
│   ├── connect-plugins/                     # HIER: Debezium-SQLite-Connector-Plugin
│   │   └── debezium-connector-sqlite/       #   (Connector-JAR + Dependencies)
│   ├── libs/                                # Kafka-JARs (kafka_2.13-4.4.0.jar, ...)
│   ├── licenses/, LICENSE, NOTICE           # Lizenzmaterial
│   ├── logs/                                # Broker-Logfiles (server.log, ...)
│   └── site-docs/                           # Mitgelieferte Kafka-Dokumentation (HTML)
├── kafka.demo/                              # Spring-Boot-Projekt (Maven)
│   ├── pom.xml                              # Spring Boot 4.1.2, Java 27, starter-kafka/webmvc
│   ├── mvnw, mvnw.cmd                       # Maven-Wrapper (3.10.0)
│   └── src/                                 # de.coba.kafka, s. spring-boot-app.md
├── users.db                                 # ⚠️ Laufzeitdatei — SQLite-Datenbank (WAL), CDC-Quelle
├── users.db-wal / users.db-shm              # WAL- und Shared-Memory-Dateien (laufend)
├── insert_users.py                          # Writer: alle 5 s ein INSERT
├── connect-standalone-sqlite.properties     # Worker-Konfiguration (Standalone-Modus)
├── connect.offsets                          # ⚠️ Laufzeitdatei — Offsets des Connect-Workers
├── sqlite-users-connector.json              # Connector-Registrierung (REST-POST)
├── debezium-connector-sqlite/               # ⚠️ NICHT im Repo — geklonter Connector-Source (github.com/debezium/debezium-connector-sqlite)
└── docs/                                    # Diese Dokumentation
```

## Topics

| Topic | Erzeuger | Inhalt |
|---|---|---|
| `sqlite.users` | Debezium Connector `sqlite-users` | Change-Events der Tabelle `users` (`op` = `r`/`c`/`u`/`d`, plus Tombstones) |
| `spring-topic` | `KafkaTopicConfig` (KafkaAdmin legt es beim App-Start an) | JSON-Objekte `{"text": "...", "createdAt": "..."}` |

Zusätzlich: Schema-Change-Events des Connectors landen im Topic `sqlite` (benannt nach `topic.prefix`).

## Restart-Verhalten

- **Broker**: State in `/tmp/kraft-combined-logs` — Topics und Daten überleben einen Neustart (solange `/tmp` nicht geleert wird).
- **Connect-Worker (Standalone)**: Connector-Config wird beim Start als Kommandozeilen-Argument mitgegeben, Offsets liegen in `connect.offsets` → Resume exakt an letzter Position.
- **Spring-Boot-App**: Stateless; `spring-topic` wird per KafkaAdmin nur angelegt, falls fehlend.
- **SQLite-Writer**: Stateless; jedes Insert bekommt eine neue Autoincrement-ID.

Einschränkung: Der CDC-Pfad funktioniert nur, wenn **Broker-Prozess, Connect-Worker, Writer und DB-Datei auf demselben Host** liegen (SQLite-WAL nutzt lokale Datei-Locks und Shared Memory, siehe [kafka-connect-cdc.md](kafka-connect-cdc.md)).
