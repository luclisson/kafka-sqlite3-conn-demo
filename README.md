# Kafka + SQLite CDC Demo

> ## ⚠️ Agentic-AI-Disclaimer
>
> Dieses Projekt dient ausschließlich **Demo-Zwecken** und ist nicht für den Produktiveinsatz bestimmt.
> Es wurde **erstellt mit Hilfe von agentic AI** — die **Architektur und die Anweisungen** stammen vom
> Repo-Inhaber, die Implementierung und Dokumentation wurden von der KI generiert.

Demo-Setup für Change Data Capture (CDC) von einer SQLite-Datenbank nach Apache Kafka — plus einer kleinen
Spring-Boot-Anwendung mit Kafka-Producer/Consumer:

- **SQLite** (`users.db`, WAL-Modus) wird über einen **Debezium-SQLite-Source-Connector** in Kafka Connect
  überwacht → Change-Events (Insert/Update/Delete) im Topic `sqlite.users`
- **Spring Boot 4** (Java 27) sendet JSON-Nachrichten auf das Topic `spring-topic` (`POST /messages`)

## Hinweis zu externen Komponenten

Beide sind **nicht Teil dieses Repos**, sondern werden lokal bereitgestellt (siehe `.gitignore`):

- **Apache Kafka 4.4.0** — entpackte Distribution im lokalen Verzeichnis `kafka/`
  (Download: <https://kafka.apache.org/downloads>)
- **Debezium SQLite Connector** — externes Open-Source-Projekt
  <https://github.com/debezium/debezium-connector-sqlite>, lokal geklont und gebaut

## Enthaltene Dateien

| Pfad | Zweck |
|---|---|
| `docs/` | Vollständige Dokumentation (Start bei [docs/README.md](docs/README.md)) |
| `kafka.demo/` | Spring-Boot-Projekt (Producer/Consumer, JSON-Serialisierung) |
| `insert_users.py` | Writer-Script: alle 5 s ein INSERT in die SQLite-DB |
| `connect-standalone-sqlite.properties` | Konfiguration des Kafka-Connect-Workers |
| `sqlite-users-connector.json` | Connector-Registrierung (CDC-Quelle `users`) |

Zur Laufzeit entstehen lokal zusätzlich: `users.db` (+ WAL-Dateien), `connect.offsets`, Log-Dateien.

## Quick Start

Die komplette Anleitung steht in **[docs/README.md](docs/README.md)** (Umgebung, Start-Reihenfolge,
CDC-Pipeline, Spring-App, Troubleshooting).
