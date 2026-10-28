# Troubleshooting

Fehler und Lösungen, die in diesem Setup tatsächlich aufgetreten sind.

---

## 1. Maven-Build: `error: release version 27 not supported`

**Symptom** bei `mvn clean install` / `mvn spring-boot:run`:

```
[ERROR] Fatal error compiling: error: release version 27 not supported
```

**Ursache:** Das Fedora-Maven-Paket (3.9.11) ist an **Java 25** (`/usr/lib/jvm/java-25-openjdk`) gebunden, das Projekt verlangt `<java.version>27</java.version>`. `JAVA_HOME` war nicht gesetzt. Der Maven-Wrapper `./mvnw` war davon nicht betroffen (wählt Java über `PATH` → 27-ea).

**Lösung:** `~/.mavenrc` anlegen:

```bash
JAVA_HOME=/usr/lib/jvm/java-latest-openjdk
```

Danach `mvn -version` prüfen — muss `Java version: 27-ea` zeigen. Hinweis: `/usr/lib/jvm/java-27-openjdk` ist ein leeres Stub-Verzeichnis; die echte 27 liegt unter `java-latest-openjdk`.

---

## 2. Spring-App-Start: `NoClassDefFoundError: com/fasterxml/jackson/databind/JavaType`

**Symptom:**

```
Failed to construct kafka consumer
Caused by: java.lang.NoClassDefFoundError: com/fasterxml/jackson/databind/JavaType
Caused by: java.lang.ClassNotFoundException: com.fasterxml.jackson.databind.JavaType
```

**Ursache:** Spring Boot 4 bringt **Jackson 3** (`tools.jackson.*`) mit. Die spring-kafka-Klassen `JsonSerializer`/`JsonDeserializer` brauchen aber **Jackson 2** (`com.fasterxml.jackson.*`) — fehlt im Classpath.

**Lösung:** In `application.properties` die Jackson-3-Varianten verwenden:

```properties
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JacksonJsonDeserializer
```

---

## 3. Broker plötzlich nicht erreichbar

**Symptome:**

- CLI-Tools: `Connection to node -1 (localhost/127.0.0.1:9092) could not be established.`
- Spring-Producer: hängt beim Senden (Warten auf Metadaten), REST-Call blockiert/timed out
- Connect-Worker: `WorkerConnector did not start` / Reconnect-Loops

**Diagnose:**

```bash
ss -ltn | grep 9092        # Port offen?
tail kafka/logs/server.log # Shutdown-Grund
```

**Lösung:** Broker starten (Siehe [kafka-installation.md](kafka-installation.md)). Broker/Writer/App/Worker danach einfach weiterlaufen lassen — Kafka-Clients reconnecten automatisch, Connect resumed am letzten Offset.

---

## 4. Spring-App kompiliert, aber `contextLoads`-Test schlägt fehl

Der Standardtest `ApplicationTests.contextLoads` ist ein `@SpringBootTest`: Der Consumer-Endpoint versucht, beim Start eine Kafka-Consumer-Verbindung aufzubauen. Ohne laufenden Broker kann der Kontext fehlschlagen.

**Lösung:** Broker starten, oder Tests überspringen: `mvn clean install -DskipTests`.

---

## 5. CDC-Connector: keine Events im Topic

Checkliste:

1. **Worker-Status:** `curl -s localhost:8083/connectors/sqlite-users/status` — muss `RUNNING` zeigen; bei `FAILED` steht dort der Stacktrace
2. **Plugin geladen?** `curl -s localhost:8083/connector-plugins` muss `io.debezium.connector.sqlite.SQLiteSourceConnector` listen; sonst stimmt `plugin.path` nicht oder der Worker läuft noch mit altem plugin.path
3. **Trigger vorhanden?** `sqlite3 users.db "SELECT name FROM sqlite_master WHERE type='trigger';"` — fehlen sie, konnte der Connector die DB nicht schreiben (Rechte/Pfad prüfen)
4. **CDC-Log wächst?** `sqlite3 users.db "SELECT COUNT(*) FROM _debezium_cdc_log;"` — wenn hier Zeilen liegen, aber nichts im Topic ankommt: Worker-Log ansehen
5. **Topic-Name:** Events landen unter `sqlite.users` (`<topic.prefix>.<tabelle>`) — nicht unter `users` oder `spring-topic`
6. **Dateirechte:** Connector-Prozess braucht rw auf `users.db`, `users.db-wal`, `users.db-shm` und das Verzeichnis

---

## 6. Debezium-SQLite-Build schlägt fehl / dauert ewig

- Der Build holt `debezium-parent 3.8.0-SNAPSHOT` vom **Maven Central Snapshots**-Repo — erster Build kann mehrere Minuten dauern (viele SNAPSHOT-Dependencies), Timeout großzügig setzen
- Java 17+ nötig (wird über `~/.mavenrc` bzw. `mvnw` sichergestellt)
- Tests überspringen: `mvn clean package -Passembly -DskipTests` (Integrationstests brauchen sonst eine Umgebung mit laufendem Worker)
- Artefakt: `target/debezium-connector-sqlite-3.8.0-SNAPSHOT-plugin.tar.gz`

---

## 7. SQLite: `database is locked`

Kann passieren, wenn sehr viele Prozesse gleichzeitig schreiben. Der Writer setzt `busy_timeout=10000` und der Debezium-Connector liest in kurzen Transaktionen — bei diesem Setup nicht aufgetreten. Falls doch: Anzahl paralleler Schreiber reduzieren.

---

## 8. Topics nach Reboot „verschwunden“

Der Broker-Storage liegt unter `/tmp/kraft-combined-logs`. Wird `/tmp` geleert, sind die Metadaten weg. Abhilfe: Broker-Storage formatieren und neu starten — `spring-topic` legt die App selbst neu an, `sqlite.users` der Connector; der CDC-Offset ist danach ungültig ⇒ vollständiger neuer Snapshot der SQLite-Daten.
