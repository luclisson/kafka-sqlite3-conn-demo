# Spring-Boot-Anwendung (`kafka.demo/`)

## Eckdaten

| | |
|---|---|
| Framework | Spring Boot **4.1.2** (Spring Framework 7) |
| Java | **27** (`<java.version>27</java.version>`) |
| Gruppe / Artefakt | `de.coba` / `kafka.demo` |
| Basis-Package | `de.coba.kafka` |
| Starter | `spring-boot-starter-kafka`, `spring-boot-starter-webmvc` (+ Test-Varianten) |

## Projektstruktur

```
kafka.demo/src/main/java/de/coba/kafka/
├── Application.java               # @SpringBootApplication, Einstiegspunkt
├── config/
│   └── KafkaTopicConfig.java      # NewTopic-Bean "spring-topic"
├── model/
│   └── DemoMessage.java           # record DemoMessage(String text, Instant createdAt)
├── producer/
│   └── MessageProducer.java       # send() via KafkaTemplate
├── consumer/
│   └── MessageConsumer.java       # @KafkaListener auf "spring-topic"
└── controller/
    └── MessageController.java     # POST /messages → Producer
```

## Klassen im Einzelnen

### `config/KafkaTopicConfig`

```java
@Bean
public NewTopic springTopic() {
    return TopicBuilder.name("spring-topic").build();
}
```

Spring Boot konfiguriert automatisch einen `KafkaAdmin`, der die Topics aller `NewTopic`-Beans **beim App-Start anlegt**, falls sie fehlen (Default-Partitionzahl, keine speziellen Optionen).

### `model/DemoMessage`

Einfaches Java-Record mit Text und Zeitstempel — dient als Payload zum Testen des JSON-Serialisierungsroundtrips.

### `producer/MessageProducer`

- Injiziert `KafkaTemplate<String, DemoMessage>` (Key-Typ String, Value-Typ DemoMessage)
- `send(DemoMessage)` → `kafkaTemplate.send("spring-topic", message)` + Log-Ausgabe

### `consumer/MessageConsumer`

```java
@KafkaListener(topics = "spring-topic")
public void consume(DemoMessage message) { log.info("received message: {}", message); }
```

Deserialisiert automatisch in `DemoMessage`; Group-ID kommt aus `application.properties` (`demo-group`).

### `controller/MessageController`

```java
@PostMapping
public void send(@RequestBody String text) {
    producer.send(new DemoMessage(text, Instant.now()));
}
```

## Konfiguration: `application.properties`

```properties
spring.kafka.bootstrap-servers=localhost:9092

spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer

spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JacksonJsonDeserializer
spring.kafka.consumer.group-id=demo-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.properties.spring.json.trusted.packages=*
spring.kafka.consumer.properties.spring.json.value.default.type=de.coba.kafka.model.DemoMessage
```

### ⚠️ Wichtig: Jackson 3, nicht Jackson 2

Spring Boot 4 bringt **Jackson 3** (`tools.jackson.*`) mit. Die klassischen spring-kafka-Klassen `JsonSerializer`/`JsonDeserializer` (Paket `org.springframework.kafka.support.serializer`) brauchen **Jackson 2** (`com.fasterxml.jackson.*`) und werfen sonst beim Consumer-Start:

```
java.lang.NoClassDefFoundError: com/fasterxml/jackson/databind/JavaType
```

→ Deshalb hier die Jackson-3-Varianten **`JacksonJsonSerializer`** / **`JacksonJsonDeserializer`** (gleiche `spring.json.*`-Properties, gleiche Funktion).

| Property | Zweck |
|---|---|
| `group-id` | Consumer-Group `demo-group` |
| `auto-offset-reset=earliest` | Ohne gespeicherten Offset von vorn lesen |
| `spring.json.trusted.packages=*` | Deserialisierung beliebiger Packages erlauben (Demo; in Produktion einschränken!) |
| `spring.json.value.default.type` | Zieltyp, wenn im Header keine Typinfo steht |

## Build & Run & Test

```bash
cd ~/Documents/repos/kafka-demo/kafka.demo

./mvnw clean install     # inkl. contextLoads-Test (braucht laufenden Broker)
./mvnw spring-boot:run   # Start auf Port 8080

# Mit System-mvn (nach .mavenrc-Setup, siehe setup.md):
mvn clean install
```

## End-to-End-Test

```bash
# Senden:
curl -X POST localhost:8080/messages \
     -H "Content-Type: text/plain" \
     -d "hello kafka"
# → HTTP 200; im App-Log: "sending message: DemoMessage[text=hello kafka, ...]"

# Topic-Inhalt:
kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic spring-topic --from-beginning --timeout-ms 10000
# → {"text":"hello kafka","createdAt":"2026-10-28T18:42:22.990348409Z"}

# Empfang (App-Log):
# received message: DemoMessage[text=hello kafka, createdAt=...]
```

Roundtrip: REST → Record → `JacksonJsonSerializer` → Broker → `JacksonJsonDeserializer` → Record → Log.
