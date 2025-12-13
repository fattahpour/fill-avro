# fill-avro

A small Java application that generates Avro GenericRecords and (on startup) publishes them to a Kafka topic.

This repository contains:
- `src/main/java/com/fattahpour/fillavro/avro` — Avro schema loader and record generator.
- `src/main/java/com/fattahpour/fillavro/kafka` — A startup component that sends a generated record to Kafka.
- `src/main/resources/application.properties` — Application configuration (Kafka, Avro path, topic).

**Requirements**

- Java 17+ (project uses modern Spring Boot / Java versions).
- Maven (to build and run tests).
- A Kafka broker running locally (default: `localhost:9092`) for end-to-end runs.

**Quickstart — build & test**

Build the project and run unit tests with Maven:

```bash
cd /path/to/fill-avro
mvn test
```

The repository includes unit tests for Avro schema loading, record generation and the startup Kafka sender.

**Run the app (dev)**

Configure the broker and topic in `src/main/resources/application.properties` or via environment variables. Defaults are:

```properties
spring.kafka.bootstrap-servers=localhost:9092
app.avro.schema-path=schema.avsc
app.kafka.topic=example-topic
app.kafka.partition=0
```

Then run with Maven:

```bash
mvn spring-boot:run
```

On startup the application will:
- Load the Avro schema at `app.avro.schema-path` (default `schema.avsc`).
- Use the `AvroRecordGenerator` to build a `GenericRecord` based on the schema.
- Serialize the record to an Avro JSON string and publish it to the configured Kafka topic.

Note: The project sends the Avro record as a JSON string (not Avro binary) and the Kafka producer is configured to use a `StringSerializer` for the value. This avoids Jackson trying to introspect Avro `Schema` objects and causing serialization errors during startup.

If you prefer Avro binary messages or to integrate with a Schema Registry, change the producer configuration and serializer to the appropriate Avro serializer (e.g., Confluent's `KafkaAvroSerializer`) and add the schema registry client dependencies.

**Integration testing with Docker (optional)**

You can run Kafka locally via Docker (one compact option is Redpanda):

```bash
# start a simple Redpanda container
docker run -d --name redpanda -p 9092:9092 vectorized/redpanda:latest redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M

# then run the app (it will target localhost:9092)
mvn spring-boot:run
```

Or use a docker-compose with Kafka + Zookeeper if you prefer the Confluent stack.

**Troubleshooting**

- If the application fails on startup with a serialization error referencing Avro `Schema`, ensure the producer value serializer is `StringSerializer` (this project converts Avro GenericRecord to a JSON string before sending).
- To see Spring conditional evaluation details re: auto-configuration, start with `--debug`:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--debug"
```

**Next improvements**

- Add an integration test that starts a lightweight Kafka (via `spring-kafka-test` or Testcontainers) and verifies a real message arrives on the topic.
- Optionally switch to Avro binary + Schema Registry for production message formats.

**Field Overrides (configurable)**

You can force specific values for generated Avro fields via Spring properties using the `app.avro.fields` map. This is useful to seed known values (for testing or predictable startup messages) or to override nested fields.

- Key format: `app.avro.fields.<field>=<value1>,<value2>`
- Nested fields: use dot-notation for nested record fields, e.g. `app.avro.fields.address.city=Smallville`
- If multiple comma-separated values are provided for a single field, the generator will randomly select one at record generation time.
- Arrays: provide array elements separated by `|`, e.g. `app.avro.fields.tags=tag1|tag2|tag3`
- Maps: provide comma-separated `key:val` entries, e.g. `app.avro.fields.attributes=foo:bar,baz:qux`
- Enums and primitives: values will be coerced to the field type (e.g. `42` -> `int`, `ACTIVE` -> enum symbol)

Examples (`src/main/resources/application.properties`):

```properties
# set a nested value
app.avro.fields.address.city=Smallville

# override array contents (generator expects '|' separator for array elements)
app.avro.fields.tags=red|green|blue

# set a map value (key:val entries)
app.avro.fields.attributes=env:dev,region:eu
```

Notes:
- The generator checks for a full dot-path override first (e.g. `address.city`) and falls back to the short field name (e.g. `city`) if the path key is not found.
- RECORD-type fields (complex nested records) are not supported as a single-string override (you can override individual nested fields using dot-notation).

Unit test verifying nested override behavior:

- `src/test/java/com/fattahpour/fillavro/avro/AvroRecordGeneratorTest.java` contains `honorsNestedFieldOverride_dotNotation()` which demonstrates `app.avro.fields.address.city=Smallville` is honored by the generator.


**License & Contributing**

This repository contains sample/demo code. Feel free to open issues or PRs for improvements.
