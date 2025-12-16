package com.fattahpour.fillavro.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Simple standalone producer that reads an Avro schema and a JSON file, converts the JSON
 * into an Avro SpecificRecord and produces it to a Kafka topic as Avro binary bytes.
 *
 * Configuration is embedded in the class (edit the constants below).
 *
 * Notes:
 * - This serializes Avro binary; consumers should deserialize with the same schema.
 * - Adjust `SCHEMA_PATH` and `JSON_PATH` to point to your files.
 */
public class JsonToSpecificProducer {

    // --- Configuration (edit as needed) ---------------------------------
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "example-user-topic";
    private static final String KEY = "key1";
    private static final String SCHEMA_PATH = "src/main/resources/schema.avsc"; // project-relative
    private static final String JSON_PATH = "src/main/resources/data.json"; // project-relative
    // ---------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        System.out.println("JsonToSpecificProducer starting...");

        // Load schema
        Schema schema = new Schema.Parser().parse(new File(SCHEMA_PATH));
        System.out.println("Loaded schema: " + schema.getFullName());

        // Read JSON content
        String json = Files.readString(Path.of(JSON_PATH));
        System.out.println("Read JSON (length=" + json.length() + ") from " + JSON_PATH);

        // Convert JSON -> GenericRecord
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        GenericRecord generic = reader.read(null, DecoderFactory.get().jsonDecoder(schema, json));

        // Convert GenericRecord -> SpecificRecord (if generated classes available)
        Object specificObj = SpecificData.get().deepCopy(schema, generic);
        if (!(specificObj instanceof SpecificRecord)) {
            System.out.println("Warning: generated SpecificRecord class not available for schema " + schema.getFullName() + ". Sending binary of GenericRecord instead.");
        } else {
            System.out.println("Built SpecificRecord instance of " + specificObj.getClass().getName());
        }

        // Serialize record to Avro binary
        byte[] payload = toAvroBytes((SpecificRecord) specificObj, schema);

        // Produce to Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("acks", "all");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, KEY, payload);
            producer.send(record).get();
            System.out.println("Sent record to topic='" + TOPIC + "' key='" + KEY + "' valueBytes=" + payload.length);
        }

        System.out.println("JsonToSpecificProducer finished.");
    }

    private static byte[] toAvroBytes(SpecificRecord record, Schema schema) throws IOException {
        if (record == null) throw new IllegalArgumentException("record is null");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
