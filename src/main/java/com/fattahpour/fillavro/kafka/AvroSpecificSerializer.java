package com.fattahpour.fillavro.kafka;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * Kafka serializer that prefers Avro SpecificRecord binary encoding but
 * falls back to GenericRecord encoding when a SpecificRecord implementation
 * is not available at runtime (e.g. when generated classes are missing).
 */
public class AvroSpecificSerializer implements Serializer<Object> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) return null;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            if (data instanceof SpecificRecord) {
                @SuppressWarnings("unchecked")
                SpecificDatumWriter<SpecificRecord> writer = new SpecificDatumWriter<>(((SpecificRecord) data).getSchema());
                writer.write((SpecificRecord) data, encoder);
            } else if (data instanceof GenericRecord) {
                GenericRecord gr = (GenericRecord) data;
                @SuppressWarnings("unchecked")
                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(gr.getSchema());
                writer.write(gr, encoder);
            } else {
                // Last resort: try to cast to SpecificRecord (for compatibility) and fail with clear message
                throw new IllegalArgumentException("AvroSpecificSerializer expected SpecificRecord or GenericRecord, got: " + data.getClass());
            }

            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Avro record to binary", e);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
