package com.fattahpour.fillavro.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fattahpour.fillavro.avro.AvroRecordGenerator;
import com.fattahpour.fillavro.avro.AvroSchemaLoader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.fattahpour.ExampleUser;
import com.fattahpour.Address;
import com.fattahpour.Status;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class StartupKafkaSender implements CommandLineRunner {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final AvroSchemaLoader schemaLoader;
    private final AvroRecordGenerator recordGenerator;
    private final String schemaPath;
    private final String topic;
    private final Integer partition;
    private final String key;

    public StartupKafkaSender(KafkaTemplate<Object, Object> kafkaTemplate,
                              AvroSchemaLoader schemaLoader,
                              AvroRecordGenerator recordGenerator,
                              @Value("${app.avro.schema-path}") String schemaPath,
                              @Value("${app.kafka.topic}") String topic,
                              @Value("${app.kafka.partition}") Integer partition,
                              @Value("${app.kafka.key:}") String key) {
        this.kafkaTemplate = kafkaTemplate;
        this.schemaLoader = schemaLoader;
        this.recordGenerator = recordGenerator;
        this.schemaPath = schemaPath;
        this.topic = topic;
        this.partition = partition;
        this.key = key;
    }

    @Override
    public void run(String... args) {
        Schema schema = schemaLoader.load(schemaPath);
        GenericRecord record = recordGenerator.generateRecord(schema);
        String messageKey = (key != null && !key.isBlank()) ? key : recordGenerator.randomKey();
        // If generated SpecificRecord classes are available, try to build an ExampleUser instance
        // using the generated builder so the produced message is a true SpecificRecord.
        try {
            ExampleUser.Builder b = ExampleUser.newBuilder();
            Object v = record.get("id");
            if (v != null) b.setId(v.toString());
            v = record.get("email");
            if (v != null) b.setEmail(v.toString());
            v = record.get("firstName");
            if (v != null) b.setFirstName(v.toString());
            v = record.get("lastName");
            if (v != null) b.setLastName(v.toString());

            Object addrObj = record.get("address");
            if (addrObj instanceof GenericRecord) {
                GenericRecord addr = (GenericRecord) addrObj;
                Address.Builder ab = Address.newBuilder();
                Object av = addr.get("street"); if (av != null) ab.setStreet(av.toString());
                av = addr.get("city"); if (av != null) ab.setCity(av.toString());
                av = addr.get("postalCode"); if (av != null) ab.setPostalCode(av.toString());
                b.setAddressBuilder(ab);
            }

            Object tagsObj = record.get("tags");
            if (tagsObj instanceof List) {
                List<?> raw = (List<?>) tagsObj;
                List<CharSequence> tags = new ArrayList<>();
                for (Object o : raw) tags.add(o == null ? null : o.toString());
                b.setTags(tags);
            }

            Object metaObj = record.get("metadata");
            if (metaObj instanceof Map) {
                Map<?,?> raw = (Map<?,?>) metaObj;
                Map<CharSequence, CharSequence> map = new HashMap<>();
                for (Map.Entry<?,?> e : raw.entrySet()) {
                    Object kk = e.getKey(); Object vv = e.getValue();
                    map.put(kk == null ? null : kk.toString(), vv == null ? null : vv.toString());
                }
                b.setMetadata(map);
            }

            Object created = record.get("createdAt");
            if (created instanceof Number) b.setCreatedAt(((Number) created).longValue());

            Object statusObj = record.get("status");
            if (statusObj != null) {
                String sym = statusObj.toString();
                try {
                    b.setStatus(Status.valueOf(sym));
                } catch (IllegalArgumentException ignore) {
                    // leave default
                }
            }

            ExampleUser specific = b.build();
            ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic, partition, messageKey, specific);
            kafkaTemplate.send(producerRecord);
            return;
        } catch (Throwable t) {
            // If anything goes wrong (e.g. generated classes not available), fall back to deepCopy.
        }

        // fallback: attempt to convert GenericRecord to a generated SpecificRecord when available;
        Object specific = SpecificData.get().deepCopy(schema, record);
        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic, partition, messageKey, specific);
        kafkaTemplate.send(producerRecord);
    }
}
