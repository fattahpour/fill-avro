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

            // id: use record value or fallback to generated random key
            Object v = record.get("id");
            b.setId(v == null ? recordGenerator.randomKey() : v.toString());

            // strings: never leave null — use empty string as sensible default
            v = record.get("email");
            b.setEmail(v == null ? "" : v.toString());
            v = record.get("firstName");
            b.setFirstName(v == null ? "" : v.toString());
            v = record.get("lastName");
            b.setLastName(v == null ? "" : v.toString());

            // address: always set an Address builder with non-null string fields
            Object addrObj = record.get("address");
            Address.Builder ab = Address.newBuilder();
            if (addrObj instanceof GenericRecord) {
                GenericRecord addr = (GenericRecord) addrObj;
                Object av = addr.get("street"); ab.setStreet(av == null ? "" : av.toString());
                av = addr.get("city"); ab.setCity(av == null ? "" : av.toString());
                av = addr.get("postalCode"); ab.setPostalCode(av == null ? "" : av.toString());
            } else {
                ab.setStreet("").setCity("").setPostalCode("");
            }
            b.setAddressBuilder(ab);

            // tags: always set a (possibly empty) list
            Object tagsObj = record.get("tags");
            List<CharSequence> tags = new ArrayList<>();
            if (tagsObj instanceof List) {
                List<?> raw = (List<?>) tagsObj;
                for (Object o : raw) tags.add(o == null ? "" : o.toString());
            }
            b.setTags(tags);

            // metadata: always set a (possibly empty) map
            Object metaObj = record.get("metadata");
            Map<CharSequence, CharSequence> map = new HashMap<>();
            if (metaObj instanceof Map) {
                Map<?,?> raw = (Map<?,?>) metaObj;
                for (Map.Entry<?,?> e : raw.entrySet()) {
                    Object kk = e.getKey(); Object vv = e.getValue();
                    map.put(kk == null ? "" : kk.toString(), vv == null ? "" : vv.toString());
                }
            }
            b.setMetadata(map);

            // createdAt: use value or current time
            Object created = record.get("createdAt");
            b.setCreatedAt(created instanceof Number ? ((Number) created).longValue() : System.currentTimeMillis());

            // status: try to map, otherwise default to first enum value
            Object statusObj = record.get("status");
            Status statusToSet = null;
            if (statusObj != null) {
                String sym = statusObj.toString();
                try {
                    statusToSet = Status.valueOf(sym);
                } catch (IllegalArgumentException ignore) {
                    // will set default below
                }
            }
            if (statusToSet == null) {
                Status[] vals = Status.values();
                statusToSet = vals.length > 0 ? vals[0] : null;
            }
            if (statusToSet != null) b.setStatus(statusToSet);

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
