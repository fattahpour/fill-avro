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
        // attempt to convert GenericRecord to a generated SpecificRecord when available;
        // do not assume conversion returns a SpecificRecord (tests may run without generated classes).
        Object specific = SpecificData.get().deepCopy(schema, record);
        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(topic, partition, messageKey, specific);
        kafkaTemplate.send(producerRecord);
    }
}
