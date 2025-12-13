package com.fattahpour.fillavro.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fattahpour.fillavro.avro.AvroRecordGenerator;
import com.fattahpour.fillavro.avro.AvroSchemaLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StartupKafkaSenderTest {

    @Mock
    KafkaTemplate<Object, Object> kafkaTemplate;

    @Mock
    AvroSchemaLoader schemaLoader;

    @Mock
    AvroRecordGenerator recordGenerator;

    @Test
    void sendsExactlyOneRecordOnStartup() throws Exception {
        String schemaJson = """
                {
                  "type": "record",
                  "name": "Simple",
                  "fields": [{"name": "id", "type": "string"}]
                }
                """;
        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", "value");

        when(schemaLoader.load("path")).thenReturn(schema);
        when(recordGenerator.generateRecord(schema)).thenReturn(record);
        when(recordGenerator.randomKey()).thenReturn("generated-key");

        StartupKafkaSender sender = new StartupKafkaSender(
                kafkaTemplate,
                schemaLoader,
                recordGenerator,
                "path",
                "topic",
                2,
                ""
        );

        sender.run();

        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());

        ProducerRecord<Object, Object> producerRecord = captor.getValue();
        assertEquals("topic", producerRecord.topic());
        assertEquals(2, producerRecord.partition());
        assertEquals("generated-key", producerRecord.key());

        // build expected Avro JSON string using the same Avro JSON encoder
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        org.apache.avro.io.DatumWriter<GenericRecord> writer = new org.apache.avro.generic.GenericDatumWriter<>(schema);
        org.apache.avro.io.Encoder encoder = org.apache.avro.io.EncoderFactory.get().jsonEncoder(schema, out);
        writer.write(record, encoder);
        encoder.flush();
        String expectedJson = out.toString(java.nio.charset.StandardCharsets.UTF_8.name());

        assertEquals(expectedJson, producerRecord.value());
    }
}
