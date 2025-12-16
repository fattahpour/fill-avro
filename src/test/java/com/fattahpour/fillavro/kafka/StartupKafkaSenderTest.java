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
        assertEquals(Integer.valueOf(2), producerRecord.partition());
        assertEquals("generated-key", producerRecord.key());

        // value should be a SpecificRecord when generated classes are available;
        // fall back to GenericRecord in test environments where generated classes are not present.
        Object sent = producerRecord.value();
        org.junit.jupiter.api.Assertions.assertNotNull(sent);
        if (sent instanceof org.apache.avro.specific.SpecificRecord) {
          org.apache.avro.specific.SpecificRecord specific = (org.apache.avro.specific.SpecificRecord) sent;
          int idPos = specific.getSchema().getField("id").pos();
          org.junit.jupiter.api.Assertions.assertEquals("value", specific.get(idPos).toString());
        } else if (sent instanceof org.apache.avro.generic.GenericRecord) {
          org.apache.avro.generic.GenericRecord g = (org.apache.avro.generic.GenericRecord) sent;
          org.junit.jupiter.api.Assertions.assertEquals("value", g.get("id").toString());
        } else {
          org.junit.jupiter.api.Assertions.fail("Unexpected message payload type: " + sent.getClass());
        }
    }

        @Test
        void channelIsAvroChannel_parsableAsAvro() throws Exception {
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
        0,
        ""
      );

      sender.run();

      ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
      verify(kafkaTemplate).send(captor.capture());
      ProducerRecord<Object, Object> producerRecord = captor.getValue();

            // value should be a SpecificRecord when generated classes are available;
            // fall back to GenericRecord in test environments where generated classes are not present.
            Object sent = producerRecord.value();
            org.junit.jupiter.api.Assertions.assertNotNull(sent);
            if (sent instanceof org.apache.avro.specific.SpecificRecord) {
              org.apache.avro.specific.SpecificRecord specific = (org.apache.avro.specific.SpecificRecord) sent;
              int idPos = specific.getSchema().getField("id").pos();
              org.junit.jupiter.api.Assertions.assertEquals("value", specific.get(idPos).toString());
            } else if (sent instanceof org.apache.avro.generic.GenericRecord) {
              org.apache.avro.generic.GenericRecord g = (org.apache.avro.generic.GenericRecord) sent;
              org.junit.jupiter.api.Assertions.assertEquals("value", g.get("id").toString());
            } else {
              org.junit.jupiter.api.Assertions.fail("Unexpected message payload type: " + sent.getClass());
            }
        }
}
