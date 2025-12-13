package com.fattahpour.fillavro.avro;

import net.datafaker.Faker;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroRecordGeneratorTest {

    private AvroRecordGenerator generator;
    private Schema schema;

    @BeforeEach
    void setUp() {
        generator = new AvroRecordGenerator(new Faker(Locale.ENGLISH));
        String schemaJson = """
                {
                  "type": "record",
                  "name": "User",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "firstName", "type": "string"},
                    {"name": "phone", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "address", "type": {
                      "type": "record",
                      "name": "Address",
                      "fields": [
                        {"name": "city", "type": "string"},
                        {"name": "postalCode", "type": "string"}
                      ]
                    }},
                    {"name": "tags", "type": {"type": "array", "items": "string"}},
                    {"name": "attributes", "type": {"type": "map", "values": "string"}},
                    {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "ACTIVE", "BLOCKED"]}},
                    {"name": "optionalEmail", "type": ["null", "string"]}
                  ]
                }
                """;
        schema = new Schema.Parser().parse(schemaJson);
    }

    @Test
    void generatesPopulatedRecordRespectingSchema() {
        GenericRecord record = generator.generateRecord(schema);

        assertNotNull(record.get("id"));
        assertNotNull(record.get("email"));
        assertNotNull(record.get("phone"));
        assertTrue(record.get("age") instanceof Integer);

        GenericRecord address = (GenericRecord) record.get("address");
        assertNotNull(address.get("city"));
        assertNotNull(address.get("postalCode"));

        assertFalse(((java.util.List<?>) record.get("tags")).isEmpty());
        assertFalse(((Map<?, ?>) record.get("attributes")).isEmpty());

        GenericData.EnumSymbol status = (GenericData.EnumSymbol) record.get("status");
        assertTrue(schema.getField("status").schema().getEnumSymbols().contains(status.toString()));

        Object optionalEmail = record.get("optionalEmail");
        if (optionalEmail != null) {
            assertTrue(optionalEmail instanceof String);
        }
    }

    @Test
    void producesSemanticallyMeaningfulValues() {
        GenericRecord record = generator.generateRecord(schema);

        String email = record.get("email").toString();
        assertTrue(email.contains("@"));

        String firstName = record.get("firstName").toString();
        assertFalse(firstName.isEmpty());

        String phone = record.get("phone").toString();
        assertFalse(phone.isEmpty());

        String postal = ((GenericRecord) record.get("address")).get("postalCode").toString();
        assertFalse(postal.isEmpty());
    }

      @Test
      void honorsNestedFieldOverride_dotNotation() {
        // prepare config with nested override for address.city
        com.fattahpour.fillavro.config.AvroConfigProperties cfg = new com.fattahpour.fillavro.config.AvroConfigProperties();
        java.util.Map<String, java.util.List<String>> map = new java.util.HashMap<>();
        map.put("address.city", java.util.List.of("Smallville"));
        cfg.setFields(map);

        AvroRecordGenerator customGenerator = new AvroRecordGenerator(new Faker(Locale.ENGLISH), cfg);
        GenericRecord record = customGenerator.generateRecord(schema);

        GenericRecord address = (GenericRecord) record.get("address");
        assertNotNull(address);
        assertEquals("Smallville", address.get("city").toString());
      }
}
