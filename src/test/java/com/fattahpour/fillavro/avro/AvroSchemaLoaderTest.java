package com.fattahpour.fillavro.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class AvroSchemaLoaderTest {

    @Test
    void loadsSchemaFromFile() throws Exception {
        String schemaJson = """
                {
                  "type": "record",
                  "name": "TempRecord",
                  "fields": [
                    {"name": "field1", "type": "string"}
                  ]
                }
                """;
        Path tempFile = Files.createTempFile("schema", ".avsc");
        Files.writeString(tempFile, schemaJson);

        AvroSchemaLoader loader = new AvroSchemaLoader();
        Schema schema = loader.load(tempFile.toString());

        assertNotNull(schema);
        assertEquals("TempRecord", schema.getName());
        assertEquals(1, schema.getFields().size());
    }
}
