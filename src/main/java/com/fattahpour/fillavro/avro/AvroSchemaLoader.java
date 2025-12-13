package com.fattahpour.fillavro.avro;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

@Component
public class AvroSchemaLoader {

    public Schema load(String schemaPath) {
        Objects.requireNonNull(schemaPath, "schemaPath");
        Schema.Parser parser = new Schema.Parser();
        Path path = Paths.get(schemaPath);
        if (Files.exists(path)) {
            try (InputStream in = Files.newInputStream(path)) {
                return parser.parse(in);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read schema at " + schemaPath, e);
            }
        }
        InputStream resource = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(schemaPath);
        if (resource != null) {
            try (resource) {
                return parser.parse(resource);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read schema resource " + schemaPath, e);
            }
        }
        throw new IllegalArgumentException("Schema not found at path or resource: " + schemaPath);
    }
}
