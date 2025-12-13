package com.fattahpour.fillavro.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "app.avro")
public class AvroConfigProperties {

    /**
     * Field overrides. Key = field name, value = list of candidate override strings.
     * For array field overrides provide a single string where elements are separated with '|',
     * e.g. "a|b|c". To provide multiple candidate arrays set multiple comma-separated values
     * which will be bound into the list.
     */
    private Map<String, List<String>> fields = new HashMap<>();

    public Map<String, List<String>> getFields() {
        return fields;
    }

    public void setFields(Map<String, List<String>> fields) {
        this.fields = fields;
    }
}
