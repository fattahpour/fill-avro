package com.fattahpour.fillavro.avro;

import net.datafaker.Faker;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import com.fattahpour.fillavro.config.AvroConfigProperties;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@Component
public class AvroRecordGenerator {

    private final Faker faker;

    private final AvroConfigProperties config;

    public AvroRecordGenerator() {
        this(new Faker(Locale.ENGLISH), new AvroConfigProperties());
    }

    public AvroRecordGenerator(Faker faker) {
        this(faker, new AvroConfigProperties());
    }

    public AvroRecordGenerator(Faker faker, AvroConfigProperties config) {
        this.faker = Objects.requireNonNull(faker);
        this.config = config == null ? new AvroConfigProperties() : config;
    }

    @Autowired
    public AvroRecordGenerator(AvroConfigProperties config) {
        this(new Faker(Locale.ENGLISH), config);
    }

    public GenericRecord generateRecord(Schema schema) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Schema must be a record");
        }
        return generateRecord(schema, "");
    }

    private GenericRecord generateRecord(Schema schema, String path) {
        GenericData.Record record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            Object value = generateValue(field.schema(), field.name(), path);
            record.put(field.name(), value);
        }
        return record;
    }

    public String randomKey() {
        return faker.internet().uuid();
    }

    private Object generateValue(Schema schema, String fieldName, String path) {
        String fullPath = (path == null || path.isBlank()) ? fieldName : path + "." + fieldName;
        // honor full-path override first, then short name
        List<String> overrides = config.getFields().get(fullPath);
        if (overrides == null || overrides.isEmpty()) {
            overrides = config.getFields().get(fieldName);
        }
        if (overrides != null && !overrides.isEmpty()) {
            String chosen = overrides.get(faker.random().nextInt(overrides.size()));
            return convertOverrideToType(schema, chosen);
        }

        return switch (schema.getType()) {
            case RECORD -> generateRecord(schema, fullPath);
            case ARRAY -> generateArray(schema.getElementType(), fieldName, fullPath);
            case MAP -> generateMap(schema.getValueType(), fieldName, fullPath);
            case UNION -> generateUnion(schema.getTypes(), fieldName, fullPath);
            case ENUM -> randomEnumSymbol(schema);
            case FIXED -> randomFixed(schema);
            case STRING -> generateStringValue(fieldName);
            case INT -> generateIntValue(fieldName);
            case LONG -> generateLongValue(fieldName);
            case FLOAT -> generateFloatValue(fieldName);
            case DOUBLE -> generateDoubleValue(fieldName);
            case BOOLEAN -> faker.bool().bool();
            case BYTES -> ByteBuffer.wrap(randomBytes(8));
            case NULL -> null;
        };
    }

    private Object convertOverrideToType(Schema schema, String value) {
        if (value == null) return null;
        switch (schema.getType()) {
            case STRING:
                return value;
            case INT:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case ENUM:
                return new GenericData.EnumSymbol(schema, value);
            case FIXED:
            case BYTES:
                return java.nio.ByteBuffer.wrap(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            case ARRAY:
                // expect value like "a|b|c" for array elements
                String[] parts = value.split("\\|");
                java.util.List<Object> list = new java.util.ArrayList<>();
                for (String p : parts) {
                    list.add(convertOverrideToType(schema.getElementType(), p));
                }
                return list;
            case MAP:
                // expect comma-separated entries key:val,key2:val2
                java.util.Map<String, Object> map = new java.util.HashMap<>();
                String[] entries = value.split(",");
                for (String e : entries) {
                    String[] kv = e.split(":", 2);
                    if (kv.length == 2) {
                        map.put(kv[0], convertOverrideToType(schema.getValueType(), kv[1]));
                    }
                }
                return map;
            case RECORD:
                // not supported via simple override; expect a JSON string and attempt to parse? fallback to null
                return null;
            case UNION:
                // pick first non-null type and convert
                for (Schema s : schema.getTypes()) {
                    if (s.getType() != Schema.Type.NULL) {
                        return convertOverrideToType(s, value);
                    }
                }
                return null;
            case NULL:
                return null;
            default:
                return value;
        }
    }

    private Object generateUnion(List<Schema> types, String fieldName, String path) {
        boolean hasNull = types.stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
        List<Schema> nonNullTypes = types.stream()
                .filter(t -> t.getType() != Schema.Type.NULL)
                .collect(Collectors.toList());
        if (hasNull && faker.random().nextBoolean()) {
            return null;
        }
        if (nonNullTypes.isEmpty()) {
            return null;
        }
        Schema chosen = nonNullTypes.get(faker.random().nextInt(nonNullTypes.size()));
        return generateValue(chosen, fieldName, path);
    }

    private Object generateUnion(List<Schema> types, String fieldName) {
        return generateUnion(types, fieldName, fieldName);
    }

    private List<Object> generateArray(Schema elementType, String fieldName, String path) {
        int size = faker.random().nextInt(1, 3);
        return ThreadLocalRandom.current()
                .ints(size, 0, Integer.MAX_VALUE)
                .mapToObj(ignored -> generateValue(elementType, fieldName, path))
                .collect(Collectors.toList());
    }

    private Map<String, Object> generateMap(Schema valueType, String fieldName, String path) {
        int size = faker.random().nextInt(1, 3);
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(faker.lorem().word(), generateValue(valueType, fieldName, path));
        }
        return map;
    }

    private Object randomEnumSymbol(Schema schema) {
        List<String> symbols = schema.getEnumSymbols();
        int index = faker.random().nextInt(symbols.size());
        return new GenericData.EnumSymbol(schema, symbols.get(index));
    }

    private Object randomFixed(Schema schema) {
        return new GenericData.Fixed(schema, randomBytes(schema.getFixedSize()));
    }

    private String generateStringValue(String fieldName) {
        String lower = fieldName.toLowerCase(Locale.ENGLISH);
        if (lower.contains("email")) {
            return faker.internet().emailAddress();
        }
        if (lower.contains("firstname") || (lower.contains("first") && lower.contains("name"))) {
            return faker.name().firstName();
        }
        if (lower.contains("lastname") || lower.contains("surname") || (lower.contains("last") && lower.contains("name"))) {
            return faker.name().lastName();
        }
        if (lower.equals("name") || lower.contains("fullname")) {
            return faker.name().fullName();
        }
        if (isAddressField(lower)) {
            return faker.address().fullAddress();
        }
        if (lower.contains("street")) {
            return faker.address().streetAddress();
        }
        if (lower.contains("city")) {
            return faker.address().city();
        }
        if (lower.contains("state")) {
            return faker.address().state();
        }
        if (lower.contains("country")) {
            return faker.address().country();
        }
        if (lower.contains("postal") || lower.contains("zip")) {
            return faker.address().zipCode();
        }
        if (lower.contains("phone") || lower.contains("mobile")) {
            return faker.phoneNumber().phoneNumber();
        }
        if (lower.endsWith("id") || lower.contains("userid")) {
            return faker.internet().uuid();
        }
        if (lower.contains("url") || lower.contains("website")) {
            return faker.internet().url();
        }
        if (lower.contains("ip")) {
            return faker.internet().ipV4Address();
        }
        if (isDateTimeField(lower)) {
            return String.valueOf(randomEpochMillis());
        }
        if (isAmountField(lower)) {
            return faker.commerce().price(10, 5000);
        }
        return faker.lorem().sentence();
    }

    private Integer generateIntValue(String fieldName) {
        String lower = fieldName.toLowerCase(Locale.ENGLISH);
        if (isDateTimeField(lower)) {
            return (int) (randomEpochMillis() % Integer.MAX_VALUE);
        }
        if (isAmountField(lower)) {
            return faker.number().numberBetween(1, 10_000);
        }
        return faker.number().numberBetween(0, 1000);
    }

    private Long generateLongValue(String fieldName) {
        String lower = fieldName.toLowerCase(Locale.ENGLISH);
        if (isDateTimeField(lower)) {
            return randomEpochMillis();
        }
        if (isAmountField(lower)) {
            return faker.number().numberBetween(100L, 10000L);
        }
        return faker.number().numberBetween(0L, 100000L);
    }

    private Float generateFloatValue(String fieldName) {
        String lower = fieldName.toLowerCase(Locale.ENGLISH);
        if (isAmountField(lower)) {
            return (float) faker.number().randomDouble(2, 10, 1000);
        }
        return (float) faker.number().randomDouble(2, 0, 100);
    }

    private Double generateDoubleValue(String fieldName) {
        String lower = fieldName.toLowerCase(Locale.ENGLISH);
        if (isAmountField(lower)) {
            return faker.number().randomDouble(2, 10, 5000);
        }
        return faker.number().randomDouble(2, 0, 1000);
    }

    private boolean isAddressField(String lower) {
        return lower.contains("address") || lower.contains("street") || lower.contains("city") || lower.contains("state");
    }

    private boolean isDateTimeField(String lower) {
        return lower.contains("created") || lower.contains("updated") || lower.contains("timestamp")
                || lower.contains("date") || lower.contains("time");
    }

    private boolean isAmountField(String lower) {
        return lower.contains("amount") || lower.contains("price") || lower.contains("balance");
    }

    private long randomEpochMillis() {
        Instant instant = faker.date().past(10_000, java.util.concurrent.TimeUnit.DAYS).toInstant();
        return instant.toEpochMilli();
    }

    private byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        java.util.concurrent.ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
