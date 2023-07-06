package com.google.cloud.syndeo.perf.provider.utils;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestDataGeneration {
    private TestDataGeneration() {
    }

    public static Stream<Row> rowsFromSchema(Schema sc) {
        return rowsFromSchema(sc, 0);
    }

    public static String randomString(Integer length, Random random) {
        return random
                .ints(length, 48, 122)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private static Object generateFieldValue(Schema.FieldType fieldType, Random random) {
        switch (fieldType.getTypeName()) {
            case STRING:
                return randomString(random.nextInt() % 16, random);
            case DECIMAL:
                return BigDecimal.valueOf(random.nextDouble());
            case INT32:
                return random.nextInt();
            case INT16:
                return (short)random.nextInt();
            case INT64:
                return random.nextLong();
            case BYTES:
                byte[] bs = new byte[random.nextInt() % 256];
                random.nextBytes(bs);
                return bs;
            case FLOAT:
                return random.nextFloat();
            case DOUBLE:
                return random.nextDouble();
            case ROW:
                return generateRow(fieldType.getRowSchema(), random);
            case BOOLEAN:
                return random.nextBoolean();
            case DATETIME:
                return new DateTime(random.nextLong());
            case ARRAY:
            case ITERABLE:
                return rowsFromSchema(fieldType.getRowSchema(), random.nextInt())
                        .limit(random.nextInt() % 256)
                        .collect(Collectors.toList());
            case BYTE:
                return (byte)random.nextInt();
            case MAP:
                Map<Object, Object> m = new HashMap<>();
                for (int i = 0; i < random.nextInt() % 256; ++i) {
                    m.put(generateFieldValue(fieldType.getMapKeyType(), random), generateFieldValue(fieldType.getMapValueType(), random));
                }
                return m;
            default:
                throw new IllegalArgumentException("Cannot create value of type " + fieldType.getTypeName().toString());
        }
    }

    private static Row generateRow(Schema sc, Random random) {
        Schema.Field firstField = sc.getField(0);
        Row.FieldValueBuilder fvb = Row.withSchema(sc).withFieldValue(firstField.getName(), generateFieldValue(firstField.getType(), random));

        for (Schema.Field field : sc.getFields()) {
            fvb = fvb.withFieldValue(field.getName(), generateFieldValue(field.getType(), random));
        }

        return fvb.build();
    }

    public static Stream<Row> rowsFromSchema(Schema sc, int seed) {
        Random r = new Random(seed);
        return Stream.generate(() -> generateRow(sc, r));
    }
}
