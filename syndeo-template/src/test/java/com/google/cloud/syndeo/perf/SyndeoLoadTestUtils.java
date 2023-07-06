/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.syndeo.perf;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.transforms.SyndeoStatsSchemaTransformProvider;
import com.google.cloud.syndeo.transforms.TypedSchemaTransformProvider;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang.SerializationUtils;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

public class SyndeoLoadTestUtils {

  public static final Long MAX_ROWS_PER_SPLIT = 1500L;

  public static String mapToJsonPayload(Map<String, Object> syndeoPipelineDefinition) {
    try {
      return new ObjectMapper().writeValueAsString(syndeoPipelineDefinition);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static class LongRange implements Iterable<Long> {
    private final long startInclusive;
    private final long endExclusive;

    public LongRange(long startInclusive, long endExclusive) {
      this.startInclusive = startInclusive;
      this.endExclusive = endExclusive;
    }

    @NotNull
    @Override
    public Iterator<Long> iterator() {
      return new Iterator<Long>() {
        long next = startInclusive;

        @Override
        public boolean hasNext() {
          return next < endExclusive;
        }

        @Override
        public Long next() {
          return next++;
        }
      };
    }
  }

  private static PCollection<Long> fastLongSequence(Pipeline p, long numRows) {
    long chunkLen = 1 << 16;
    long numChunks = numRows / chunkLen;

    if (numChunks == 0) {
      return p.apply(Create.of(new LongRange(0, numRows)));
    }

    PCollection<Long> bulk = p.apply(Create.of(new LongRange(0, numChunks)))
            .apply(FlatMapElements.into(TypeDescriptors.longs())
                    .via(i -> new LongRange(i * chunkLen, i * chunkLen + chunkLen)));

    if (numRows % chunkLen == 0) {
        return bulk;
    }

    PCollection<Long> remainder = p.apply(Create.of(
            new LongRange(numChunks * chunkLen, numChunks * chunkLen + numRows % chunkLen)));

    return PCollectionList.of(bulk).and(remainder).apply(Flatten.pCollections());
  }

  private static PCollection<Long> longSequence(
      Pipeline dataGenerator, Long numRows, Long runtimeSeconds) {
    final long numSplits = Math.max(numRows / MAX_ROWS_PER_SPLIT, 1);
    final long periodPerSplitMsecs = Math.max((runtimeSeconds * 1000) / numSplits, 1);
    System.out.printf(
        "Producing %s rows in %s splits. Each split every %s msecs. Each split has max %s rows.%n",
        numRows, numSplits, periodPerSplitMsecs, MAX_ROWS_PER_SPLIT);
    final Instant startTime = Instant.now();
    return dataGenerator
        .apply(
            PeriodicImpulse.create()
                .startAt(startTime)
                .stopAt(Instant.now().plus(Duration.standardSeconds(runtimeSeconds)))
                .withInterval(Duration.millis(periodPerSplitMsecs)))
        .apply(Reshuffle.viaRandomKey())
        .apply(
            FlatMapElements.into(TypeDescriptors.longs())
                .via(
                    inst -> {
                      assert inst != null;
                      long ordinal =
                          inst.minus(Duration.millis(startTime.getMillis())).getMillis()
                              / periodPerSplitMsecs;
                      return LongStream.range(
                              ordinal * MAX_ROWS_PER_SPLIT,
                              Math.min(numRows, (ordinal + 1) * MAX_ROWS_PER_SPLIT))
                          .boxed()
                          .collect(Collectors.toList());
                    }));
  }

  public static PCollection<Row> inputData(
      Pipeline dataGenerator, Long numRows, Long runtimeSeconds, Schema dataSchema) {
    return PCollectionRowTuple.of(
            "input",
            fastLongSequence(dataGenerator, numRows)
                .apply(
                    MapElements.into(TypeDescriptors.rows())
                        .via(
                            ordinal ->
                                SyndeoLoadTestUtils.randomRowForSchema(
                                    dataSchema, 0.05, new Random(ordinal))))
                .setRowSchema(dataSchema))
        .apply(
            new SyndeoStatsSchemaTransformProvider()
                .from(
                    SyndeoStatsSchemaTransformProvider.SyndeoStatsConfiguration.create("inputData"))
                .buildTransform())
        .get("output");
  }

  // arrays do not work in bigquery, so this super simple table schema omits it
  // dateTime is also omitted because it seems that the syndeo template has issues matching the schema
  public static final Schema SUPER_SIMPLE_TABLE_SCHEMA =
          Schema.builder()
                  .addField(Schema.Field.nullable("commit", Schema.FieldType.STRING))
                  .addField(Schema.Field.nullable("repo_name", Schema.FieldType.STRING))
                  .addField(Schema.Field.nullable("message", Schema.FieldType.STRING))
                  .addInt64Field("linesAdded")
                  .addBooleanField("merged")
                  .addByteArrayField("sha1")
                  .build();

  // A schema for a table that has been slightly inspired on the Github public dataset
  // provided by BigQuery.
  public static final Schema SIMPLE_TABLE_SCHEMA =
      Schema.builder()
          .addField(Schema.Field.nullable("commit", Schema.FieldType.STRING))
          .addField(Schema.Field.nullable("repo_name", Schema.FieldType.STRING))
          .addField(Schema.Field.of("parent", Schema.FieldType.array(Schema.FieldType.STRING)))
          .addField(Schema.Field.of("commitDate", Schema.FieldType.DATETIME))
          .addField(Schema.Field.nullable("message", Schema.FieldType.STRING))
          .addInt64Field("linesAdded")
          // TODO(pabloem): This field should be INT32
          .addInt64Field("linesRemoved")
          .addBooleanField("merged")
          .addByteArrayField("sha1")
          // A decimal field that means nothing but that we include for good measure : )
          .addDecimalField("decimalForGoodMeasure")
          .build();

  public static final Schema NESTED_TABLE_SCHEMA =
      Schema.builder()
          .addFields(SIMPLE_TABLE_SCHEMA.getFields())
          .addField(
              Schema.Field.nullable(
                  "author",
                  Schema.FieldType.row(
                      Schema.builder()
                          .addField(Schema.Field.of("name", Schema.FieldType.STRING))
                          .addField(Schema.Field.of("email", Schema.FieldType.STRING))
                          .build())))
          .build();

  public static String randomString(Integer length, Random randomSeed) {
    return randomSeed
        .ints(length, 48, 122)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  private static <T> T generateOrNull(
      SerializableFunction<Void, T> generator, double nullProbability, Random randomSeed) {
    if (randomSeed.nextDouble() > nullProbability) {
      return generator.apply(null);
    } else {
      return null;
    }
  }

  public static Row randomRowForSchema(
      Schema inputSchema, double nullProbability, Random randomSeed) {
    Row.FieldValueBuilder rowBuilder =
        Row.withSchema(inputSchema).withFieldValue(inputSchema.getField(0).getName(), "any");
    //    Row.FieldValueBuilder fieldValueBuilder = null;
    for (Schema.Field f : inputSchema.getFields()) {
      switch (f.getType().getTypeName()) {
        case STRING:
          String str =
              generateOrNull(
                  ignored -> randomString(50, randomSeed),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (str == null) continue;
          rowBuilder.withFieldValue(f.getName(), str);
          break;
        case DECIMAL:
          BigDecimal bigDecimal =
              generateOrNull(
                  ignored -> new BigDecimal(String.valueOf(randomSeed.nextLong())),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (bigDecimal == null) continue;
          rowBuilder.withFieldValue(f.getName(), bigDecimal);
          break;
        case INT32:
        case INT16:
          Integer theInt =
              generateOrNull(
                  ignored -> randomSeed.nextInt(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theInt == null) continue;
          rowBuilder.withFieldValue(f.getName(), theInt);
          break;
        case INT64:
          Long theLong =
              generateOrNull(
                  ignored -> randomSeed.nextLong(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theLong == null) continue;
          rowBuilder.withFieldValue(f.getName(), theLong);
          break;
        case BYTES:
          // Bytes always non-null
          byte[] theBytes = new byte[50];
          randomSeed.nextBytes(theBytes);
          rowBuilder.withFieldValue(f.getName(), theBytes);
          break;
        case FLOAT:
          Float theFloat =
              generateOrNull(
                  ignored -> randomSeed.nextFloat(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theFloat == null) continue;
          rowBuilder.withFieldValue(f.getName(), theFloat);
          break;
        case DOUBLE:
          Double theDouble =
              generateOrNull(
                  ignored -> randomSeed.nextDouble(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theDouble == null) continue;
          rowBuilder.withFieldValue(f.getName(), theDouble);
          break;
        case ROW:
          rowBuilder.withFieldValue(
              f.getName(), randomRowForSchema(f.getType().getRowSchema(), 0, randomSeed));
          break;
        case BOOLEAN:
          Boolean theBool =
              generateOrNull(
                  ignored -> randomSeed.nextBoolean(),
                  f.getType().getNullable() ? nullProbability : 0,
                  randomSeed);
          if (theBool == null) continue;
          rowBuilder.withFieldValue(f.getName(), theBool);
          break;
        case DATETIME:
          rowBuilder.withFieldValue(f.getName(), new DateTime(randomSeed.nextLong()));
          break;
        case ARRAY:
        case ITERABLE:
          rowBuilder.withFieldValue(
              f.getName(),
              IntStream.range(0, 10)
                  .mapToObj(
                      inty ->
                          randomRowForSchema(
                              Schema.builder()
                                  .addField("main", f.getType().getCollectionElementType())
                                  .build(),
                              0,
                              randomSeed))
                  .map(row -> row.getValue("main"))
                  .collect(Collectors.toList()));
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unable to generate field with type %s", f.getType()));
      }
    }
    return rowBuilder.build();
  }

  @AutoService(SchemaTransformProvider.class)
  public static class GenerateDataSchemaTransformProvider
      extends TypedSchemaTransformProvider<
          GenerateDataSchemaTransformProvider.GenerateDataSchemaTransformConfiguration> {

    @Override
    public Class<GenerateDataSchemaTransformConfiguration> configurationClass() {
      return GenerateDataSchemaTransformConfiguration.class;
    }

    @Override
    public SchemaTransform from(GenerateDataSchemaTransformConfiguration configuration) {
      byte[] schemaBytes = Base64.getDecoder().decode(configuration.getSchemaBase64());
      Schema dataSchema = (Schema)SerializationUtils.deserialize(schemaBytes);

      return new SchemaTransform() {
        @Override
        public @UnknownKeyFor @NonNull @Initialized PTransform<
                @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
                @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
            buildTransform() {
          return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
            @Override
            public PCollectionRowTuple expand(PCollectionRowTuple input) {
              return PCollectionRowTuple.of(
                  "output",
                  inputData(
                      input.getPipeline(),
                      configuration.getNumRows(),
                      configuration.getRuntimeSeconds(),
                      dataSchema));
            }
          };
        }
      };
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized String identifier() {
      return "syndeo_test:schematransform:com.google.cloud:generate_data:v1";
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
        inputCollectionNames() {
      return List.of();
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
        outputCollectionNames() {
      return List.of("output");
    }

    @DefaultSchema(AutoValueSchema.class)
    @AutoValue
    public abstract static class GenerateDataSchemaTransformConfiguration {
      public abstract Long getNumRows();

      public abstract Long getRuntimeSeconds();

      public abstract String getSchemaBase64();

      public static GenerateDataSchemaTransformConfiguration create(
          Long numRows, Long runtimeSeconds, String getSchemaBase64) {
        return new AutoValue_SyndeoLoadTestUtils_GenerateDataSchemaTransformProvider_GenerateDataSchemaTransformConfiguration(
            numRows, runtimeSeconds, getSchemaBase64);
      }
    }
  }
}
