package com.google.cloud.syndeo.perf.provider;

import com.google.cloud.bigquery.Field;

import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryDirectReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryStorageWriteApiSchemaTransformProvider;
import com.google.cloud.bigquery.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BigQueryProvider implements Source, Sink {
    private final BigQueryResourceManager rm;
    private final ProviderContext ctx;
    private final String tableName;
    private boolean setupCalled;

    public BigQueryProvider(ProviderContext ctx) {
        this.rm = BigQueryResourceManager.builder(ctx.getTestId(), ctx.getProject()).build();
        this.ctx = ctx;
        this.tableName = "syndeo_lt_table_" + ctx.getTestId();
    }

    private void assertSetupCalled() {
        if (!setupCalled) {
            throw new IllegalStateException("Call setup() first.");
        }
    }

    @Override
    public String sourceUrn() {
        return "beam:schematransform:org.apache.beam:bigquery_storage_read:v1";
    }

    @Override
    public void setup() {
        if (setupCalled) return;
        rm.createDataset(ctx.getRegion());
        rm.createTable(tableName, beamSchemaToBq(ctx.beamSchema()));
        setupCalled = true;
    }

    @Override
    public Map<String, Object> sourceConfiguration() {
        assertSetupCalled();
        return ProviderUtils.configToMap(
                BigQueryDirectReadSchemaTransformProvider
                        .BigQueryDirectReadSchemaTransformConfiguration.builder()
                        .setTableSpec(String.format("%s:%s.%s", ctx.getProject(), rm.getDatasetId(), tableName))
                        .build());
    }

    @Override
    public long rowCount() {
        assertSetupCalled();
        return rm.getRowCount(tableName);
    }

    @Override
    public String sinkUrn() {
        return "beam:schematransform:org.apache.beam:bigquery_storage_write:v1";
    }

    @Override
    public Map<String, Object> sinkConfiguration() {
        assertSetupCalled();
        return ProviderUtils.configToMap(
                BigQueryStorageWriteApiSchemaTransformProvider
                        .BigQueryStorageWriteApiSchemaTransformConfiguration.builder()
                        .setTable(String.format("%s:%s.%s", ctx.getProject(), rm.getDatasetId(), tableName))
                        .setAutoSharding(true)
                        .build());
    }

    @Override
    public void cleanup() {
        rm.cleanupAll();
    }

    private static Field beamFieldToBq(org.apache.beam.sdk.schemas.Schema.Field beamField) {
        StandardSQLTypeName tn;
        List<Field> subFields = new ArrayList<>();

        switch (beamField.getType().getTypeName()) {
            case STRING:
                tn = StandardSQLTypeName.STRING;
                break;
            case BYTE:
            case INT16:
            case INT32:
            case INT64:
                tn = StandardSQLTypeName.INT64;
                break;
            case BOOLEAN:
                tn = StandardSQLTypeName.BOOL;
                break;
            case FLOAT:
            case DOUBLE:
                // beam does not support BIGNUMERIC, so DECIMAL also maps to FLOAT64
            case DECIMAL:
                tn = StandardSQLTypeName.FLOAT64;
                break;
            case ARRAY:
            case ITERABLE:
                tn = StandardSQLTypeName.ARRAY;
                break;
            case DATETIME:
                tn = StandardSQLTypeName.DATETIME;
                break;
            case BYTES:
                tn = StandardSQLTypeName.BYTES;
                break;
            case ROW:
            case MAP:
                tn = StandardSQLTypeName.STRUCT;
                subFields = beamField.getType().getRowSchema().getFields()
                        .stream()
                        .map(BigQueryProvider::beamFieldToBq)
                        .collect(Collectors.toList());
                break;
            default:
                throw new IllegalArgumentException("Cannot convert Beam type " + beamField.getType().getTypeName().toString() + " to an equivalent BigQuery type.");
        }

        if (tn == StandardSQLTypeName.ARRAY) {
            throw new IllegalArgumentException("You cannot have a field of type ARRAY, because the BigQuery SDK's Field type doesn't support it. Of course, there is no documentation saying so. If you try to pass in ARRAY, you'll get a confusing NullPointerException that will waste a good amount of your time. More specifically, it tries to convert the StandardSQLTypeName to a LegacySQLTypeName internally, but there is no LegacySQLTypeName corresponding to ARRAY. Rather than throwing an exception with a helpful error message, the SDK instead decides to return null, leading to a confusing error message down the line when it tries to instantiate a Field of type null (yes, the LegacySQLTypeName enum is implemented as a class and not an enum, because why wouldn't you want your integer to be a reference type?).");
        }

        return Field.of(beamField.getName(), tn, subFields.toArray(new Field[0]));
    }

    private static Schema beamSchemaToBq(org.apache.beam.sdk.schemas.Schema beamSchema) {
        return Schema.of(beamSchema.getFields()
                .stream()
                .map(BigQueryProvider::beamFieldToBq)
                .collect(Collectors.toList()));
    }
}
