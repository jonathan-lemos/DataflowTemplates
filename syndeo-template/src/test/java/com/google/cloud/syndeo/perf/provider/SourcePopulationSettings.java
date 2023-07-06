package com.google.cloud.syndeo.perf.provider;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.Schema;

@AutoValue
public abstract class SourcePopulationSettings {
    public abstract int getNumRowsToGenerate();
    public abstract Schema getSchema();
}
