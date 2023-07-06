package com.google.cloud.syndeo.perf.provider;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.Schema;

@AutoValue
public abstract class ProviderContext {
    public abstract CredentialsProvider getCredsProvider();
    public abstract String getTestId();
    public abstract String getRegion();
    public abstract String getProject();
    public abstract Schema beamSchema();

    public static ProviderContext create(CredentialsProvider creds, String testId, String region, String project, Schema beamSchema) {
        return new AutoValue_ProviderContext(creds, testId, region, project, beamSchema);
    }
}
