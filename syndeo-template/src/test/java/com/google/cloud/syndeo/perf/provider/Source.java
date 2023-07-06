package com.google.cloud.syndeo.perf.provider;

import java.util.Map;

// a source must be a sink, because the test uses Syndeo to seed the source with data
public interface Source extends Sink {
    /**
     * @return The Syndeo URN of the source
     * e.g. "beam:schematransform:org.apache.beam:bigquery_storage_read:v1" for BigQuery
     *
     * This is meant to be an opaque identifier and should not be parsed.
     */
    String sourceUrn();

    /**
     * @return The Syndeo configurationParameters of the source.
     *
     * What specific parameters go in here depends on which source you're using, but generally speaking,
     * settings for which table/topic you're connecting to or the URL of the service you're connecting to
     * (if applicable) go in here.
     */
    Map<String, Object> sourceConfiguration();
}
