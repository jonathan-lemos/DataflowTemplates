package com.google.cloud.syndeo.perf.provider;

import java.util.Map;

public interface Sink extends Provider {
    /**
     * @return The Syndeo URN of the sink
     * e.g. "beam:schematransform:org.apache.beam:bigquery_storage_write:v1" for BigQuery
     *
     * This is meant to be an opaque identifier and should not be parsed.
     */
    String sinkUrn();

    /**
     * @return The Syndeo configurationParameters of the sink.
     *
     * What specific parameters go in here depends on which sink you're using, but generally speaking,
     * settings for which table/topic you're connecting to or the URL of the service you're connecting to
     * (if applicable) go in here.
     */
    Map<String, Object> sinkConfiguration();
}
