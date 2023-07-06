package com.google.cloud.syndeo.perf.provider;

public interface Provider {
    /**
     * Sets up the Provider for use.
     *
     * This is where you stand up tables/topics or any other resources necessary for the Provider to read/write data.
     * Do not seed the Provider with data at this step.
     */
    void setup();

    /**
     * Cleans up all resources created by the Provider.
     */
    void cleanup();

    /**
     * @return The total amount of rows that <b>have ever</b> gone through the Provider.
     * e.g. for Pub/Sub, this is not total amount of rows currently pending, but the total amount of rows that have ever entered the topic.
     */
    long rowCount();
}
