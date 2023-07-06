package com.google.cloud.syndeo.perf.provider;

import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.syndeo.transforms.pubsub.SyndeoPubsubReadSchemaTransformProvider;
import com.google.cloud.syndeo.transforms.pubsub.SyndeoPubsubWriteSchemaTransformProvider;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.monitoring.v3.*;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.*;
import com.google.pubsub.v1.ProjectName;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.cloud.pubsub.v1.Subscriber;

public class PubsubProvider implements Source, Sink {
    private final PubsubResourceManager rm;
    private final ProviderContext ctx;
    private TopicName topicName;
    private SubscriptionName subName;
    private SubscriptionName dummySubForSize;
    private AtomicLong sizeSeen;
    private boolean setupCalled;
    private Subscriber sizeSubscriber;


    public PubsubProvider(ProviderContext ctx) {
        try {
            rm = PubsubResourceManager.builder(ctx.getTestId(), ctx.getProject())
                    .credentialsProvider(ctx.getCredsProvider())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.ctx = ctx;
        sizeSeen = new AtomicLong(0);
    }

    private void assertSetupCalled() {
        if (!setupCalled) {
            throw new IllegalArgumentException("Call setup() first.");
        }
    }

    @Override
    public void setup() {
        if (setupCalled) return;
        topicName = rm.createTopic("syndeo-lt-" + ctx.getTestId());
        subName = rm.createSubscription(topicName, "sub");

        /*
        dummySubForSize = rm.createSubscription(topicName, "source-size-calculator");

        sizeSubscriber = Subscriber.newBuilder(dummySubForSize.toString(), new MessageReceiver() {
            @Override
            public void receiveMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
                ackReplyConsumer.ack();
                sizeSeen.incrementAndGet();
            }
        }).build();

        try {
            sizeSubscriber.startAsync().awaitRunning(60, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        */

        setupCalled = true;
    }

    @Override
    public String sourceUrn() {
        return "syndeo:schematransform:com.google.cloud:pubsub_read:v1";
    }

    @Override
    public Map<String, Object> sourceConfiguration() {
        assertSetupCalled();
        return ProviderUtils.configToMap(
                SyndeoPubsubReadSchemaTransformProvider
                        .SyndeoPubsubReadSchemaTransformConfiguration.builder()
                        .setFormat("AVRO")
                        .setSchema(
                                AvroUtils.toAvroSchema(ctx.beamSchema())
                                        .toString())
                        .setSubscription(subName.toString())
                        .build());
    }

    @Override
    public long rowCount() {
        assertSetupCalled();

        try (MetricServiceClient msc = MetricServiceClient.create()) {
            ListTimeSeriesRequest req = ListTimeSeriesRequest
                    .newBuilder()
                    .setName(ProjectName.of(ctx.getProject()).toString())
                    .setFilter(String.format("metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.labels.subscription_id = \"%s\"", subName.getSubscription()))
                    // .setFilter("metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\"")
                    .setInterval(TimeInterval.newBuilder()
                            .setStartTime(Timestamps.fromMillis(System.currentTimeMillis() - 10 * 60 * 1000))
                            .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
                            .build())
                    .build();
            MetricServiceClient.ListTimeSeriesPagedResponse resp = msc.listTimeSeries(req);
            TimeSeries ts = resp.iterateAll().iterator().next();
            return ts.getPoints(ts.getPointsCount() - 1).getValue().getInt64Value();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        catch (NoSuchElementException e) {
            // there is no time series data for the subscription yet
            return 0;
        }
    }

    @Override
    public String sinkUrn() {
        return "syndeo:schematransform:com.google.cloud:pubsub_write:v1";
    }

    @Override
    public Map<String, Object> sinkConfiguration() {
        assertSetupCalled();
        return ProviderUtils.configToMap(
                SyndeoPubsubWriteSchemaTransformProvider.SyndeoPubsubWriteConfiguration
                        .create("AVRO", topicName.toString()));
    }

    @Override
    public void cleanup() {
        /*
        try {
            sizeSubscriber.stopAsync().awaitTerminated(60, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        */
        rm.cleanupAll();
    }
}
