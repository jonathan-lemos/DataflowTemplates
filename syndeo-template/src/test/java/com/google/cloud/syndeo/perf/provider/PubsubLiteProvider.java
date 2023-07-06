package com.google.cloud.syndeo.perf.provider;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;
import com.google.cloud.teleport.it.gcp.pubsublite.PubsubliteResourceManager;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteWriteSchemaTransformProvider;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class PubsubLiteProvider implements Source, Sink {
    private final PubsubliteResourceManager rm;
    private final ProviderContext ctx;
    private ReservationPath reservationPath;
    private TopicName topicName;
    private SubscriptionName subName;
    private Subscriber dummySubscriberForSize;
    private AtomicLong sizeSeen;
    private boolean setupCalled;

    public PubsubLiteProvider(ProviderContext ctx) {
        rm = new PubsubliteResourceManager();
        this.ctx = ctx;
        sizeSeen = new AtomicLong();
    }

    private void assertSetupCalled() {
        if (!setupCalled) {
            throw new IllegalArgumentException("Call setup() first.");
        }
    }

    private Subscriber makeDummySubscriber() {
        SubscriptionName dummySubNameForSize = rm.createSubscription(reservationPath, topicName, "syndeo-lt-size-calc-" + ctx.getTestId());

        SubscriptionPath path = SubscriptionPath.newBuilder()
                .setLocation(CloudRegion.of(ctx.getRegion()))
                .setProject(ProjectId.of(ctx.getProject()))
                .setName(SubscriptionName.of("syndeo-lt-size-calc-" + ctx.getTestId()))
                .build();

        MessageReceiver receiver =
                (PubsubMessage msg, AckReplyConsumer consumer) -> {
                    sizeSeen.incrementAndGet();
                    consumer.ack();
                };

        FlowControlSettings flowControlSettings =
                FlowControlSettings.builder()
                        .setBytesOutstanding(100 * 1024 * 1024L)
                        .setMessagesOutstanding(1000L)
                        .build();

        SubscriberSettings settings =
                SubscriberSettings.newBuilder()
                        .setCredentialsProvider(ctx.getCredsProvider())
                        .setSubscriptionPath(path)
                        .setReceiver(receiver)
                        .setPerPartitionFlowControlSettings(flowControlSettings)
                        .build();

        Subscriber s = Subscriber.create(settings);
        try {
            s.startAsync().awaitRunning(120, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        return s;
    }

    @Override
    public void setup() {
        if (setupCalled) return;

        reservationPath = rm.createReservation("syndeo-lt-" + ctx.getTestId(), ctx.getRegion(), ctx.getProject(), 100L);
        topicName = rm.createTopic("syndeo-lt-" + ctx.getTestId(), reservationPath);
        subName = rm.createSubscription(reservationPath, topicName, "syndeo-lt-" + ctx.getTestId());
        dummySubscriberForSize = makeDummySubscriber();

        setupCalled = true;
    }

    @Override
    public void cleanup() {
        try {
            dummySubscriberForSize.stopAsync().awaitTerminated(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            // we still want to try to clean up everything else even if the above subscriber cleanup fails
            e.printStackTrace();
        }
        rm.cleanupAll();
    }

    @Override
    public long rowCount() {
        assertSetupCalled();
        return sizeSeen.get();
    }

    @Override
    public String sinkUrn() {
        return "beam:schematransform:org.apache.beam:pubsublite_write:v1";
    }

    @Override
    public Map<String, Object> sinkConfiguration() {
        assertSetupCalled();
        return ProviderUtils.configToMap(
                PubsubLiteWriteSchemaTransformProvider
                        .PubsubLiteWriteSchemaTransformConfiguration.builder()
                        .setFormat("AVRO")
                        .setLocation(reservationPath.location().value())
                        .setProject(reservationPath.project().toString())
                        .setTopicName("syndeo-lt-" + ctx.getTestId())
                        .build());
    }

    @Override
    public String sourceUrn() {
        return "beam:schematransform:org.apache.beam:pubsublite_read:v1";
    }

    @Override
    public Map<String, Object> sourceConfiguration() {
        assertSetupCalled();
        return ProviderUtils.configToMap(
                PubsubLiteReadSchemaTransformProvider
                        .PubsubLiteReadSchemaTransformConfiguration.builder()
                        .setFormat("AVRO")
                        .setSchema(
                                AvroUtils.toAvroSchema(ctx.beamSchema())
                                        .toString())
                        .setLocation(reservationPath.location().value())
                        .setProject(reservationPath.project().toString())
                        .setSubscriptionName("syndeo-lt-" + ctx.getTestId())
                        .build());
    }
}
