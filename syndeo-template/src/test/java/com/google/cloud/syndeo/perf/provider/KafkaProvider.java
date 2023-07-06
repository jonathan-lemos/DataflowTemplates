package com.google.cloud.syndeo.perf.provider;

import com.google.cloud.teleport.it.kafka.KafkaResourceManager;
import org.apache.beam.sdk.io.kafka.KafkaWriteSchemaTransformProvider;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

// Source is not implemented because kafka read is currently broken
// reading from it produces 0 elements for an unknown reason
public class KafkaProvider implements Sink {
    private final KafkaResourceManager rm;
    private final ProviderContext ctx;
    private String topicName;
    private boolean setupCalled;
    private KafkaConsumer<Bytes, Bytes> dummyConsumerForSize;
    private long elementsSeen;

    public KafkaProvider(ProviderContext ctx) {
        try {
            rm = KafkaResourceManager.builder(ctx.getTestId()).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.ctx = ctx;
    }

    private void assertSetupCalled() {
        if (!setupCalled) {
            throw new IllegalStateException("Call setup() first.");
        }
    }

    @Override
    public String sinkUrn() {
        return "beam:schematransform:org.apache.beam:kafka_write:v1";
    }

    @Override
    public void setup() {
        if (setupCalled) return;
        topicName = rm.createTopic(ctx.getTestId(), 1);
        setupCalled = true;
        dummyConsumerForSize = rm.buildConsumer(new BytesDeserializer(), new BytesDeserializer());
        dummyConsumerForSize.subscribe(List.of(topicName));
    }

    @Override
    public void cleanup() {
        rm.cleanupAll();
    }

    @Override
    public Map<String, Object> sinkConfiguration() {
        assertSetupCalled();
        return ProviderUtils.configToMap(
                KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransformConfiguration
                        .builder()
                        .setBootstrapServers(rm.getBootstrapServers())
                        .setTopic(topicName)
                        .setFormat("AVRO")
                        .build());
    }

    @Override
    public long rowCount() {
        assertSetupCalled();
        ConsumerRecords<Bytes, Bytes> records;
        while ((records = dummyConsumerForSize.poll(Duration.ofSeconds(3))).count() > 0) {
            elementsSeen += records.count();
        }
        return elementsSeen;
    }
}
