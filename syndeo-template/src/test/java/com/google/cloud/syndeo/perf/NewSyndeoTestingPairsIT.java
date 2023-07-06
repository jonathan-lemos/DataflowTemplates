package com.google.cloud.syndeo.perf;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auto.value.AutoValue;
import com.google.cloud.syndeo.perf.provider.*;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.gcp.LoadTestBase;
import com.google.cloud.teleport.it.gcp.dataflow.DefaultPipelineLauncher;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.v1.SyndeoV1;
import com.google.cloud.teleport.it.common.utils.PipelineUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Collectors;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.After;
import org.junit.Test;

import static com.google.cloud.syndeo.perf.AnyToAnySyndeoTestLT.*;

@RunWith(Parameterized.class)
public class NewSyndeoTestingPairsIT extends LoadTestBase {
    public static List<Class<? extends Source>> sources() {
        return List.of(
                BigQueryProvider.class,
                PubsubProvider.class

                // P/S Lite is too weak to handle the incredible power of 100 dataflow workers
                // syndeo also does not have any throttling mechanism to slow down when the sink is overloaded
                // PubsubLiteProvider.class
        );
    }

    public static List<Class<? extends Sink>> sinks() {
        return List.of(
                BigQueryProvider.class,
                PubsubProvider.class

                // P/S Lite is too weak to handle the incredible power of 100 dataflow workers
                // syndeo also does not have any throttling mechanism to slow down when the sink is overloaded
                // PubsubLiteProvider.class

                // uncomment when the Kafka RM stands up Kafka on GKE instead of locally
                // this will not work on dataflow, because "localhost" no longer refers to our machine when running on dataflow
                // KafkaProvider.class,
        );
    }

    public static List<Long> rowSizes() {
        return List.of(
               100L,
               500_000_000L
        );
    }

    private static final String PROJECT = TestProperties.project();
    private static final String REGION = TestProperties.region();
    private static final String ZONE_SUFFIX = "-c";
    private static final Logger LOG = LoggerFactory.getLogger(NewSyndeoTestingPairsIT.class);

    // we have tuples at home
    @AutoValue
    public abstract static class TestCase {
        public abstract Class<? extends Source> getSource();

        public abstract Class<? extends Sink> getSink();

        public abstract String getTestCaseId();
        public abstract long getNumRows();

        public static TestCase create(Class<? extends Source> source, Class<? extends Sink> sink, String testCaseId, long numRows) {
            return new AutoValue_NewSyndeoTestingPairsIT_TestCase(source, sink, testCaseId, numRows);
        }

        @Override
        public String toString() {
            return String.format("%s -> %s (%s)", getSource().getSimpleName(), getSink().getSimpleName(), getTestCaseId());
        }
    }

    @Override
    protected PipelineLauncher launcher() {
        return DefaultPipelineLauncher.builder().setCredentials(TestProperties.credentials()).build();
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestCase> testCases() {
        // this will test (every source -> first sink) + (first source -> every sink)
        // for a total of (count(sources) + count(sinks) - 1) tests
        // this ensures that every source/sink is tested at least once without having to enumerate N * M combinations

        Class<? extends Source> firstSource = sources().get(0);
        Class<? extends Sink> firstSink = sinks().get(0);
        List<TestCase> ret = new ArrayList<>();

        for (long numRows : rowSizes()) {
            for (Class<? extends Source> s : sources()) {
                ret.add(TestCase.create(s, firstSink, String.format("syndeo_it_source_%s_%d", s.getSimpleName(), numRows), numRows));
            }

            // skip the first element because the previous loop already got the (0, 0) pair
            for (Class<? extends Sink> s : sinks().stream().skip(1).collect(Collectors.toList())) {
                ret.add(TestCase.create(firstSource, s, String.format("syndeo_it_sink_%s_%d", s.getSimpleName(), numRows), numRows));
            }
        }

        return ret;
    }

    private final TestCase tc;
    private Source source;
    private Sink sink;
    private ProviderContext ctx;
    @Rule public TestPipeline syndeoPipeline = TestPipeline.create();
    @Rule public TestPipeline dataGenerator = TestPipeline.create();
    private PipelineResult dataGeneratorResult;
    private PipelineResult syndeoPipelineResult;
    private Random random = new Random();

    public NewSyndeoTestingPairsIT(TestCase tc) {
        this.tc = tc;
    }

    private void instantiateWithTestName() {
        StackTraceElement[] st = Thread.currentThread().getStackTrace();
        String testName = st[2].getMethodName();

        ctx = ProviderContext.create(
                FixedCredentialsProvider.create(TestProperties.credentials()),
                tc.getTestCaseId() + "_" + testName + "_" + random.nextInt(),
                TestProperties.region(),
                TestProperties.project(),
                SyndeoLoadTestUtils.SUPER_SIMPLE_TABLE_SCHEMA);

        source = ProviderUtils.instantiateSource(tc.getSource(), ctx);
        sink = ProviderUtils.instantiateSink(tc.getSink(), ctx);
    }

    @Test
    public void testSyndeoTemplateBasicLocal() throws Exception {
        instantiateWithTestName();

        if (tc.getNumRows() > 10_000) {
            LOG.info("Not running DirectRunner test with {} rows because it will not complete in a reasonable amount of time.", tc.getNumRows());
            return;
        }

        LOG.info("Running test for {}", tc);
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(
                                "--blockOnRun=false",
                                "--runner=DirectRunner",
                                // TODO(pabloem): Avoid passing this value. Instead use a proper auto-defined value.
                                "--numStorageWriteApiStreams=10")
                        .create();
        run(options, options, Duration.ofSeconds(120 + tc.getNumRows()));
    }

    @Test
    public void testSyndeoTemplateDataflow() throws Exception {
        instantiateWithTestName();

        PipelineOptions syndeoOptions =
                PipelineOptionsFactory.fromArgs(
                                "--blockOnRun=false",
                                "--runner=DataflowRunner",
                                "--region=" + REGION,
                                "--project=" + PROJECT,
                                "--experiments=enable_streaming_engine",
                                "--experiments=enable_streaming_auto_sharding=true",
                                "--experiments=streaming_auto_sharding_algorithm=FIXED_THROUGHPUT",
                                "--numStorageWriteApiStreams=10",
                                "--numWorkers=50",
                                "--maxNumWorkers=100")
                        .create();
        PipelineOptions dataGenOptions =
                PipelineOptionsFactory.fromArgs(
                                "--blockOnRun=false",
                                "--runner=DataflowRunner",
                                "--region=" + REGION,
                                "--project=" + PROJECT,
                                "--experiments=enable_streaming_engine",
                                "--experiments=enable_streaming_auto_sharding=true",
                                "--experiments=streaming_auto_sharding_algorithm=FIXED_THROUGHPUT",
                                "--numStorageWriteApiStreams=10",
                                "--numWorkers=50",
                                "--maxNumWorkers=100")
                        .create();
        run(syndeoOptions, dataGenOptions, Duration.ofSeconds(300 + tc.getNumRows()));
    }

    private PipelineResult populateSourceChunk(PipelineOptions dataGenOptions, long numRows) throws IOException {
        long rowsInitial = source.rowCount();

        Pipeline p = Pipeline.create(dataGenOptions);

        String payload = buildPipelinePayload(dataGeneratorConfiguration(numRows), buildJsonConfig(source.sinkUrn(), source.sinkConfiguration()));

        SyndeoV1.PipelineDescription generatorDescription =
                SyndeoTemplate.buildFromJsonPayload(payload);

        LOG.info("Building data generator pipeline from configuration: {}", payload);
        SyndeoTemplate.buildPipeline(p, generatorDescription);

        LOG.info("Starting data generation pipeline");
        PipelineResult generatorResult = p.run(dataGenOptions);

        LOG.info(
                "Waiting for data generation pipeline to finish before starting Syndeo pipeline"
                        + " because the source is a batch source.");
        generatorResult.waitUntilFinish();

        try {
            PipelineUtils.waitUntil(() -> source.rowCount() >= rowsInitial + numRows, 600 * 1000L);
        }
        catch (InterruptedException e) {
            throw new IllegalStateException(String.format("Expected %d rows, but actually have %d rows", rowsInitial + numRows, source.rowCount()));
        }

        if (source.rowCount() < rowsInitial + numRows) {
            throw new IllegalStateException(String.format("Expected %d rows, but actually have %d rows", rowsInitial + numRows, source.rowCount()));
        }

        LOG.info("Up to {} rows", source.rowCount());
        return generatorResult;
    }

    private void populateSource(PipelineOptions dataGenOptions) throws IOException {
        populateSourceChunk(dataGenOptions, tc.getNumRows());

        /*
        long chunkLen = 1 << 19;
        long numChunks = tc.getNumRows() / chunkLen;

        for (long i = 0; i < numChunks; ++i) {
            populateSourceChunk(dataGenOptions, chunkLen);
        }
        if (tc.getNumRows() % chunkLen != 0) {
            populateSourceChunk(dataGenOptions, tc.getNumRows() % chunkLen);
        }
        */
    }

    private PipelineResult runSyndeoPipeline(PipelineOptions syndeoOptions) throws IOException {
        String syndeoPipelinePayload =
                buildPipelinePayload(buildJsonConfig(source.sourceUrn(), source.sourceConfiguration()), buildJsonConfig(sink.sinkUrn(), sink.sinkConfiguration()));

        SyndeoV1.PipelineDescription syndeoPipeDescription =
                SyndeoTemplate.buildFromJsonPayload(syndeoPipelinePayload);

        LOG.info("Building syndeo test pipeline from configuration: {}", syndeoPipelinePayload);
        SyndeoTemplate.buildPipeline(syndeoPipeline, syndeoPipeDescription);

        LOG.info("Starting syndeo test pipeline");
        return syndeoPipeline.run(syndeoOptions);
    }

    private static boolean pipelineIsDead(PipelineResult result) {
        return Set.of(
                        PipelineResult.State.CANCELLED,
                        PipelineResult.State.DONE,
                        PipelineResult.State.STOPPED,
                        PipelineResult.State.FAILED)
                .contains(result.getState());
    }

    private static <T extends Throwable> Duration executionTime(RunnableCanThrow<T> f) throws T {
        long start = System.currentTimeMillis();
        f.run();
        long end = System.currentTimeMillis();
        return Duration.ofMillis(end - start);
    }

    private PipelineLauncher.LaunchInfo getDataflowJobLaunchInfo(DataflowPipelineJob dpj, Timestamp startTime) {
        String sdk = dpj.getDataflowOptions().getUserAgent().split("/")[1];

        return PipelineLauncher.LaunchInfo.builder()
                .setJobId(dpj.getJobId())
                .setProjectId(dpj.getProjectId())
                .setRegion(dpj.getRegion())
                .setState(PipelineLauncher.JobState.DONE)
                .setCreateTime(Timestamps.toString(startTime))
                .setSdk(sdk)
                .setVersion("1.0")
                .setJobType("")
                .setRunner("DataflowRunner")
                .setParameters(ImmutableMap.of())
                .build();
    }

    private Map<String, Double> getDataflowJobMetrics(PipelineLauncher.LaunchInfo info) {
        try {
            return super.getMetrics(info);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void run(
            PipelineOptions syndeoOptions, PipelineOptions dataGenOptions, Duration timeoutMultiplier)
            throws Exception {
        source.setup();
        sink.setup();

        populateSource(dataGenOptions);

        /*
        LOG.info(
                "Waiting up to {} minutes for syndeo test pipeline to finish processing all the input."
                        + " Input size: {} elements",
                4 * timeoutMultiplier.toMinutes(),
                source.rowCount());
         */

        Timestamp startTime = Timestamps.fromMillis(System.currentTimeMillis());

        Duration syndeoRuntime = executionTime(() -> {
            syndeoPipelineResult = runSyndeoPipeline(syndeoOptions);

            boolean completed =
                    PipelineUtils.waitUntil(
                            () -> sink.rowCount() >= tc.getNumRows() || pipelineIsDead(syndeoPipelineResult),
                            4 * timeoutMultiplier.toMillis());

            LOG.info(
                    "Pipeline {} in processing all elements. Cancelling and waiting "
                            + "for cancellation to succeed.",
                    completed ? "succeeded" : "failed");

            syndeoPipelineResult.cancel();

            PipelineUtils.waitUntil(() -> pipelineIsDead(syndeoPipelineResult), 4 * timeoutMultiplier.toMillis());
        });

        LOG.info("Syndeo execution processed {} rows in {} seconds", sink.rowCount(), syndeoRuntime.getSeconds());

        PipelineUtils.waitUntil(() -> sink.rowCount() >= tc.getNumRows(), 600 * 1000L);

        boolean success = sink.rowCount() >= tc.getNumRows();
        if (!success) {
            throw new AssertionError(
                    "Processed a total of "
                            + sink.rowCount()
                            + " elements. Expected "
                            + tc.getNumRows());
        }

        if (syndeoPipelineResult instanceof DataflowPipelineJob) {
            LOG.info("Exporting metrics to BigQuery");
            PipelineLauncher.LaunchInfo li = getDataflowJobLaunchInfo((DataflowPipelineJob) syndeoPipelineResult, startTime);
            Map<String, Double> metrics = getDataflowJobMetrics(li);
            exportMetricsToBigQuery(li, metrics);
        }
    }

    private interface RunnableCanThrow<T extends Throwable> {
        void run() throws T;
    }

    private void swallowExceptions(RunnableCanThrow<Exception> f) {
        try {
            f.run();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void cleanUp() {
        swallowExceptions(source::cleanup);
        swallowExceptions(sink::cleanup);
        if (dataGeneratorResult != null) {
            swallowExceptions(dataGeneratorResult::cancel);
        }
        if (syndeoPipelineResult != null) {
            swallowExceptions(syndeoPipelineResult::cancel);
        }
    }

    Map<String, Object> dataGeneratorConfiguration(long numRows) {
        byte[] schemaBytes = SerializationUtils.serialize(ctx.beamSchema());
        String schemaB64 = Base64.getEncoder().encodeToString(schemaBytes);

        return Map.of(
                "urn",
                "syndeo_test:schematransform:com.google.cloud:generate_data:v1",
                "configurationParameters",
                Map.of("numRows", numRows, "runtimeSeconds", numRows, "schemaBase64", schemaB64));
    }
}
