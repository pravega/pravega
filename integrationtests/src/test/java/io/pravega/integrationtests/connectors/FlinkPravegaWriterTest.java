/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.integrationtests.connectors;

import io.pravega.integrationtests.utils.SetupUtils;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.testcommon.AssertExtensions;
import com.google.common.base.Preconditions;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Automated tests for {@link FlinkPravegaWriter}.
 * Tests the atleast once guarantee provided by the pravega writer.
 */
@Slf4j
public class FlinkPravegaWriterTest {

    // Number of events to generate for each of the tests.
    private static final int EVENT_COUNT_PER_SOURCE = 20;

    // Ensure each test completes within 30 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    // Setup utility.
    private SetupUtils setupUtils = new SetupUtils();

    @Before
    public void setup() throws Exception {
        this.setupUtils.startAllServices();
    }

    @After
    public void tearDown() throws Exception {
        this.setupUtils.stopAllServices();
    }

    @Test
    public void testWriter() throws Exception {
        runTest(1, false, "TestStream1");
        runTest(4, false, "TestStream2");
        runTest(1, true, "TestStream3");
        runTest(4, true, "TestStream4");

        log.info("All tests successful");
    }

    /**
     * Read the test data from the stream and verify that all events are present.
     *
     * @param streamName            The test stream name containing the data to be verified.
     * @param jobParallelism        The number of subtasks in the flink job, corresponding to the number of event sources.
     * @param eventCountPerSource   The number of events per source/parallelism.
     *
     * @throws Exception on any errors.
     */
    private void consumeAndVerify(final String streamName, final int jobParallelism, final int eventCountPerSource)
            throws Exception {
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(jobParallelism > 0);
        Preconditions.checkArgument(eventCountPerSource > 0);

        // TODO: Remove the end marker workaround once the following issue is fixed:
        // https://github.com/emccode/pravega/issues/408
        final int streamEndMarker = 99999;

        // Write the end marker.
        @Cleanup
        EventStreamWriter<Integer> eventWriter = this.setupUtils.getIntegerWriter(streamName);
        eventWriter.writeEvent("fixedkey", streamEndMarker);
        eventWriter.flush();

        // Read all data from the stream.
        @Cleanup
        EventStreamReader<Integer> consumer = this.setupUtils.getIntegerReader(streamName);
        List<Integer> readElements = new ArrayList<>();
        while (true) {
            Integer event = consumer.readNextEvent(1000).getEvent();
            if (event == null || event == streamEndMarker) {
                log.info("Reached end of stream: " + streamName);
                break;
            }
            readElements.add(event);
            log.debug("Stream: " + streamName + ". Read event: " + event);
        }

        // Now verify that all expected events are present in the stream. Having extra elements are fine since we are
        // testing the atleast once writer.
        Collections.sort(readElements);
        int expectedEventValue = 0;
        for (int i = 0; i < readElements.size();) {
            if (readElements.get(i) != expectedEventValue) {
                throw new IllegalStateException("Element: " + expectedEventValue + " missing in the stream");
            }

            int countElem = 0;
            while (i < readElements.size() && readElements.get(i) == expectedEventValue) {
                countElem++;
                i++;
            }
            AssertExtensions.assertGreaterThanOrEqual("Repeated events", jobParallelism, countElem);
            expectedEventValue++;
        }
        Assert.assertEquals(expectedEventValue, eventCountPerSource);
    }

    /**
     * Execute a single test.
     *
     * @param jobParallelism    The number of subtasks in the flink job, corresponding to the number of event sources.
     * @param withFailure       Simulate a job failure to test support for snapshot/recover.
     * @param streamName        The test stream name to use.
     *
     * @throws Exception on any errors.
     */
    private void runTest(final int jobParallelism, final boolean withFailure, final String streamName)
            throws Exception {
        Preconditions.checkArgument(jobParallelism > 0);
        Preconditions.checkNotNull(streamName);

        this.setupUtils.createTestStream(streamName, 1);

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment().
                setParallelism(jobParallelism);
        execEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        DataStreamSource<Integer> dataStream = execEnv.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
                .addSource(new IntegerGeneratingSource(withFailure, EVENT_COUNT_PER_SOURCE));

        FlinkPravegaWriter<Integer> pravegaSink = new FlinkPravegaWriter<>(
                this.setupUtils.getControllerUri(),
                this.setupUtils.getScope(),
                streamName,
                element -> {
                    ByteBuffer result = ByteBuffer.allocate(4).putInt(element);
                    result.rewind();
                    return result.array();
                },
                event -> "fixedkey");
        pravegaSink.setPravegaWriterMode(PravegaWriterMode.ATLEAST_ONCE);

        dataStream.addSink(pravegaSink);
        execEnv.execute();
        consumeAndVerify(streamName, jobParallelism, EVENT_COUNT_PER_SOURCE);
    }
}
