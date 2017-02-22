/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.connectors;

import com.emc.pravega.connectors.flink.FlinkPravegaWriter;
import com.emc.pravega.connectors.flink.PravegaWriterMode;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.google.common.base.Preconditions;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Automated tests for {@link FlinkPravegaWriter}.
 * Tests the atleast once guarantee provided by the pravega writer.
 */
@Slf4j
public class FlinkPravegaWriterTest {
    private static final String SCOPE = "Scope";
    private static final int EVENT_COUNT_PER_SOURCE = 20;

    // Setup and execute all the tests.
    public static void main(String[] args) throws Exception {
        try {
            SetupUtils.startPravegaServices();

            FlinkPravegaWriterTest test = new FlinkPravegaWriterTest();
            test.runTest(1, false, "TestStream1");
            test.runTest(4, false, "TestStream2");
            test.runTest(1, true, "TestStream3");
            test.runTest(4, true, "TestStream4");

            log.info("All tests successful");
        } catch (Exception e) {
            log.error("Tests failed with exception: ", e);
        }

        System.exit(0);
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
        EventStreamWriter<Integer> eventWriter = SetupUtils.getIntegerWriter(SCOPE, streamName);
        eventWriter.writeEvent("fixedkey", streamEndMarker);
        eventWriter.flush();

        // Read all data from the stream.
        @Cleanup
        EventStreamReader<Integer> consumer = SetupUtils.getIntegerReader(SCOPE, streamName);
        List<Integer> readElements = new ArrayList<>();
        while (true) {
            Integer event = consumer.readNextEvent(1).getEvent();
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
            Preconditions.checkState(countElem >= jobParallelism, "Element: " + expectedEventValue +
                    " count less than expected in the stream. Expected count: " + jobParallelism + ". Found: " +
                    countElem);
            expectedEventValue++;
        }
        Preconditions.checkState(expectedEventValue == eventCountPerSource, "Event:" + expectedEventValue +
                " not found in the stream");
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

        SetupUtils.createTestStream(SCOPE, streamName, 1);

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment().
                setParallelism(jobParallelism);
        execEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        DataStreamSource<Integer> dataStream = execEnv.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE)
                .addSource(new IntegerGeneratingSource(withFailure, EVENT_COUNT_PER_SOURCE));

        FlinkPravegaWriter<Integer> pravegaSink = new FlinkPravegaWriter<>(
                SetupUtils.CONTROLLER_URI,
                SCOPE,
                streamName,
                element -> String.valueOf(element).getBytes(),
                event -> "fixedkey");
        pravegaSink.setPravegaWriterMode(PravegaWriterMode.ATLEAST_ONCE);

        dataStream.addSink(pravegaSink);
        execEnv.execute();
        consumeAndVerify(streamName, jobParallelism, EVENT_COUNT_PER_SOURCE);
    }
}
