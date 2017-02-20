/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.connectors;

import com.emc.pravega.connectors.flink.FlinkPravegaReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.apache.flink.contrib.streaming.DataStreamUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * Automated tests for {@link FlinkPravegaReader}.
 */
@Slf4j
public class FlinkPravegaReaderTest {
    private static final String SCOPE = "Scope";

    // Number of events to produce into the test stream.
    private static final int NUM_STREAM_ELEMENTS = 10000;

    // TODO: Remove the end marker workaround once the following issue is fixed:
    // https://github.com/emccode/pravega/issues/408
    private final static int STREAM_END_MARKER = 99999;

    // Setup and execute all the tests.
    public static void main(String[] args) throws Exception {
        try {
            SetupUtils.startPravegaServices();

            runTest("teststream1", 1);

            log.info("All tests successful");
        } catch (Exception e) {
            log.error("Tests failed with exception: ", e);
        }

        System.exit(0);
    }

    /**
     * Execute a single test.
     *
     * @param streamName        The test stream name to use.
     * @param jobParallelism    The number of subtasks in the flink job, corresponding to the number of event sources.
     *
     * @throws Exception on any errors.
     */
    private static void runTest(final String streamName, final int jobParallelism) throws Exception {
        prepareStream(streamName, jobParallelism);

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment().
                setParallelism(jobParallelism);
        execEnv.setRestartStrategy(RestartStrategies.noRestart());

        FlinkPravegaReader<Integer> pravegaSource = new FlinkPravegaReader<>(
                SetupUtils.CONTROLLER_URI,
                 SCOPE,
                 Collections.singletonList(streamName),
                 0,
                 new AbstractDeserializationSchema<Integer>() {
                     @Override
                     public Integer deserialize(byte[] message) throws IOException {
                         return Integer.valueOf(new String(message));
                     }

                     @Override
                     public boolean isEndOfStream(Integer nextElement) {
                         if (nextElement == STREAM_END_MARKER) {
                             return true;
                         }
                         return false;
                     }
                 });

        DataStreamSource<Integer> dataStream = execEnv
                .addSource(pravegaSource)
                .setParallelism(jobParallelism);

        final Iterator<Integer> collect = DataStreamUtils.collect(dataStream);
        execEnv.execute();

        final ArrayList<Integer> integers = Lists.newArrayList(collect);
        Collections.sort(integers);

        Preconditions.checkState(integers.size() == NUM_STREAM_ELEMENTS,
                                 "integers.size() != NUM_STREAM_ELEMENTS", integers.size(), NUM_STREAM_ELEMENTS);
        Preconditions.checkState(integers.get(0) == 0);
        Preconditions.checkState(integers.get(NUM_STREAM_ELEMENTS - 1) == (NUM_STREAM_ELEMENTS - 1));
    }

    private static void prepareStream(final String streamName, final int parallelism) throws Exception {
        SetupUtils.createTestStream(SCOPE, streamName, parallelism);

        @Cleanup
        EventStreamWriter<Integer> eventWriter = SetupUtils.getIntegerWriter(SCOPE, streamName);
        for (int i = 0; i < NUM_STREAM_ELEMENTS; i++) {
            eventWriter.writeEvent(String.valueOf(i), i);
        }
        eventWriter.flush();

        // Write same number of end markers and routing keys to guarantee that all segments have this marker.
        for (int i = 0; i < NUM_STREAM_ELEMENTS; i++) {
            eventWriter.writeEvent(String.valueOf(i), STREAM_END_MARKER);
        }
        eventWriter.flush();
    }
}
