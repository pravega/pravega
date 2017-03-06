/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests.connectors;

import com.emc.pravega.connectors.flink.FlinkPravegaReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.google.common.collect.Lists;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Automated tests for {@link FlinkPravegaReader}.
 */
@Slf4j
public class FlinkPravegaReaderTest {

    // Number of events to produce into the test stream.
    private static final int NUM_STREAM_ELEMENTS = 1000;

    // TODO: Remove the end marker workaround once the following issue is fixed:
    // https://github.com/emccode/pravega/issues/408
    private final static int STREAM_END_MARKER = 99999;

    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    //Ensure each test completes within 30 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    @Before
    public void setup() throws Exception {
        SETUP_UTILS.startPravegaServices();
    }

    @Test
    public void testReader() throws Exception {
        runTest("teststream1", 1);
        log.info("All tests successful");
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
                SETUP_UTILS.getControllerUri(),
                SETUP_UTILS.getScope(),
                Collections.singleton(streamName),
                0,
                new AbstractDeserializationSchema<Integer>() {
                    @Override
                    public Integer deserialize(byte[] message) throws IOException {
                        return ByteBuffer.wrap(message).getInt();
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

        Assert.assertEquals(NUM_STREAM_ELEMENTS, integers.size());
        Assert.assertEquals(0, integers.get(0).longValue());
        Assert.assertEquals(NUM_STREAM_ELEMENTS - 1, integers.get(NUM_STREAM_ELEMENTS - 1).longValue());

    }

    private static void prepareStream(final String streamName, final int parallelism) throws Exception {
        SETUP_UTILS.createTestStream(streamName, parallelism);

        @Cleanup
        EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);
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
