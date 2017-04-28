/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.connectors;

import io.pravega.connectors.flink.FlinkExactlyOncePravegaReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.test.integration.connectors.utils.FailingMapper;
import io.pravega.test.integration.connectors.utils.IntSequenceExactlyOnceValidator;
import io.pravega.test.integration.connectors.utils.NotifyingMapper;
import io.pravega.test.integration.connectors.utils.SuccessException;
import io.pravega.test.integration.connectors.utils.ThrottledIntegerWriter;
import io.pravega.test.integration.utils.SetupUtils;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Automated tests for {@link FlinkExactlyOncePravegaReader}.
 */
@Slf4j
public class FlinkExactlyOncePravegaReaderTest extends StreamingMultipleProgramsTestBase {

    // Number of events to produce into the test stream.
    private static final int NUM_STREAM_ELEMENTS = 10000;

    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    //Ensure each test completes within 30 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(60, TimeUnit.SECONDS);

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testOneSourceOneSegment() throws Exception {
        runTest(1, 1, NUM_STREAM_ELEMENTS);
    }

    @Test
    public void testOneSourceMultipleSegments() throws Exception {
        runTest(1, 4, NUM_STREAM_ELEMENTS);
    }

    // this test currently does not work, see https://github.com/pravega/pravega/issues/1152
    //    @Test
    //    public void testMultipleSourcesOneSegment() throws Exception {
    //        runTest(4, 1, NUM_STREAM_ELEMENTS);
    //    }

    @Test
    public void testMultipleSourcesMultipleSegments() throws Exception {
        runTest(4, 4, NUM_STREAM_ELEMENTS);
    }


    private static void runTest(
            final int sourceParallelism,
            final int numPravegaSegments,
            final int numElements) throws Exception {

        // set up the stream
        final String streamName = RandomStringUtils.randomAlphabetic(20);
        SETUP_UTILS.createTestStream(streamName, numPravegaSegments);
        
        try (
                final EventStreamWriter<Integer> eventWriter = SETUP_UTILS.getIntegerWriter(streamName);

                // create the producer that writes to the stream
                final ThrottledIntegerWriter producer = new ThrottledIntegerWriter(
                        eventWriter,
                        numElements,
                        numElements * 2 / 3, // the latest when a checkpoint must have happened
                        1                    // the initial sleep time per element
                )
                
        ) {
            producer.start();

            // the producer is throttled so that we don't run the (whatever small) risk of pumping
            // all elements through before completing the first checkpoint (that would make the test senseless)
            
            // to speed the test up, we un-throttle the producer as soon as the first checkpoint
            // has gone through. Rather than implementing a complicated observer that polls the status
            // from Flink, we simply forward the 'checkpoint complete' notification from the user functions
            // the thr throttler, via a static variable
            NotifyingMapper.TO_CALL_ON_CHECKPOINT_COMPLETION.set(producer::unthrottle);
        
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            
            env.setParallelism(sourceParallelism);
            env.enableCheckpointing(100); // checkpoint frequently
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));

            // the Pravega reader
            final FlinkExactlyOncePravegaReader<Integer> pravegaSource = new FlinkExactlyOncePravegaReader<>(
                    SETUP_UTILS.getControllerUri(),
                    SETUP_UTILS.getScope(),
                    Collections.singleton(streamName),
                    0,
                    new IntDeserializer());
    
            env
                    .addSource(pravegaSource)
    
                    // this mapper throws an exception half way through the number of elements
                    .map(new FailingMapper<>(numElements / 2 / sourceParallelism))
    
                    // hook in the notifying mapper
                    .map(new NotifyingMapper<>())

                    // the sink validates that the exactly-once semantics hold
                    // it must be non-parallel so that it sees all elements and can trivially
                    // check for duplicates
                    .addSink(new IntSequenceExactlyOnceValidator(numElements))
                            .setParallelism(1);

            final long executeStart = System.nanoTime();

            // if these calls complete without exception, then the test passes
            try {
                env.execute();
            } catch (Exception e) {
                if (!(ExceptionUtils.getRootCause(e) instanceof SuccessException)) {
                    throw e;
                }
            }

            // this method forwards exception thrown in the data generator thread
            producer.sync();

            final long executeEnd = System.nanoTime();
            System.out.println(String.format("Test execution took %d ms", (executeEnd - executeStart) / 1_000_000));
        }
    }

    // ----------------------------------------------------------------------------

    private static class IntDeserializer extends AbstractDeserializationSchema<Integer> {

        @Override
        public Integer deserialize(byte[] message) throws IOException {
            return ByteBuffer.wrap(message).getInt();
        }

        @Override
        public boolean isEndOfStream(Integer nextElement) {
            return false;
        }
    }
}
