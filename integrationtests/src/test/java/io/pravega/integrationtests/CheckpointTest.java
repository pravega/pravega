/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.integrationtests;

import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.host.handler.PravegaConnectionListener;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.stream.Checkpoint;
import io.pravega.stream.EventRead;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReaderGroup;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.ReinitializationRequiredException;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.Sequence;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.mock.MockClientFactory;
import io.pravega.stream.mock.MockStreamManager;
import io.pravega.testcommon.InlineExecutor;
import io.pravega.testcommon.TestUtils;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CheckpointTest {

    private static final long CLOCK_ADVANCE_INTERVAL = 60 * 1000000000L;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
    }

    @Test(timeout = 20000)
    public void testCheckpointAndRestore() throws ReinitializationRequiredException, InterruptedException, ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroupName = "group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName,
                                   StreamConfiguration.builder()
                                                      .scope(scope)
                                                      .streamName(streamName)
                                                      .scalingPolicy(ScalingPolicy.fixed(1))
                                                      .build());
        ReaderGroup readerGroup = streamManager.createReaderGroup(readerGroupName,
                                                                  groupConfig,
                                                                  Collections.singleton(streamName));
        JavaSerializer<String> serializer = new JavaSerializer<>();
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName,
                                                                             serializer,
                                                                             EventWriterConfig.builder().build());
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName,
                                                                      readerGroupName,
                                                                      serializer,
                                                                      ReaderConfig.builder().build(),
                                                                      clock::get,
                                                                      clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", new InlineExecutor());
        assertFalse(checkpoint.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("Checkpoint", cpResult.getName());
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());
        assertFalse(read.isCheckpoint());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        readerGroup.resetReadersToCheckpoint(cpResult);
        try {
            reader.readNextEvent(60000);
            fail();
        } catch (ReinitializationRequiredException e) {
            //Expected
        }
        reader.close();
        reader = clientFactory.createReader(readerName, readerGroupName, serializer, ReaderConfig.builder().build());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());
        assertFalse(read.isCheckpoint());
    }

}
