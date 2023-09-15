/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.test.system;


import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import static io.pravega.test.system.AbstractSystemTest.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReaderCheckpointWithInternalThreadpool {
    private static final long READ_TIMEOUT = SECONDS.toMillis(500);
    private static final String SCOPE = "scope";
    private static final String STREAM = "checkpointStream";
    private static final String READER_GROUP_NAME = "checkpointRG";
    private static final String CHECKPOINT1 = "checkpoint1";
    private static final String CHECKPOINT2 = "checkpoint2";
    private static final String READER = "reader";
    @Rule
    public Timeout globalTimeout = Timeout.seconds(7 * 60);
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller = null;
    private ConnectionFactory connectionFactory = null;
    private ClientConfig clientConfig = null;
    private final StreamConfiguration streamConfig = StreamConfiguration.builder()
            .scalingPolicy(ScalingPolicy.fixed(1)).build();

    private final ScheduledExecutorService readerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1,
            "readerCheckpoint-reader");

    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        controllerURI = fetchControllerURI();
        clientConfig = Utils.buildClientConfig(controllerURI);
        streamManager = StreamManager.create(clientConfig);

        connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(clientConfig)
                .build(), connectionFactory.getInternalExecutor());
    }

    @After
    public void tearDown() {
        streamManager.close();
        controller.close();
        connectionFactory.close();
    }

    /*This test case is to call initaite checkpoint API with internal excutor thread pool.
    * Number of thread to construct SocketConnectionFactory should be default to more than 1 in ReaderGroupManager constructor
    * with single thread we do see that when readers read continuously and in parallel if we get request for initiate checkpoint then that checkpoint never completes
     */

    @Test
    public void initiateCheckpointTest() {
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, streamConfig));

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(SCOPE, STREAM))
                .disableAutomaticCheckpoints()
                .build();
        readerGroupManager.createReaderGroup(READER_GROUP_NAME, readerGroupConfig);

        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP_NAME);

        // Write events
        int start = 1;
        int end = 500;
        log.info("Write events with range [{},{})", start, end);
        writeEventsToStream(SCOPE, IntStream.range(start, end).boxed().collect(Collectors.toList()));
        asyncReadEvents(SCOPE, READER);
        //Initiate 2 checkpoints and make sure its completed
        initiateCheckpointAndVerify(readerGroup);
    }

    private Checkpoint initiateCheckpoint(final ReaderGroup readerGroup, final String checkPointName) {
        Checkpoint result = null;
        log.info("Called Initiating checkpoint for {} ", checkPointName);
        CompletableFuture<Checkpoint> cp = readerGroup.initiateCheckpoint(checkPointName);

        try {
            result = cp.get(5, TimeUnit.SECONDS);
            log.info("Checkpoint isDone {} for checkpoint {} ", cp.isDone(), result.getName());
        } catch (Exception e)  {
            if (e instanceof InterruptedException || e instanceof ExecutionException || e instanceof TimeoutException) {
                log.error("Exception while initiating checkpoint: {}", checkPointName, e);
                fail("Exception while initiating checkpoint is not expected");
            }
        }
        return result;
    }

    private void initiateCheckpointAndVerify(final ReaderGroup readerGroup) {
        log.info("Initiating checkpoint ");
        final List<Checkpoint> cpResults = new ArrayList<>(2);
        final List<String> cpNames = new ArrayList<>(Arrays.asList(CHECKPOINT1, CHECKPOINT2));

        for (String name: cpNames) {
            cpResults.add(initiateCheckpoint( readerGroup, name));
        }
        //assert for checkpoint names
        for (Checkpoint cp : cpResults) {
            assertTrue(cpNames.contains(cp.getName()));
        }
    }

    private void asyncReadEvents(final String scope, final String readerId) {
        CompletableFuture<Void> result = CompletableFuture.supplyAsync(
                () -> readEvents(scope, readerId), readerExecutor);
        Futures.exceptionListener(result,
                t -> log.error("Error observed while reading events for reader id :{}", readerId, t));

    }

    private <T extends Serializable> void writeEventsToStream(final String scope, final List<T> events) {
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
             EventStreamWriter<T> writer = clientFactory.createEventWriter(STREAM,
                     new JavaSerializer<T>(),
                     EventWriterConfig.builder().build())) {
            for (T event : events) {
                log.info("Writing message: {} with routing-key: {} to stream {}", event, "", STREAM);
                writer.writeEvent("", event);
            }
            log.info("All events written successfully to stream {}", STREAM);
        }
    }

    private <T extends Serializable> Void readEvents(final String scope, final String readerId) {
        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
             EventStreamReader<T> reader = clientFactory.createReader(readerId,
                     READER_GROUP_NAME,
                     new JavaSerializer<T>(),
                     ReaderConfig.builder().build())) {
            log.info("Reading events from {}/{} with readerId: {}", scope, STREAM, readerId);
            EventRead<T> event = null;
            do {
                try {
                    event = reader.readNextEvent(READ_TIMEOUT);
                    log.info("Read event {}", event.getEvent());
                    if (event.isCheckpoint()) {
                        log.info("Read a check point event, checkpointName: {}", event.getCheckpointName());
                    }
                } catch (ReinitializationRequiredException e) {
                    log.error("Exception while reading event using readerId: {}", readerId, e);
                    fail("Reinitialization Exception is not expected");
                }
            } while (event.isCheckpoint() && event.getCheckpointName() != CHECKPOINT2);
            //stop reading if CHECKPOINT_2 is acked by reader.
            log.info("No more events from {}/{} for readerId: {}", scope, STREAM, readerId);
        }
        return null;
    }

    private URI fetchControllerURI() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        return ctlURIs.get(0);
    }

}
