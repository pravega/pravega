/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.services.BookkeeperService;
import com.emc.pravega.framework.services.PravegaControllerService;
import com.emc.pravega.framework.services.PravegaSegmentStoreService;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReadWithAutoScaleTest {

    private final static String SCOPE = "testReadAutoScale" + new Random().nextInt();
    private final static String STREAM_NAME = "testScaleUp";
    private final static String READER_GROUP_NAME = "testReaderGroup" + new Random().nextInt();

    //Initial number of segments is 2.
    private static final ScalingPolicy SCALING_POLICY = new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS_PER_SEC,
            1, 2, 2);
    private static final StreamConfiguration CONFIG_UP = StreamConfiguration.builder().scope(SCOPE)
            .streamName(STREAM_NAME).scalingPolicy(SCALING_POLICY).build();

    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

    private final AtomicReference<ClientFactory> clientFactoryRef = new AtomicReference<>();
    private final AtomicReference<ControllerImpl> controllerRef = new AtomicReference<>();

    @Environment
    public static void setup() throws Exception {

        //1. check if zk is running, if not start it
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);

        //get the zk ip details and pass it to bk, host, controller
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = new BookkeeperService("bookkeeper", zkUris.get(0));
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        log.debug("Bookkeeper service details: {}", bkService.getServiceDetails());

        //3. start controller
        Service conService = new PravegaControllerService("controller", zkUris.get(0));
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);

        //4.start host
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUris.get(0), conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        log.debug("Pravega host service details: {}", segService.getServiceDetails());
    }

    /**
     * Invoke the createStream method, ensure we are able to create scope, stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     * @throws ExecutionException   if error in create stream
     */
    @Before
    public void createStream() throws InterruptedException, URISyntaxException, ExecutionException {

        //create a scope
        URI controllerUri = getControllerURI();

        com.emc.pravega.stream.impl.Controller controller = getController(controllerUri);
        CompletableFuture<Controller.CreateScopeStatus> createScopeStatus = controller.createScope(SCOPE);
        log.debug("Create scope status {}", createScopeStatus.get().getStatus());
        assertNotEquals(Controller.CreateScopeStatus.Status.FAILURE, createScopeStatus.get().getStatus());

        //create a stream
        CompletableFuture<Controller.CreateStreamStatus> createStreamStatus = controller.createStream(CONFIG_UP);
        log.debug("Create stream status for scale up stream {}", createStreamStatus.get().getStatus());
        assertNotEquals(Controller.CreateStreamStatus.Status.FAILURE, createStreamStatus.get().getStatus());
    }

    @Test
    public void scaleTests() throws URISyntaxException, InterruptedException {

        CompletableFuture<Void> testResult = scaleUpWithTxnAndReaderGroup();
        FutureHelpers.getAndHandleExceptions(testResult
                .whenComplete((r, e) -> {
                    recordResult(testResult, "ScaleUpWithTxnWithReaderGroup");
                }), RuntimeException::new);
    }

    /**
     * System test to test the following scenario:
     * - One or more writers writing to a stream using transactions.
     * - A reader group reading from the stream.
     * - Auto scaling is exercised by inducing higher write load to the stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     */
    public CompletableFuture<Void> scaleUpWithTxnAndReaderGroup() throws InterruptedException, URISyntaxException {

        URI controllerUri = getControllerURI();
        ControllerImpl controller = getController(controllerUri);
        ConcurrentLinkedQueue<Long> eventsReadFromPravega = new ConcurrentLinkedQueue<>();

        final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false); //TODO: remove

        final AtomicLong data = new AtomicLong(); //data used by each of the writers.

        @Cleanup
        ClientFactory clientFactory = getClientFactory();

        //1. Start writing events to the Stream.
        startNewTxnWriter(data, clientFactory, stopWriteFlag);

        //2. Start a reader group with 2 readers (The stream is configured with 2 segments.)

        //2.1 Create a reader group.
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        readerGroupManager.createReaderGroup(READER_GROUP_NAME, ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singleton(STREAM_NAME));

        //2.2 Create readers.
        CompletableFuture<Void> reader1 = startReader("reader-1", clientFactory, READER_GROUP_NAME,
                eventsReadFromPravega);
        CompletableFuture<Void> reader2 = startReader("reader-2", clientFactory, READER_GROUP_NAME,
                eventsReadFromPravega);

        //3 Now increase the number of TxnWriters to trigger scale operation.
        startNewTxnWriter(data, clientFactory, stopWriteFlag);
        startNewTxnWriter(data, clientFactory, stopWriteFlag);
        startNewTxnWriter(data, clientFactory, stopWriteFlag);
        startNewTxnWriter(data, clientFactory, stopWriteFlag);
        startNewTxnWriter(data, clientFactory, stopWriteFlag);

        //4 Wait until the scale operation is triggered (else time out)
        //    validate the data read by the readers ensuring all the events are read and there are no duplicates.
        return Retry.withExpBackoff(10, 10, 200, Duration.ofSeconds(10).toMillis())
                .retryingOn(NotDoneException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> controller.getCurrentSegments(SCOPE, STREAM_NAME)
                        .thenAccept(x -> {
                            int currentNumOfSegments = x.getSegments().size();
                            if (currentNumOfSegments == 2) {
                                throw new NotDoneException();
                            } else if (currentNumOfSegments > 2) {
                                //scale operation successful.
                                log.info("Current Number of segments is {}", currentNumOfSegments);
                                stopWriteFlag.set(true);
                            } else {
                                Assert.fail("Current number of Segments reduced to less than 2. Failure of test");
                            }
                        }), EXECUTOR_SERVICE).thenCompose(v -> CompletableFuture.allOf(reader1, reader2))
                .thenRun(() -> validateResults(data.get(), eventsReadFromPravega));
    }

    //Helper methods
    private void validateResults(final long lastEventCount, final Collection<Long> readEvents) {
        log.info("Last Event Count is {}", lastEventCount);
        assertTrue("Overflow in the number of event published ", lastEventCount > 0);
        assertEquals(lastEventCount, readEvents.size()); // Number of event read should be equal to number of events
        // published.
        assertEquals(lastEventCount, new TreeSet<>(readEvents).size()); //check unique events.
    }

    private CompletableFuture<Void> startReader(final String id, final ClientFactory clientFactory, final String
            readerGroupName, ConcurrentLinkedQueue<Long> result) {

        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<Long> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());

            while (true) {
                final Long longEvent;
                try {
                    longEvent = reader.readNextEvent(SECONDS.toMillis(5)).getEvent();
                    // result is null if no events present/ all events consumed.
                    if (longEvent != null) {
                        result.add(longEvent);
                    } else {
                        //null returned indicates nothing to read.
                        log.info("Stopping reader with ID: {} has nothing to read", id);
                        break;
                    }
                } catch (ReinitializationRequiredException e) {
                    log.warn("Test Exeception while reading from the stream", e);
                    break;
                }
            }
        });
    }


    private void startNewTxnWriter(final AtomicLong data, final ClientFactory clientFactory, final AtomicBoolean
            exitFlag) {
        CompletableFuture.runAsync(() -> {
            @Cleanup
            EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().build());
            while (!exitFlag.get()) {
                try {
                    //create a transaction with 10 events.
                    Transaction<Long> transaction = writer.beginTxn(5000, 3600000, 60000);
                    for (int i = 0; i < 10; i++) {
                        long value = data.incrementAndGet();
                        transaction.writeEvent(String.valueOf(value), value);
                    }
                    transaction.commit();

                } catch (Throwable e) {
                    log.warn("test exception writing events in a transaction.", e);
                    break;
                }
            }
        });
    }

    private ControllerImpl getController(final URI controllerUri) {
        if (controllerRef.get() == null) {
            log.debug("Controller uri:" + controllerUri.getHost() + ":" + controllerUri.getPort());

            controllerRef.set(new ControllerImpl(controllerUri.getHost(), controllerUri.getPort()));
        }
        return controllerRef.get();
    }

    private URI getControllerURI() {
        Service conService = new PravegaControllerService("controller", null);
        List<URI> ctlURIs = conService.getServiceDetails();
        return ctlURIs.get(0);
    }

    private ClientFactory getClientFactory() {
        if (clientFactoryRef.get() == null) {
            clientFactoryRef.set(ClientFactory.withScope(SCOPE, getControllerURI()));
        }
        return clientFactoryRef.get();
    }

    private class NotDoneException extends RuntimeException {
    }

    private void recordResult(CompletableFuture<Void> scaleTestResult, String testName) {
        FutureHelpers.getAndHandleExceptions(scaleTestResult.handle((r, e) -> {
            if (e != null) {
                log.error("test {} failed with exception {}", testName, e);
            } else {
                log.debug("test {} succeed", testName);
            }
            return null;
        }), RuntimeException::new);
    }
}
