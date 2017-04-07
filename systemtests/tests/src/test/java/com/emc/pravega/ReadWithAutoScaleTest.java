/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
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
import java.net.URI;
import java.net.URISyntaxException;
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
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReadWithAutoScaleTest extends AbstractScaleTests {

    private final static String SCOPE = "testReadAutoScale" + new Random().nextInt(Integer.MAX_VALUE);
    private final static String STREAM_NAME = "testScaleUp";
    private final static String READER_GROUP_NAME = "testReaderGroup" + new Random().nextInt(Integer.MAX_VALUE);

    //Initial number of segments is 2.
    private static final ScalingPolicy SCALING_POLICY = ScalingPolicy.byEventRate(1, 2, 2);
    private static final StreamConfiguration CONFIG = StreamConfiguration.builder().scope(SCOPE)
            .streamName(STREAM_NAME).scalingPolicy(SCALING_POLICY).build();

    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

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

        URI controllerUri = getControllerURI();
        com.emc.pravega.stream.impl.Controller controller = getController(controllerUri);

        //create a scope
        Boolean createScopeStatus = controller.createScope(SCOPE).get();
        log.debug("Create scope status {}", createScopeStatus);

        //create a stream
        Boolean createStreamStatus = controller.createStream(CONFIG).get();
        log.debug("Create stream status {}", createStreamStatus);
    }

    @Test
    public void scaleTestsWithReader() throws URISyntaxException, InterruptedException {

        URI controllerUri = getControllerURI();
        ControllerImpl controller = getController(controllerUri);
        ConcurrentLinkedQueue<Long> eventsReadFromPravega = new ConcurrentLinkedQueue<>();

        final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
        final AtomicLong data = new AtomicLong(); //data used by each of the writers.

        @Cleanup
        ClientFactory clientFactory = getClientFactory(SCOPE);

        //1. Start writing events to the Stream.
        CompletableFuture<Void> writer1 = startNewTxnWriter(data, clientFactory, stopWriteFlag);

        //2. Start a reader group with 2 readers (The stream is configured with 2 segments.)

        //2.1 Create a reader group.
        log.info("Creating Reader group : {}", READER_GROUP_NAME);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        readerGroupManager.createReaderGroup(READER_GROUP_NAME, ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singleton(STREAM_NAME));

        //2.2 Create readers.
        CompletableFuture<Void> reader1 = startReader("reader1", clientFactory, READER_GROUP_NAME,
                eventsReadFromPravega, stopReadFlag );
        CompletableFuture<Void> reader2 = startReader("reader2", clientFactory, READER_GROUP_NAME,
                eventsReadFromPravega, stopWriteFlag);

        //3 Now increase the number of TxnWriters to trigger scale operation.
        log.info("Increasing the number of writers to 6");
        CompletableFuture<Void> writer2 = startNewTxnWriter(data, clientFactory, stopWriteFlag);
        CompletableFuture<Void> writer3 = startNewTxnWriter(data, clientFactory, stopWriteFlag);
        CompletableFuture<Void> writer4 = startNewTxnWriter(data, clientFactory, stopWriteFlag);
        CompletableFuture<Void> writer5 = startNewTxnWriter(data, clientFactory, stopWriteFlag);
        CompletableFuture<Void> writer6 = startNewTxnWriter(data, clientFactory, stopWriteFlag);

        //4 Wait until the scale operation is triggered (else time out)
        //    validate the data read by the readers ensuring all the events are read and there are no duplicates.
        CompletableFuture<Void> testResult = Retry.withExpBackoff(10, 10, 40, ofSeconds(10).toMillis())
                .retryingOn(ScaleOperationNotDoneException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> controller.getCurrentSegments(SCOPE, STREAM_NAME)
                        .thenAccept(x -> {
                            int currentNumOfSegments = x.getSegments().size();
                            if (currentNumOfSegments == 2) {
                                log.info("The current number of segments is equal to 2, ScaleOperation did not happen");
                                //Scaling operation did not happen, retry operation.
                                throw new ScaleOperationNotDoneException();
                            } else if (currentNumOfSegments > 2) {
                                //scale operation successful.
                                log.info("Current Number of segments is {}", currentNumOfSegments);
                                stopWriteFlag.set(true);
                            } else {
                                Assert.fail("Current number of Segments reduced to less than 2. Failure of test");
                            }
                        }), EXECUTOR_SERVICE)
                .thenCompose(v -> CompletableFuture.allOf(writer1, writer2, writer3, writer4, writer5, writer6))
                .thenCompose(v -> {
                    stopReadFlag.set(true);
                    return CompletableFuture.allOf(reader1, reader2);
                })
                .thenRun(() -> validateResults(data.get(), eventsReadFromPravega));

        FutureHelpers.getAndHandleExceptions(testResult
                .whenComplete((r, e) -> {
                    recordResult(testResult, "ScaleUpWithTxnWithReaderGroup");
                }), RuntimeException::new);
    }

    //Helper methods
    private void validateResults(final long lastEventCount, final Collection<Long> readEvents) {
        log.info("Last Event Count is {}", lastEventCount);
        assertTrue("Overflow in the number of events published ", lastEventCount > 0);
        assertEquals(lastEventCount, readEvents.size()); // Number of event read should be equal to number of events
        // published.
        assertEquals(lastEventCount, new TreeSet<>(readEvents).size()); //check unique events.
    }

    private CompletableFuture<Void> startReader(final String id, final ClientFactory clientFactory, final String
            readerGroupName, final ConcurrentLinkedQueue<Long> result, final AtomicLong writeCount , final AtomicBoolean
            exitFlag) {

        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<Long> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            boolean isLastEventNull = false;
            while (!(exitFlag.get() && result.size() == writeCount.get())) {
                // exit only if exitFlag and read Count and write count are same.
                try {
                    final Long longEvent = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
                    if (longEvent != null) {
                        //update if event read is not null.
                        result.add(longEvent);
                    }
                } catch (ReinitializationRequiredException e) { //TODO: Remove throwable once issue #862 is resolved.
                    log.warn("Test Exception while reading from the stream", e);
                    break;
                }
            }
        });
    }

    private CompletableFuture<Void> startNewTxnWriter(final AtomicLong data, final ClientFactory clientFactory,
                                                      final AtomicBoolean exitFlag) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            EventStreamWriter<Long> writer = clientFactory.createEventWriter(STREAM_NAME,
                    new JavaSerializer<Long>(),
                    EventWriterConfig.builder().build());
            while (!exitFlag.get()) {
                try {
                    //create a transaction with 10 events.
                    Transaction<Long> transaction = Retry.withExpBackoff(10, 10, 20, ofSeconds(1).toMillis())
                            .retryingOn(TxnCreationFailedException.class)
                            .throwingOn(RuntimeException.class)
                            .run(() -> createTransaction(writer, exitFlag));

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

    private Transaction<Long> createTransaction(EventStreamWriter<Long> writer, final AtomicBoolean exitFlag) {
        Transaction<Long> txn = null;
        try {
            //Default max scale grace period is 30000
            txn = writer.beginTxn(5000, 3600000, 29000);
            log.debug("Transaction created with id:{} ", txn.getTxnId());
        } catch (RuntimeException ex) {
            log.info("Exception encountered while trying to begin Transaction ", ex.getCause());
            final Class<? extends Throwable> exceptionClass = ex.getCause().getClass();
            if (exceptionClass.equals(io.grpc.StatusRuntimeException.class) && !exitFlag.get())  {
                //Exit flag is true no need to retry.
                log.debug("Cause for failure is {} and we need to retry", exceptionClass.getName());
                throw new TxnCreationFailedException(); // we can retry on this exception.
            } else {
                throw ex;
            }
        }
        return txn;
    }

    private class TxnCreationFailedException extends RuntimeException {
    }
}
