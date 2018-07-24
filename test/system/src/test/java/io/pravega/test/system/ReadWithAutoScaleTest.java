/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReadWithAutoScaleTest extends AbstractScaleTests {

    private final static String STREAM_NAME = "testTxnScaleUpWithRead";
    private final static String READER_GROUP_NAME = "testReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    //Initial number of segments is 2.
    private static final ScalingPolicy SCALING_POLICY = ScalingPolicy.byEventRate(1, 2, 2);
    private static final StreamConfiguration CONFIG = StreamConfiguration.builder().scope(SCOPE)
            .streamName(STREAM_NAME).scalingPolicy(SCALING_POLICY).build();

    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(12 * 60);

    @Environment
    public static void setup() {

        //1. check if zk is running, if not start it
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);

        //get the zk ip details and pass it to bk, host, controller
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = Utils.createBookkeeperService(zkUris.get(0));
        if (!bkService.isRunning()) {
            bkService.start(true);
        }
        log.debug("Bookkeeper service details: {}", bkService.getServiceDetails());

        //3. start controller
        Service conService = Utils.createPravegaControllerService(zkUris.get(0));
        if (!conService.isRunning()) {
            conService.start(true);
        }
        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);

        //4.start host
        Service segService = Utils.createPravegaSegmentStoreService(zkUris.get(0), conUris.get(0));
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
    public void createStream() throws InterruptedException, ExecutionException {

        Controller controller = getController();

        //create a scope
        Boolean createScopeStatus = controller.createScope(SCOPE).get();
        log.debug("Create scope status {}", createScopeStatus);

        //create a stream
        Boolean createStreamStatus = controller.createStream(CONFIG).get();
        log.debug("Create stream status {}", createStreamStatus);
    }

    @Ignore
    @Test
    public void scaleTestsWithReader() {

        URI controllerUri = getControllerURI();
        ControllerImpl controller = getController();
        ConcurrentLinkedQueue<Long> eventsReadFromPravega = new ConcurrentLinkedQueue<>();

        final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
        final AtomicLong eventData = new AtomicLong(); //data used by each of the writers.
        final AtomicLong eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.

        @Cleanup
        ClientFactory clientFactory = getClientFactory();

        //1. Start writing events to the Stream.
        CompletableFuture<Void> writer1 = startNewTxnWriter(eventData, clientFactory, stopWriteFlag);

        //2. Start a reader group with 2 readers (The stream is configured with 2 segments.)

        //2.1 Create a reader group.
        log.info("Creating Reader group : {}", READER_GROUP_NAME);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, controllerUri);
        readerGroupManager.createReaderGroup(READER_GROUP_NAME, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM_NAME)).build());

        //2.2 Create readers.
        CompletableFuture<Void> reader1 = startReader("reader1", clientFactory, READER_GROUP_NAME,
                eventsReadFromPravega, eventData, eventReadCount, stopReadFlag );
        CompletableFuture<Void> reader2 = startReader("reader2", clientFactory, READER_GROUP_NAME,
                eventsReadFromPravega, eventData, eventReadCount, stopReadFlag);

        //3 Now increase the number of TxnWriters to trigger scale operation.
        log.info("Increasing the number of writers to 6");
        CompletableFuture<Void> writer2 = startNewTxnWriter(eventData, clientFactory, stopWriteFlag);
        CompletableFuture<Void> writer3 = startNewTxnWriter(eventData, clientFactory, stopWriteFlag);
        CompletableFuture<Void> writer4 = startNewTxnWriter(eventData, clientFactory, stopWriteFlag);
        CompletableFuture<Void> writer5 = startNewTxnWriter(eventData, clientFactory, stopWriteFlag);
        CompletableFuture<Void> writer6 = startNewTxnWriter(eventData, clientFactory, stopWriteFlag);

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
                    log.info("All writers have stopped. Setting Stop_Read_Flag. Event Written Count:{}, Event Read " +
                            "Count: {}", eventData.get(), eventsReadFromPravega.size());
                    return CompletableFuture.allOf(reader1, reader2);
                })
                .thenRun(() -> validateResults(eventData.get(), eventsReadFromPravega));

        Futures.getAndHandleExceptions(testResult
                .whenComplete((r, e) -> {
                    recordResult(testResult, "ScaleUpWithTxnWithReaderGroup");
                }), RuntimeException::new);
        readerGroupManager.deleteReaderGroup(READER_GROUP_NAME);
    }

    //Helper methods
    private void validateResults(final long lastEventCount, final Collection<Long> readEvents) {
        log.info("Last Event Count is {}", lastEventCount);
        assertTrue("Overflow in the number of events published ", lastEventCount > 0);
        // Number of event read should be equal to number of events published.
        assertEquals(lastEventCount, readEvents.size());
        assertEquals(lastEventCount, new TreeSet<>(readEvents).size()); //check unique events.
    }

    private CompletableFuture<Void> startReader(final String id, final ClientFactory clientFactory, final String
            readerGroupName, final ConcurrentLinkedQueue<Long> readResult, final AtomicLong writeCount, final
    AtomicLong readCount, final AtomicBoolean exitFlag) {

        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<Long> reader = clientFactory.createReader(id,
                    readerGroupName,
                    new JavaSerializer<Long>(),
                    ReaderConfig.builder().build());
            long count;
            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                // exit only if exitFlag is true  and read Count equals write count.
                try {
                    final Long longEvent = reader.readNextEvent(SECONDS.toMillis(60)).getEvent();
                    if (longEvent != null) {
                        //update if event read is not null.
                        readResult.add(longEvent);
                        count = readCount.incrementAndGet();
                        log.debug("Reader {}, read count {}", id, count);
                    } else {
                        log.debug("Null event, reader {}, read count {}", id, readCount.get());
                    }
                } catch (ReinitializationRequiredException e) {
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
                    EventWriterConfig.builder().transactionTimeoutTime(25000).build());
            while (!exitFlag.get()) {
                try {
                    //create a transaction with 10 events.
                    Transaction<Long> transaction = Retry.withExpBackoff(10, 10, 20, ofSeconds(1).toMillis())
                            .retryingOn(TxnCreationFailedException.class)
                            .throwingOn(RuntimeException.class)
                            .run(() -> createTransaction(writer, exitFlag));

                    for (int i = 0; i < 100; i++) {
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
            txn = writer.beginTxn();
            log.info("Transaction created with id:{} ", txn.getTxnId());
        } catch (RuntimeException ex) {
            log.info("Exception encountered while trying to begin Transaction ", ex.getCause());
            final Class<? extends Throwable> exceptionClass = ex.getCause().getClass();
            if (exceptionClass.equals(io.grpc.StatusRuntimeException.class) && !exitFlag.get())  {
                //Exit flag is true no need to retry.
                log.warn("Cause for failure is {} and we need to retry", exceptionClass.getName());
                throw new TxnCreationFailedException(); // we can retry on this exception.
            } else {
                throw ex;
            }
        }
        return txn;
    }

    private class TxnCreationFailedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
