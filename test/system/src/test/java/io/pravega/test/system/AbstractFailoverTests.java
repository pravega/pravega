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

import com.google.common.base.Preconditions;
import io.netty.util.internal.ConcurrentSet;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
abstract class AbstractFailoverTests {

    static final String AUTO_SCALE_STREAM = "testReadWriteAndAutoScaleStream";
    static final String SCALE_STREAM = "testReadWriteAndScaleStream";
    static final String RK_VALUE_SEPARATOR = ":";
    //Duration for which the system test waits for writes/reads to happen post failover.
    //10s (SessionTimeout) + 10s (RebalanceContainers) + 20s (For Container recovery + start) + NetworkDelays
    static final int WAIT_AFTER_FAILOVER_MILLIS = 40 * 1000;
    static final int WRITER_MAX_BACKOFF_MILLIS = 5 * 1000;
    static final int WRITER_MAX_RETRY_ATTEMPTS = 20;
    static final int NUM_EVENTS_PER_TRANSACTION = 50;
    static final int RK_RENEWAL_RATE_TRANSACTION = NUM_EVENTS_PER_TRANSACTION / 2;
    static final int RK_RENEWAL_RATE_WRITER = 500;
    static final int SCALE_WAIT_ITERATIONS = 12;

    final String readerName = "reader";
    Service controllerInstance;
    Service segmentStoreInstance;
    URI controllerURIDirect = null;
    ScheduledExecutorService executorService;
    ScheduledExecutorService controllerExecutorService;
    Controller controller;
    TestState testState;

    static class TestState {
        //read and write count variables
        final AtomicBoolean stopReadFlag = new AtomicBoolean(false);
        final AtomicBoolean stopWriteFlag = new AtomicBoolean(false);
        final AtomicReference<Throwable> getWriteException = new AtomicReference<>();
        final AtomicReference<Throwable> getTxnWriteException = new AtomicReference<>();
        final AtomicReference<Throwable> getReadException =  new AtomicReference<>();
        //list of all writer's futures
        final List<CompletableFuture<Void>> writers = synchronizedList(new ArrayList<>());
        //list of all reader's futures
        final List<CompletableFuture<Void>> readers = synchronizedList(new ArrayList<>());
        final List<CompletableFuture<Void>> writersListComplete = synchronizedList(new ArrayList<>());
        final CompletableFuture<Void> writersComplete = new CompletableFuture<>();
        final CompletableFuture<Void> newWritersComplete = new CompletableFuture<>();
        final CompletableFuture<Void> readersComplete = new CompletableFuture<>();
        final List<CompletableFuture<Void>> txnStatusFutureList = synchronizedList(new ArrayList<>());
        final ConcurrentSet<UUID> committingTxn = new ConcurrentSet<>();
        final ConcurrentSet<UUID> abortedTxn = new ConcurrentSet<>();
        final boolean txnWrite;

        final AtomicLong writtenEvents = new AtomicLong();
        final AtomicLong readEvents = new AtomicLong();

        TestState(boolean txnWrite) {
            this.txnWrite = txnWrite;
        }

        long incrementTotalWrittenEvents() {
            return writtenEvents.incrementAndGet();
        }

        long incrementTotalWrittenEvents(int increment) {
            return writtenEvents.addAndGet(increment);
        }

        long incrementTotalReadEvents() {
            return readEvents.incrementAndGet();
        }

        long getEventWrittenCount() {
            return writtenEvents.get();
        }

        long getEventReadCount() {
            return readEvents.get();
        }

        void checkForAnomalies() {
            boolean failed = false;
            long eventReadCount = getEventReadCount();
            long eventWrittenCount = getEventWrittenCount();
            if (eventReadCount != eventWrittenCount) {
                failed = true;
                log.error("Read write count mismatch => readCount = {}, writeCount = {}", eventReadCount, eventWrittenCount);
            }

            if (committingTxn.size() > 0) {
                failed = true;
                log.error("Txn left committing: {}", committingTxn);
            }

            if (abortedTxn.size() > 0) {
                failed = true;
                log.error("Txn aborted: {}", abortedTxn);
            }
            assertFalse("Test Failed", failed);
        }

        public void cancelAllPendingWork() {
            synchronized (readers) {
                readers.forEach(future -> {
                    try {
                        future.cancel(true);
                    } catch (Exception e) {
                        log.error("exception thrown while cancelling reader thread", e);
                    }
                });
            }

            synchronized (writers) {
                writers.forEach(future -> {
                    try {
                        future.cancel(true);
                    } catch (Exception e) {
                        log.error("exception thrown while cancelling writer thread", e);
                    }
                });
            }
        }
    }

    void performFailoverTest() throws ExecutionException {
        log.info("Test with 3 controller, segment store instances running and without a failover scenario");
        long currentWriteCount1 = testState.getEventWrittenCount();
        long currentReadCount1 = testState.getEventReadCount();
        log.info("Read count: {}, write count: {} without any failover", currentReadCount1, currentWriteCount1);

        //check reads and writes after sleeps
        log.info("Sleeping for {} ", WAIT_AFTER_FAILOVER_MILLIS);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));

        long currentWriteCount2 = testState.getEventWrittenCount();
        long currentReadCount2 = testState.getEventReadCount();
        log.info("Read count: {}, write count: {} without any failover after sleep before scaling", currentReadCount2, currentWriteCount2);
        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down segment store instances to 2
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(2), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down Segment Store instances from 3 to 2");

        currentWriteCount1 = testState.getEventWrittenCount();
        currentReadCount1 = testState.getEventReadCount();
        log.info("Read count: {}, write count: {} after Segment Store failover after sleep", currentReadCount1, currentWriteCount1);
        //ensure writes are happening
        assertTrue(currentWriteCount1 > currentWriteCount2);
        //ensure reads are happening
        assertTrue(currentReadCount1 > currentReadCount2);

        //Scale down controller instances to 2
        Futures.getAndHandleExceptions(controllerInstance.scaleService(2), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down controller instances from 3 to 2");

        currentWriteCount2 = testState.getEventWrittenCount();
        currentReadCount2 = testState.getEventReadCount();
        log.info("Read count: {}, write count: {} after controller failover after sleep", currentReadCount2, currentWriteCount2);
        //ensure writes are happening
        assertTrue(currentWriteCount2 > currentWriteCount1);
        //ensure reads are happening
        assertTrue(currentReadCount2 > currentReadCount1);

        //Scale down segment  store, controller to 1 instance each.
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(1), ExecutionException::new);
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down to 1 controller, 1 Segment Store instance");

        currentWriteCount1 = testState.getEventWrittenCount();
        currentReadCount1 = testState.getEventReadCount();
        log.info("Stop write flag status: {}, stop read flag status: {} ", testState.stopWriteFlag.get(), testState.stopReadFlag.get());
        log.info("Read count: {}, write count: {} with Segment Store  and controller failover after sleep", currentReadCount1, currentWriteCount1);
    }

    void performFailoverForTestsInvolvingTxns() throws ExecutionException {
        log.info("Test with 3 controller, segment store instances running and without a failover scenario");
        log.info("Read count: {}, write count: {} without any failover",
                testState.getEventReadCount(), testState.getEventWrittenCount());

        //check reads and writes after sleeps
        log.info("Sleeping for {} ", WAIT_AFTER_FAILOVER_MILLIS);
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Read count: {}, write count: {} without any failover after sleep before scaling",
                testState.getEventReadCount(),  testState.getEventWrittenCount());

        //Scale down segment store instances to 2
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(2), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down Segment Store instances from 3 to 2");
        log.info("Read count: {}, write count: {} after Segment Store  failover after sleep",
                testState.getEventReadCount(),  testState.getEventWrittenCount());

        //Scale down controller instances to 2
        Futures.getAndHandleExceptions(controllerInstance.scaleService(2), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down controller instances from 3 to 2");
        log.info("Read count: {}, write count: {} after controller failover after sleep",
                testState.getEventReadCount(),  testState.getEventWrittenCount());

        //Scale down segment store, controller to 1 instance each.
        Futures.getAndHandleExceptions(segmentStoreInstance.scaleService(1), ExecutionException::new);
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);
        //zookeeper will take about 30 seconds to detect that the node has gone down
        Exceptions.handleInterrupted(() -> Thread.sleep(WAIT_AFTER_FAILOVER_MILLIS));
        log.info("Scaling down  to 1 controller, 1 Segment Store  instance");
        log.info("Stop write flag status: {}, stop read flag status: {} ",
                testState.stopWriteFlag.get(), testState.stopReadFlag.get());
        log.info("Read count: {}, write count: {} with Segment Store  and controller failover after sleep",
                testState.getEventReadCount(),  testState.getEventWrittenCount());
    }

    CompletableFuture<Void> startWriting(final EventStreamWriter<String> writer) {
        return CompletableFuture.runAsync(() -> {
            String uniqueRoutingKey = UUID.randomUUID().toString();
            long seqNumber = 0;
            while (!testState.stopWriteFlag.get()) {
                try {
                    Exceptions.handleInterrupted(() -> Thread.sleep(100));

                    // The content of events is generated following the pattern routingKey:seq_number, where
                    // seq_number is monotonically increasing for every routing key, being the expected delta between
                    // consecutive seq_number values always 1.
                    final String eventContent = uniqueRoutingKey + RK_VALUE_SEPARATOR + seqNumber;
                    log.debug("Event write count before write call {}", testState.getEventWrittenCount());
                    writer.writeEvent(uniqueRoutingKey, eventContent);
                    log.debug("Event write count before flush {}", testState.getEventWrittenCount());
                    writer.flush();
                    testState.incrementTotalWrittenEvents();
                    log.debug("Writing event {}", eventContent);
                    log.debug("Event write count {}", testState.getEventWrittenCount());
                    seqNumber++;

                    // Renewal of routing key to test writing in multiple segments for the same writer.
                    if (seqNumber == RK_RENEWAL_RATE_WRITER) {
                        log.info("Renew writer routing key and reinitialize seqNumber at event {}.", seqNumber);
                        uniqueRoutingKey = UUID.randomUUID().toString();
                        seqNumber = 0;
                    }
                } catch (Throwable e) {
                    log.error("Test exception in writing events: ", e);
                    testState.getWriteException.set(e);
                }
            }
            closeWriter(writer);
        }, executorService);
    }

    private void closeWriter(EventStreamWriter<String> writer) {
        try {
            log.info("Closing writer");
            writer.close();
        } catch (Throwable e) {
            log.error("Error while closing writer", e);
            testState.getWriteException.compareAndSet(null, e);
        }
    }

    void waitForTxnsToComplete() {
        log.info("Wait for txns to complete");
        if (!Futures.await(Futures.allOf(testState.txnStatusFutureList))) {
            log.error("Transaction futures did not complete with exceptions");
        }
        // check for exceptions during transaction commits
        if (testState.getTxnWriteException.get() != null) {
            log.info("Unable to commit transaction:", testState.getTxnWriteException.get());
            Assert.fail("Unable to commit transaction. Test failure");
        }
    }

    CompletableFuture<Void> startWritingIntoTxn(final EventStreamWriter<String> writer) {
        return CompletableFuture.runAsync(() -> {
            while (!testState.stopWriteFlag.get()) {
                Transaction<String> transaction = null;
                AtomicBoolean txnIsDone = new AtomicBoolean(false);

                try {
                    transaction = writer.beginTxn();
                    String uniqueRoutingKey = transaction.getTxnId().toString();
                    long seqNumber = 0;
                    for (int j = 1; j <= NUM_EVENTS_PER_TRANSACTION; j++) {
                        // The content of events is generated following the pattern routingKey:seq_number. In this case,
                        // the context of the routing key is the transaction.
                        transaction.writeEvent(uniqueRoutingKey, uniqueRoutingKey + RK_VALUE_SEPARATOR + seqNumber);
                        log.debug("Writing event: {} into transaction: {}", uniqueRoutingKey + RK_VALUE_SEPARATOR + seqNumber,
                                transaction.getTxnId());
                        seqNumber++;

                        // Renewal of routing key to test writing in multiple segments for the same transaction.
                        if (j % RK_RENEWAL_RATE_TRANSACTION == 0) {
                            log.info("Renew transaction writer routing key and reinitialize seqNumber at event {}.", j);
                            uniqueRoutingKey = UUID.randomUUID().toString();
                            seqNumber = 0;
                        }
                    }
                    //commit Txn
                    transaction.commit();
                    txnIsDone.set(true);

                    //wait for transaction to get committed
                    testState.txnStatusFutureList.add(checkTxnStatus(transaction, NUM_EVENTS_PER_TRANSACTION));
                } catch (Throwable e) {
                    // Given that we have retry logic both in the interaction with controller and
                    // segment store, we should fail the test case in the presence of any exception
                    // caught here.
                    txnIsDone.set(true);
                    log.warn("Exception while writing events in the transaction: ", e);
                    if (transaction != null) {
                        log.debug("Transaction with id: {}  failed", transaction.getTxnId());
                    }
                    testState.getTxnWriteException.set(e);
                    return;
                }
            }
            closeWriter(writer);
        }, executorService);
    }

    private CompletableFuture<Void> checkTxnStatus(Transaction<String> txn, int eventsWritten) {
        testState.committingTxn.add(txn.getTxnId());
        return Retry.indefinitelyWithExpBackoff("Txn did not get committed").runAsync(() -> {
            Transaction.Status status = txn.checkStatus();
            log.debug("Txn id {} status is {}", txn.getTxnId(), status);
            if (status.equals(Transaction.Status.COMMITTED)) {
                testState.incrementTotalWrittenEvents(eventsWritten);
                testState.committingTxn.remove(txn.getTxnId());
                log.info("Event write count: {}", testState.getEventWrittenCount());
            } else if (status.equals(Transaction.Status.ABORTED)) {
                log.debug("Transaction with id: {} aborted", txn.getTxnId());
                testState.abortedTxn.add(txn.getTxnId());
            } else {
                throw new TxnNotCompleteException();
            }

            return CompletableFuture.completedFuture(null);
        }, executorService);
    }

    CompletableFuture<Void> startReading(final EventStreamReader<String> reader) {
        return CompletableFuture.runAsync(() -> {
            log.info("Exit flag status: {}, Read count: {}, Write count: {}", testState.stopReadFlag.get(),
                    testState.getEventReadCount(), testState.getEventWrittenCount());
            final Map<String, Long> routingKeySeqNumber = new HashMap<>();
            while (!(testState.stopReadFlag.get() && testState.getEventReadCount() == testState.getEventWrittenCount())) {
                log.info("Entering read loop");
                // Exit only if exitFlag is true  and read Count equals write count.
                try {
                    final String event = reader.readNextEvent(SECONDS.toMillis(5)).getEvent();
                    log.debug("Reading event {}", event);
                    if (event != null) {
                        // For every new event read, the reader asserts that its value is equal to the existing
                        // seq_number + 1. This ensures that readers receive events in the same order that writers
                        // produced them and that there are no duplicate or missing events.
                        final String[] keyAndSeqNum = event.split(RK_VALUE_SEPARATOR);
                        final long seqNumber = Long.valueOf(keyAndSeqNum[1]);
                        routingKeySeqNumber.compute(keyAndSeqNum[0], (rk, currentSeqNum) -> {
                            if (currentSeqNum != null && currentSeqNum + 1 != seqNumber) {
                                throw new AssertionError("Event order violated at " + currentSeqNum + " by " + seqNumber);
                            }
                            return seqNumber;
                        });
                        testState.incrementTotalReadEvents();
                        log.debug("Event read count {}", testState.getEventReadCount());
                    } else {
                        log.debug("Read timeout");
                    }
                } catch (Throwable e) {
                    log.error("Test exception in reading events: ", e);
                    testState.getReadException.set(e);
                }
            }
            log.info("Completed reading");
            closeReader(reader);
        }, executorService);
    }

    private void closeReader(EventStreamReader<String> reader) {
        try {
            log.info("Closing reader");
            reader.close();
        } catch (Throwable e) {
            log.error("Error while closing reader", e);
            testState.getReadException.compareAndSet(null, e);
        }
    }

    void cleanUp(String scope, String stream, ReaderGroupManager readerGroupManager, String readerGroupName) throws InterruptedException, ExecutionException {
        CompletableFuture<Boolean> sealStreamStatus = Retry.indefinitelyWithExpBackoff("Failed to seal stream. retrying ...")
                .runAsync(() -> controller.sealStream(scope, stream), executorService);
        log.info("Sealing stream {}", stream);
        assertTrue(sealStreamStatus.get());
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, stream);
        log.info("Deleting stream {}", stream);
        assertTrue(deleteStreamStatus.get());
        log.info("Deleting readergroup {}", readerGroupName);
        readerGroupManager.deleteReaderGroup(readerGroupName);
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
        log.info("Deleting scope {}", scope);
        assertTrue(deleteScopeStatus.get());
    }

    void createScopeAndStream(String scope, String stream, StreamConfiguration config, StreamManager streamManager) {
        Boolean createScopeStatus = streamManager.createScope(scope);
        log.info("Creating scope with scope name {}", scope);
        log.debug("Create scope status {}", createScopeStatus);
        Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
        log.debug("Create stream status {}", createStreamStatus);
    }

    void createWriters(ClientFactory clientFactory, final int writers, String scope, String stream) {
        Preconditions.checkNotNull(testState.writersListComplete.get(0));
        log.info("Client factory details {}", clientFactory.toString());
        log.info("Creating {} writers", writers);
        List<EventStreamWriter<String>> writerList = new ArrayList<>(writers);
        List<CompletableFuture<Void>> writerFutureList = new ArrayList<>();
        log.info("Writers writing in the scope {}", scope);
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < writers; i++) {
                log.info("Starting writer{}", i);

                if (!testState.txnWrite) {
                    final EventStreamWriter<String> tmpWriter = clientFactory.createEventWriter(stream,
                            new JavaSerializer<>(),
                            EventWriterConfig.builder().maxBackoffMillis(WRITER_MAX_BACKOFF_MILLIS)
                                    .retryAttempts(WRITER_MAX_RETRY_ATTEMPTS).build());
                    writerList.add(tmpWriter);
                    final CompletableFuture<Void> writerFuture = startWriting(tmpWriter);
                    Futures.exceptionListener(writerFuture, t -> log.error("Error while writing events:", t));
                    writerFutureList.add(writerFuture);
                } else  {
                    final EventStreamWriter<String> tmpWriter = clientFactory.createEventWriter(stream,
                            new JavaSerializer<>(),
                            EventWriterConfig.builder().maxBackoffMillis(WRITER_MAX_BACKOFF_MILLIS)
                                    .retryAttempts(WRITER_MAX_RETRY_ATTEMPTS)
                                    .transactionTimeoutTime(59000).build());
                    writerList.add(tmpWriter);
                    final CompletableFuture<Void> txnWriteFuture = startWritingIntoTxn(tmpWriter);
                    Futures.exceptionListener(txnWriteFuture, t -> log.error("Error while writing events into transaction:", t));
                    writerFutureList.add(txnWriteFuture);
                }

            }
        }).thenRun(() -> {
            testState.writers.addAll(writerFutureList);
            Futures.completeAfter(() -> Futures.allOf(writerFutureList),
                    testState.writersListComplete.get(0));
            Futures.exceptionListener(testState.writersListComplete.get(0),
                    t -> log.error("Exception while waiting for writers to complete", t));
        });
    }

    void createReaders(ClientFactory clientFactory, String readerGroupName, String scope,
                                 ReaderGroupManager readerGroupManager, String stream, final int readers) {
        log.info("Creating Reader group: {}, with readergroup manager using scope: {}", readerGroupName, scope);
        readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build());
        log.info("Reader group name: {}, Reader group scope: {}, Online readers: {}",
                readerGroupManager.getReaderGroup(readerGroupName).getGroupName(), readerGroupManager
                        .getReaderGroup(readerGroupName).getScope(), readerGroupManager
                        .getReaderGroup(readerGroupName).getOnlineReaders());
        log.info("Creating {} readers", readers);
        List<EventStreamReader<String>> readerList = new ArrayList<>(readers);
        List<CompletableFuture<Void>> readerFutureList = new ArrayList<>();
        log.info("Scope that is seen by readers {}", scope);

        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < readers; i++) {
                log.info("Starting reader: {}, with id: {}", i, readerName + i);
                final EventStreamReader<String> reader = clientFactory.createReader(readerName + i,
                        readerGroupName,
                        new JavaSerializer<>(),
                        ReaderConfig.builder().build());
                readerList.add(reader);
                final CompletableFuture<Void> readerFuture = startReading(reader);
                Futures.exceptionListener(readerFuture, t -> log.error("Error while reading events:", t));
                readerFutureList.add(readerFuture);
            }
        }).thenRun(() -> {
            testState.readers.addAll(readerFutureList);
            Futures.completeAfter(() -> Futures.allOf(readerFutureList), testState.readersComplete);
            Futures.exceptionListener(testState.readersComplete,
                    t -> log.error("Exception while waiting for all readers to complete", t));
        });
    }

    void addNewWriters(ClientFactory clientFactory, final int writers, String scope, String stream) {
        Preconditions.checkNotNull(testState.writersListComplete.get(1));
        log.info("Client factory details {}", clientFactory.toString());
        log.info("Creating {} writers", writers);
        List<EventStreamWriter<String>> newlyAddedWriterList = new ArrayList<>();
        List<CompletableFuture<Void>> newWritersFutureList = new ArrayList<>();
        log.info("Writers writing in the scope {}", scope);
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < writers; i++) {
                log.info("Starting writer{}", i);
                if (!testState.txnWrite) {
                    final EventStreamWriter<String> tmpWriter = clientFactory.createEventWriter(stream,
                            new JavaSerializer<>(),
                            EventWriterConfig.builder().maxBackoffMillis(WRITER_MAX_BACKOFF_MILLIS)
                                    .retryAttempts(WRITER_MAX_RETRY_ATTEMPTS).build());
                    newlyAddedWriterList.add(tmpWriter);
                    final CompletableFuture<Void> writerFuture = startWriting(tmpWriter);
                    Futures.exceptionListener(writerFuture, t -> log.error("Error while writing events:", t));
                    newWritersFutureList.add(writerFuture);
                } else  {
                    final EventStreamWriter<String> tmpWriter = clientFactory.createEventWriter(stream,
                            new JavaSerializer<>(),
                            EventWriterConfig.builder().maxBackoffMillis(WRITER_MAX_BACKOFF_MILLIS)
                                    .retryAttempts(WRITER_MAX_RETRY_ATTEMPTS)
                                    .transactionTimeoutTime(59000).build());
                    newlyAddedWriterList.add(tmpWriter);
                    final CompletableFuture<Void> txnWriteFuture = startWritingIntoTxn(tmpWriter);
                    Futures.exceptionListener(txnWriteFuture, t -> log.error("Error while writing events into transaction:", t));
                    newWritersFutureList.add(txnWriteFuture);
                }
            }
        }).thenRun(() -> {
            testState.writers.addAll(newWritersFutureList);
            Futures.completeAfter(() -> Futures.allOf(newWritersFutureList), testState.writersListComplete.get(1));
            Futures.exceptionListener(testState.writersListComplete.get(1),
                    t -> log.error("Exception while waiting for writers to complete", t));
        });
    }

    void stopWriters() {
        //Stop Writers
        log.info("Stop write flag status {}", testState.stopWriteFlag);
        testState.stopWriteFlag.set(true);

        log.info("Wait for writers execution to complete");
        if (!Futures.await(Futures.allOf(testState.writersListComplete))) {
            log.error("Writers stopped with exceptions");
        }

        // check for exceptions during writes
        if (testState.getWriteException.get() != null) {
            log.info("Unable to write events:", testState.getWriteException.get());
            Assert.fail("Unable to write events. Test failure");
        }
    }

    void stopReaders() {
        //Stop Readers
        log.info("Stop read flag status {}", testState.stopReadFlag);
        testState.stopReadFlag.set(true);

        log.info("Wait for readers execution to complete");
        if (!Futures.await(testState.readersComplete)) {
            log.error("Readers stopped with exceptions");
        }
        //check for exceptions during read
        if (testState.getReadException.get() != null) {
            log.info("Unable to read events:", testState.getReadException.get());
            Assert.fail("Unable to read events. Test failure");
        }
    }

    void validateResults() {
        log.info("All writers and readers have stopped. Event Written Count:{}, Event Read " +
                "Count: {}", testState.getEventWrittenCount(), testState.getEventReadCount());
        assertEquals(testState.getEventWrittenCount(), testState.getEventReadCount());
    }

    void waitForScaling(String scope, String stream, StreamConfiguration initialConfig) {
        int initialMaxSegmentNumber = initialConfig.getScalingPolicy().getMinNumSegments() - 1;
        boolean scaled = false;
        for (int waitCounter = 0; waitCounter < SCALE_WAIT_ITERATIONS; waitCounter++) {
            StreamSegments streamSegments = controller.getCurrentSegments(scope, stream).join();
            if (streamSegments.getSegments().stream().mapToLong(Segment::getSegmentId).max().orElse(-1) > initialMaxSegmentNumber) {
                scaled = true;
                break;
            }
            //Scaling operation did not happen, wait
            Exceptions.handleInterrupted(() -> Thread.sleep(10000));
        }

        assertTrue("Scaling did not happen within desired time", scaled);
    }

    static URI startZookeeperInstance() {
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }
        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        return zkUris.get(0);
    }

    static void startBookkeeperInstances(final URI zkUri) {
        Service bkService = Utils.createBookkeeperService(zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }
        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);
    }

    static URI startPravegaControllerInstances(final URI zkUri) throws ExecutionException {
        Service controllerService = Utils.createPravegaControllerService(zkUri);
        if (!controllerService.isRunning()) {
            controllerService.start(true);
        }
        Futures.getAndHandleExceptions(controllerService.scaleService(3), ExecutionException::new);
        List<URI> conUris = controllerService.getServiceDetails();
        log.info("conuris {} {}", conUris.get(0), conUris.get(1));
        log.debug("Pravega Controller service  details: {}", conUris);
        // Fetch all the RPC endpoints and construct the client URIs.
        final List<String> uris = conUris.stream().filter(uri -> Utils.DOCKER_BASED ? uri.getPort() == Utils.DOCKER_CONTROLLER_PORT
                : uri.getPort() == Utils.MARATHON_CONTROLLER_PORT).map(URI::getAuthority)
                .collect(Collectors.toList());

        URI controllerURI = URI.create("tcp://" + String.join(",", uris));
        log.info("Controller Service direct URI: {}", controllerURI);
        return controllerURI;
    }

    static void startPravegaSegmentStoreInstances(final URI zkUri, final URI controllerURI) throws ExecutionException {
        Service segService = Utils.createPravegaSegmentStoreService(zkUri, controllerURI);
        if (!segService.isRunning()) {
            segService.start(true);
        }
        Futures.getAndHandleExceptions(segService.scaleService(3), ExecutionException::new);
        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega Segmentstore service  details: {}", segUris);
    }

    static class TxnNotCompleteException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

}
