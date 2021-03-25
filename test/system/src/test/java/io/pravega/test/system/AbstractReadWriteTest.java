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

import com.google.common.base.Preconditions;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Abstract class that provides convenient event read/write methods for testing purposes.
 */
@Slf4j
abstract class AbstractReadWriteTest extends AbstractSystemTest {

    static final String RK_VALUE_SEPARATOR = ":";
    static final int WRITER_MAX_BACKOFF_MILLIS = 5 * 1000;
    static final int WRITER_MAX_RETRY_ATTEMPTS = 30;
    static final int NUM_EVENTS_PER_TRANSACTION = 50;
    static final int RK_RENEWAL_RATE_TRANSACTION = NUM_EVENTS_PER_TRANSACTION / 2;
    static final int TRANSACTION_TIMEOUT = 119 * 1000;
    static final int RK_RENEWAL_RATE_WRITER = 500;
    static final int SCALE_WAIT_ITERATIONS = 12;
    private static final int READ_TIMEOUT = 10000;
    private static final int WRITE_THROTTLING_TIME = 100;

    final String readerName = "reader";
    ScheduledExecutorService executorService;
    TestState testState;

    /**
     * This class encapsulates the information regarding the execution of a system test. This includes the references to
     * readers and writers, the number of events read/written, the exceptions occurred on readers/writers through the
     * test's execution, and the flags to stop the execution of readers/writers. It also contains logic to perform some
     * validation logic on the events processed.
     */
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
        final ConcurrentSkipListSet<UUID> committingTxn = new ConcurrentSkipListSet<>();
        final ConcurrentSkipListSet<UUID> abortedTxn = new ConcurrentSkipListSet<>();
        final boolean txnWrite;

        final AtomicLong writtenEvents = new AtomicLong();
        final AtomicLong readEvents = new AtomicLong();
        // Due to segment rebalances across readers, any reader may read arbitrary sub-sequences of events for various
        // routing keys. This calls for a global state across readers to check the correctness of event sequences.
        final Map<String, Long> routingKeySeqNumber = new ConcurrentHashMap<>();

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

    CompletableFuture<Void> startWriting(final EventStreamWriter<String> writer) {
        return startWriting(writer, testState.stopWriteFlag);
    }

    CompletableFuture<Void> startWriting(final EventStreamWriter<String> writer, AtomicBoolean stopFlag) {
        return CompletableFuture.runAsync(() -> {
            String uniqueRoutingKey = UUID.randomUUID().toString();
            long seqNumber = 0;
            while (!stopFlag.get()) {
                try {
                    Exceptions.handleInterrupted(() -> Thread.sleep(WRITE_THROTTLING_TIME));

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
                    break;
                }
            }
            log.info("Completed writing");
            closeWriter(writer);
        }, executorService);
    }

    CompletableFuture<Void> startWritingIntoTxn(final TransactionalEventStreamWriter<String> writer) {
        return startWritingIntoTxn(writer, testState.stopWriteFlag);
    }

    CompletableFuture<Void> startWritingIntoTxn(final TransactionalEventStreamWriter<String> writer, AtomicBoolean stopFlag) {
        return CompletableFuture.runAsync(() -> {
            while (!stopFlag.get()) {
                Transaction<String> transaction = null;

                try {
                    transaction = writer.beginTxn();
                    String uniqueRoutingKey = transaction.getTxnId().toString();
                    long seqNumber = 0;
                    for (int j = 1; j <= NUM_EVENTS_PER_TRANSACTION; j++) {
                        Exceptions.handleInterrupted(() -> Thread.sleep(WRITE_THROTTLING_TIME));
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

                    //wait for transaction to get committed
                    testState.txnStatusFutureList.add(checkTxnStatus(transaction, NUM_EVENTS_PER_TRANSACTION));
                } catch (Throwable e) {
                    // Given that we have retry logic both in the interaction with controller and
                    // segment store, we should fail the test case in the presence of any exception
                    // caught here.
                    log.warn("Exception while writing events in the transaction: ", e);
                    if (transaction != null) {
                        log.debug("Transaction with id: {}  failed", transaction.getTxnId());
                    }
                    testState.getTxnWriteException.set(e);
                    return;
                }
            }
            log.info("Completed writing into txn");
            closeWriter(writer);
        }, executorService);
    }

    CompletableFuture<Void> startReading(final EventStreamReader<String> reader) {
        return startReading(reader, testState.stopReadFlag);
    }

    CompletableFuture<Void> startReading(final EventStreamReader<String> reader, AtomicBoolean stopFlag) {
        return CompletableFuture.runAsync(() -> {
            log.info("Exit flag status: {}, Read count: {}, Write count: {}", testState.stopReadFlag.get(),
                    testState.getEventReadCount(), testState.getEventWrittenCount());
            while (!(stopFlag.get() && testState.getEventReadCount() == testState.getEventWrittenCount())) {
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
                        final long seqNumber = Long.parseLong(keyAndSeqNum[1]);
                        testState.routingKeySeqNumber.compute(keyAndSeqNum[0], (rk, currentSeqNum) -> {
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
                    break;
                }
            }
            log.info("Completed reading");
            closeReader(reader);
        }, executorService);
    }

    void createReaders(EventStreamClientFactory clientFactory, String readerGroupName, String scope,
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
        }, executorService).thenRun(() -> {
            testState.readers.addAll(readerFutureList);
            Futures.completeAfter(() -> Futures.allOf(readerFutureList), testState.readersComplete);
            Futures.exceptionListener(testState.readersComplete,
                    t -> log.error("Exception while waiting for all readers to complete", t));
        });
    }

    void createWriters(EventStreamClientFactory clientFactory, final int writers, String scope, String stream) {
        createWritersInternal(clientFactory, writers, scope, stream, testState.writersComplete, testState.txnWrite);
    }

    void addNewWriters(EventStreamClientFactory clientFactory, final int writers, String scope, String stream) {
        Preconditions.checkNotNull(testState.writersListComplete.get(0));
        createWritersInternal(clientFactory, writers, scope, stream, testState.newWritersComplete, testState.txnWrite);
    }

    void waitForTxnsToComplete() {
        log.info("Wait for txns to complete");
        if (!Futures.await(Futures.allOf(testState.txnStatusFutureList))) {
            log.error("Transaction futures did not complete with exceptions");
        }
        // check for exceptions during transaction commits
        if (testState.getTxnWriteException.get() != null) {
            log.info("Unable to commit transaction:", testState.getTxnWriteException.get());
            fail("Unable to commit transaction. Test failure");
        }
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
            fail("Unable to write events. Test failure");
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
            fail("Unable to read events. Test failure");
        }
    }

    void validateResults() {
        log.info("All writers and readers have stopped. Event Written Count:{}, Event Read " +
                "Count: {}", testState.getEventWrittenCount(), testState.getEventReadCount());
        assertEquals(testState.getEventWrittenCount(), testState.getEventReadCount());
    }

    void writeEvents(EventStreamClientFactory clientFactory, String streamName, int totalEvents) {
        writeEvents(clientFactory, streamName, totalEvents, 0);
    }

    void writeEvents(EventStreamClientFactory clientFactory, String streamName, int totalEvents, int initialPoint) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = initialPoint; i < totalEvents + initialPoint; i++) {
            writer.writeEvent(String.format("%03d", i)).join(); // this ensures the event size is constant.
            log.debug("Writing event: {} to stream {}.", streamName + i, streamName);
        }
        log.info("Writer {} finished writing {} events.", writer, totalEvents - initialPoint);
    }

    <T extends Serializable> List<CompletableFuture<Integer>> readEventFutures(EventStreamClientFactory client, String rGroup, int numReaders, int limit) {
        List<EventStreamReader<T>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(client.createReader(rGroup + "-" + i, rGroup, new JavaSerializer<>(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() -> readEvents(r, limit / numReaders))).collect(toList());
    }

    List<CompletableFuture<Integer>> readEventFutures(EventStreamClientFactory clientFactory, String readerGroup, int numReaders) {
        return readEventFutures(clientFactory, readerGroup, numReaders, Integer.MAX_VALUE);
    }

    // Private methods region

    private void createWritersInternal(EventStreamClientFactory clientFactory, final int writers, String scope, String stream,
                                       CompletableFuture<Void> writersComplete, boolean isTransactionalWriter) {
        testState.writersListComplete.add(writersComplete);
        log.info("Client factory details {}", clientFactory.toString());
        log.info("Creating {} writers", writers);
        List<CompletableFuture<Void>> writerFutureList = new ArrayList<>();
        log.info("Writers writing in the scope {}", scope);
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < writers; i++) {
                log.info("Starting writer: {} (is transactional? {})", i, isTransactionalWriter);
                final CompletableFuture<Void> writerFuture = isTransactionalWriter ?
                        startWritingIntoTxn(instantiateTransactionalWriter("writer-" + i, clientFactory, stream)) :
                        startWriting(instantiateWriter(clientFactory, stream));
                Futures.exceptionListener(writerFuture, t -> log.error("Error while writing events:", t));
                writerFutureList.add(writerFuture);
            }
        }, executorService).thenRun(() -> {
            testState.writers.addAll(writerFutureList);
            Futures.completeAfter(() -> Futures.allOf(writerFutureList), writersComplete);
            Futures.exceptionListener(writersComplete, t -> log.error("Exception while waiting for writers to complete", t));
        });
    }

    private <T extends Serializable> EventStreamWriter<T> instantiateWriter(EventStreamClientFactory clientFactory, String stream) {
        return clientFactory.createEventWriter(stream, new JavaSerializer<>(), buildWriterConfig());
    }

    private <T extends Serializable> TransactionalEventStreamWriter<T> instantiateTransactionalWriter(String writerId,
                                                                                                      EventStreamClientFactory clientFactory,
                                                                                                      String stream) {
        return clientFactory.createTransactionalEventWriter(writerId, stream, new JavaSerializer<T>(), buildWriterConfig());
    }

    private EventWriterConfig buildWriterConfig() {
        return EventWriterConfig.builder()
                                .maxBackoffMillis(WRITER_MAX_BACKOFF_MILLIS)
                                .retryAttempts(WRITER_MAX_RETRY_ATTEMPTS)
                                .transactionTimeoutTime(TRANSACTION_TIMEOUT)
                                .build();
    }

    private <T> void closeWriter(EventStreamWriter<T> writer) {
        try {
            log.info("Closing writer");
            writer.close();
        } catch (Throwable e) {
            log.error("Error while closing writer", e);
            testState.getWriteException.compareAndSet(null, e);
        }
    }
    
    private <T> void closeWriter(TransactionalEventStreamWriter<T> writer) {
        try {
            log.info("Closing writer");
            writer.close();
        } catch (Throwable e) {
            log.error("Error while closing writer", e);
            testState.getWriteException.compareAndSet(null, e);
        }
    }

    protected <T> void closeReader(EventStreamReader<T> reader) {
        try {
            log.info("Closing reader");
            reader.close();
        } catch (Throwable e) {
            log.error("Error while closing reader", e);
            testState.getReadException.compareAndSet(null, e);
        }
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

    private <T> int readEvents(EventStreamReader<T> reader, int limit) {
        return readEvents(reader, limit, false);
    }

    private <T> int readEvents(EventStreamReader<T> reader, int limit, boolean reinitializationExpected) {
        EventRead<T> event = null;
        int validEvents = 0;
        boolean reinitializationRequired = false;
        try {
            do {
                try {
                    event = reader.readNextEvent(READ_TIMEOUT);
                    log.debug("Read event result in readEvents: {}.", event.getEvent());
                    if (event.getEvent() != null) {
                        validEvents++;
                    }
                    reinitializationRequired = false;
                } catch (ReinitializationRequiredException e) {
                    log.error("Exception while reading event using readerId: {}", reader, e);
                    if (reinitializationExpected) {
                        reinitializationRequired = true;
                    } else {
                        fail("Reinitialization Exception is not expected");
                    }
                }
            } while (reinitializationRequired || ((event.getEvent() != null || event.isCheckpoint()) && validEvents < limit));
        } finally {
            closeReader(reader);
        }
        log.info("Reader {} finished reading {} events (limit was {}).", reader, validEvents, limit);
        return validEvents;
    }

    static class TxnNotCompleteException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
