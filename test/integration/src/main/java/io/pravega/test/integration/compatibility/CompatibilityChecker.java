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
package io.pravega.test.integration.compatibility;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.Insert;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Put;
import io.pravega.client.tables.Remove;
import io.pravega.client.tables.TableKey;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * CompatibilityChecker class is exercising all APIs we have in Pravega Samples.
 * This class can be used for compatibility check against a server endpoint passed by parameter.
 */
@Slf4j
public class CompatibilityChecker {
    private static final int READER_TIMEOUT_MS = 2000;
    private final static int NUM_EVENTS = 10;
    private static final int TEST_MAX_STREAMS = 10;
    private static final int TEST_MAX_KEYS = 10;
    private static final PaddedStringSerializer USERNAME_SERIALIZER = new PaddedStringSerializer(64);
    private static final UTF8StringSerializer SERIALIZER = new UTF8StringSerializer();
    private URI controllerURI;
    private StreamManager streamManager;
    private StreamConfiguration streamConfig;
    private KeyValueTableManager keyValueTableManager;
    private ScheduledExecutorService executor;

    private void setUp(String uri) {
        controllerURI = URI.create(uri);
        streamManager = StreamManager.create(controllerURI);
        streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        keyValueTableManager = KeyValueTableManager.create(controllerURI);
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "compatibility-test");
    }

   private void createScopeAndStream(String scopeName, String streamName) {
       streamManager.createScope(scopeName);
       streamManager.createStream(scopeName, streamName, streamConfig);
   }

   private String getRandomID() {
       return UUID.randomUUID().toString().replace("-", "");
   }
   
    /**
    * This method is Checking the working of the read and write event of the stream.
    * Here we are trying to create a stream and a scope, and then we are writing a couple of events.
    * And then reading those written event from the stream.
    */
    @Test(timeout = 20000)
    private void checkWriteAndReadEvent() {
        String scopeName = "write-and-read-test-scope";
        String streamName = "write-and-read-test-stream";
        createScopeAndStream(scopeName, streamName);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        String readerGroupId = getRandomID();
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<String> reader =  clientFactory.createReader("reader-1", readerGroupId, new UTF8StringSerializer(), ReaderConfig.builder().build());
        int writeCount = 0;
        // Writing 10 Events to the stream
        for (int event = 1; event <= 10; event++) {
            writer.writeEvent("event test" + event);
            writeCount++;
        }
        log.info("Reading all the events from {}, {}", scopeName, streamName);
        int eventNumber = 1;
        int readCount = 0;
        EventRead<String> event = reader.readNextEvent(READER_TIMEOUT_MS);
        // Reading written Events
        while (event.getEvent() != null) {
            assertEquals("event test" + eventNumber, event.getEvent());
            event = reader.readNextEvent(READER_TIMEOUT_MS);
            readCount++;
            eventNumber++;
        }
        // Validating the readCount and writeCount
        assertEquals(readCount, writeCount);
        log.info("No more events from {}, {}", scopeName, streamName);
    }

    /**
    * This method is checking the Truncation feature of the stream.
    * Here we are creating some stream and writing events to that stream.
    * After that we are truncating to that stream and validating whether the stream truncation is working properly.
    */
    @Test(timeout = 20000)
    private void checkTruncationOfStream() {
        String scopeName = "truncate-test-scope";
        String streamName = "truncate-test-stream";
        createScopeAndStream(scopeName, streamName);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());

        String readerGroupId = getRandomID();

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupId);

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        for (int event = 0; event < 2; event++) {
            writer.writeEvent(String.valueOf(event)).join();
        }
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerGroupId + "1", readerGroupId,
                new UTF8StringSerializer(), ReaderConfig.builder().build());
        assertEquals(reader.readNextEvent(5000).getEvent(), "0");
        reader.close();

        // Get StreamCut and truncate the Stream at that point.
        StreamCut streamCut = readerGroup.getStreamCuts().get(Stream.of(scopeName, streamName));
        assertTrue(streamManager.truncateStream(scopeName, streamName, streamCut));

        // Verify that a new reader reads from event 1 onwards.
        final String newReaderGroupName = readerGroupId + "new";
        readerGroupManager.createReaderGroup(newReaderGroupName, ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build());
        @Cleanup
        final EventStreamReader<String> newReader = clientFactory.createReader(newReaderGroupName + "2",
                newReaderGroupName, new UTF8StringSerializer(), ReaderConfig.builder().build());

        assertEquals("Expected read event: ", "1", newReader.readNextEvent(5000).getEvent());
    }

    /**
    * This method is trying to check the stream seal feature of the stream.
    * Here we are creating a stream and writing some event to it.
    * And then sealing that stream by using stream manager and validating the stream whether sealing worked properly.
    */
    @Test(timeout = 20000)
    private void checkSealStream() {
        String scopeName = "stream-seal-test-scope";
        String streamName = "stream-seal-test-stream";
        createScopeAndStream(scopeName, streamName);
        assertTrue(streamManager.checkStreamExists(scopeName, streamName));
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        String readerGroupId = getRandomID();

        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerGroupId + "1", readerGroupId,
                new UTF8StringSerializer(), ReaderConfig.builder().build());
        // Before sealing of the stream it must write to the existing stream.
        writer.writeEvent("0").join();
        assertTrue(streamManager.sealStream(scopeName, streamName));
        // It must complete Exceptionally because stream is sealed already.
        assertTrue(writer.writeEvent("1").completeExceptionally(new IllegalStateException()));
        // But there should not be any problem while reading from a sealed stream.
        assertEquals(reader.readNextEvent(READER_TIMEOUT_MS).getEvent(), "0");
    }

    /**
     * This method attempts to create, delete and validate the existence of a scope.
     * @throws DeleteScopeFailedException if unable to seal and delete a stream.
     */
    @Test(timeout = 20000)
    private void checkDeleteScope() throws DeleteScopeFailedException {
        String scopeName = "scope-delete-test-scope";
        String streamName = "scope-delete-test-stream";
        createScopeAndStream(scopeName, streamName);
        assertTrue(streamManager.checkScopeExists(scopeName));

        assertTrue(streamManager.deleteScopeRecursive(scopeName));

        //validating whether the scope deletion is successful, expression must be false.
        assertFalse(streamManager.checkScopeExists(scopeName));
    }

    /**
     * This method is trying to create a stream and writing a couple of events.
     * Then deleting the newly created stream and validating it.
     */
    @Test(timeout = 20000)
    private void checkStreamDelete() {
        String scopeName = "stream-delete-test-scope";
        String streamName = "stream-delete-test-stream";
        createScopeAndStream(scopeName, streamName);
        assertTrue(streamManager.checkStreamExists(scopeName, streamName));
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(), EventWriterConfig.builder().build());
        // Before sealing of the stream it must write to the existing stream.
        writer.writeEvent("0").join();
        assertTrue(streamManager.sealStream(scopeName, streamName));

        assertTrue(streamManager.deleteStream(scopeName, streamName));
        assertFalse(streamManager.checkStreamExists(scopeName, streamName));
    }

    /**
     * This method creates a scope and streams, labeling the streams with a set of tags.
     * Then listing streams and validating the existing stream tags.
     */
    @Test(timeout = 20000)
    private  void checkStreamTags() {
        String scope = "stream-tags-scope";
        streamManager.createScope(scope);
        final ImmutableSet<String> tagSet1 = ImmutableSet.of("t1", "t2", "t3");
        final ImmutableSet<String> tagSet2 = ImmutableSet.of("t3", "t4", "t5");
        // Create and Update Streams
        for (int j = 1; j <= TEST_MAX_STREAMS; j++) {
            StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(j)).build();
            final String stream = "stream" + j;
            log.info("creating a new stream in scope {}/{}", stream, scope);
            streamManager.createStream(scope, stream, config);
            log.info("updating the stream in scope {}/{}", stream, scope);
            streamManager.updateStream(scope, stream, config.toBuilder().tags(tagSet1).build());
            assertEquals(tagSet1, streamManager.getStreamTags(scope, stream));
        }
        // Check the size of streams with tagName t1
        assertEquals(TEST_MAX_STREAMS, newArrayList(streamManager.listStreams(scope, "t1")).size());
        // Check if the lists of tag t3 and t1 are equal
        assertEquals(newArrayList(streamManager.listStreams(scope, "t3")), newArrayList(streamManager.listStreams(scope, "t1")));

        // Update the streams with new tagSet
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int j = 1; j <= TEST_MAX_STREAMS; j++) {
            StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(j)).build();
            final String stream = "stream" + j;
            log.info("updating the stream tag scope {}/{}", stream, scope);
            futures.add(CompletableFuture.runAsync(() -> streamManager.updateStream(scope, stream, config.toBuilder().clearTags().tags(tagSet2).build())));
        }
        assertEquals(TEST_MAX_STREAMS, futures.size());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        // Check if the update was successfully done
        assertTrue(newArrayList(streamManager.listStreams(scope, "t1")).isEmpty());
        assertEquals(TEST_MAX_STREAMS, newArrayList(streamManager.listStreams(scope, "t4")).size());
        final int tagT3Size = newArrayList(streamManager.listStreams(scope, "t3")).size();
        final int tagT4Size = newArrayList(streamManager.listStreams(scope, "t4")).size();
        log.info("list size of t3 tags and t4 are {}/{}", tagT3Size, tagT4Size);
        assertEquals(tagT3Size, tagT4Size);

        // seal and delete stream
        for (int j = 1; j <= TEST_MAX_STREAMS; j++) {
            final String stream = "stream" + j;
            streamManager.sealStream(scope, stream);
            log.info("deleting the stream in scope {}/{}", stream, scope);
            streamManager.deleteStream(scope, stream);
        }
        // Check if list streams is updated.
        assertTrue(newArrayList(streamManager.listStreams(scope)).isEmpty());
    }

    /**
     * This method attempts to validate abort functionality after creating a transaction.
     * @throws TxnFailedException if unable to commit or write to the transaction.
     */
    @Test(timeout = 20000)
    private void checkTransactionAbort() throws TxnFailedException {
        String scopeName = "transaction-abort-test-scope";
        String streamName = "transaction-abort-test-stream";
        createScopeAndStream(scopeName, streamName);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        TransactionalEventStreamWriter<String> writerTxn = clientFactory.createTransactionalEventWriter(streamName, new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        // Begin a transaction
        Transaction<String> txn = writerTxn.beginTxn();
        assertNotNull(txn.getTxnId());
        // Writing to the transaction
        txn.writeEvent("event test");
        // Checking and validating the Transaction status.
        assertEquals(Transaction.Status.OPEN, txn.checkStatus());
        // Aborting the transaction
        txn.abort();
        // It must fail if we are going to write to an aborted or aborting transaction.
        assertThrows(TxnFailedException.class, () -> txn.writeEvent("event test"));
        String readerGroupId = getRandomID();
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<String> reader =  clientFactory.createReader("reader-1", readerGroupId, new UTF8StringSerializer(), ReaderConfig.builder().build());
        // It should not read an event from the stream because transaction was aborted.
        EventRead<String>  eventRead =  reader.readNextEvent(READER_TIMEOUT_MS);
        assertTrue(eventRead.getEvent() == null);
        assertEquals(Transaction.Status.ABORTED, txn.checkStatus());
    }

    /**
     * This method tests the ability to successfully commit a transaction, including writing and reading events from a stream.
     * @throws TxnFailedException if unable to commit or write to the transaction.
     */
    @Test(timeout = 20000)
    private void checkTransactionReadAndWrite() throws TxnFailedException {
        String scopeName = "transaction-test-scope";
        String streamName = "transaction-test-stream";
        createScopeAndStream(scopeName, streamName);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        TransactionalEventStreamWriter<String> writerTxn = clientFactory.createTransactionalEventWriter(streamName, new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        assertNotNull(writerTxn);
        // Begin a transaction.
        Transaction<String> txn = writerTxn.beginTxn();
        assertNotNull(txn.getTxnId());
        int writeCount = 0;
        // Writing 10 Events to the transaction
        for (int event = 1; event <= 10; event++) {
            txn.writeEvent("event test" + event);
            writeCount++;
        }
        // Checking Status of transaction.
        assertEquals(Transaction.Status.OPEN, txn.checkStatus());
        // Committing the transaction.
        txn.commit();

        String readerGroupId = getRandomID();
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader-1", readerGroupId, new UTF8StringSerializer(), ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(READER_TIMEOUT_MS);
        int readCount = 0;
        // Reading events committed by the transaction.
        while (event.getEvent() != null) {
            readCount++;
            assertEquals("event test" + readCount, event.getEvent());
            event = reader.readNextEvent(READER_TIMEOUT_MS);
        }
        // Validating the readCount and writeCount of event which was written by transaction.
        assertEquals(readCount, writeCount);
        assertEquals( Transaction.Status.COMMITTED, txn.checkStatus());
    }

    private static TableKey toKey(String s, PaddedStringSerializer serializer) {
        return new TableKey(serializer.serialize(s));
    }

    private KeyValueTable getKVTable(String keyValueTableName, String scopeName) {
        KeyValueTableFactory keyValueTableFactory = KeyValueTableFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        return keyValueTableFactory.forKeyValueTable(keyValueTableName,
                KeyValueTableClientConfiguration.builder().build());
    }

    /**
     * This method tests the ability to create a KeyValueTable in some scope.
     * This method performs all CRUD operations on a Key-Value table and validates them.
     */
    @Test(timeout = 20000)
    private void checkKeyValueTable() {
        String scopeName = "transaction-test-scope";
        String keyValueTableName = "KVT-test";
        String userName = "Compatibility-checker";
        String testData = "test-data";

        val kvtConfig = KeyValueTableConfiguration.builder()
                .partitionCount(2)
                .primaryKeyLength(USERNAME_SERIALIZER.getMaxLength())
                .secondaryKeyLength(0)
                .build();
        val created = keyValueTableManager.createKeyValueTable(scopeName, keyValueTableName, kvtConfig);
        assertTrue(created);
        // creating key value table twice should fail.
        assertFalse(keyValueTableManager.createKeyValueTable(scopeName, keyValueTableName, kvtConfig));

        KeyValueTable testKVTables = getKVTable(keyValueTableName, scopeName);
        // Inserting single entry to validate duplication of the key
        val insertEntry = new Insert(toKey(userName + "0", USERNAME_SERIALIZER), SERIALIZER.serialize(testData + "0"));
        testKVTables.update(insertEntry).join();
        assertTrue(testKVTables.exists(toKey(userName + "0", USERNAME_SERIALIZER)).join());
        val data = SERIALIZER.deserialize(testKVTables.get(toKey(userName + "0", USERNAME_SERIALIZER)).join().getValue());
        assertEquals(data, testData + "0");

        // While updating an already existing key, an exception of type ConditionalTableUpdateException should be thrown.
        testKVTables.update(insertEntry)
                .handle((r, ex) -> {
                    ex = Exceptions.unwrap(ex);
                    assertTrue(ex instanceof ConditionalTableUpdateException);
                    return null;
                }).join();
        
        // Inserting 9 more key into the tables
        for (int i = 1; i < TEST_MAX_KEYS; i++) {
            val insert1 = new Insert(toKey(userName + i, USERNAME_SERIALIZER), SERIALIZER.serialize(testData + i));
            testKVTables.update(insert1).join();
        }
        // Validating the content of the KV table
        for (int i = 0; i < TEST_MAX_KEYS; i++) {
            assertTrue(testKVTables.exists(toKey(userName + i, USERNAME_SERIALIZER)).join());
            assertEquals(SERIALIZER.deserialize(testKVTables.get(toKey(userName + i, USERNAME_SERIALIZER)).join().getValue()), testData + i);
        }
        // Listing all the entries of kv tables
        val count = new AtomicInteger(0);
        testKVTables.iterator()
                .maxIterationSize(20)
                .all()
                .keys()
                .forEachRemaining(ii -> {
                    for (val user : ii.getItems()) {
                        val key = USERNAME_SERIALIZER.deserialize(user.getPrimaryKey());
                        System.out.println(String.format("\t%s", key));
                        count.incrementAndGet();
                    }
                }, this.executor).join();
        assertEquals(TEST_MAX_KEYS, count.get());
        // Updating the key to some new data and validating it
        val put = new Put(toKey(userName + "0", USERNAME_SERIALIZER), SERIALIZER.serialize(testData));
        testKVTables.update(put);
        assertTrue(testKVTables.exists(toKey(userName + "0", USERNAME_SERIALIZER)).join());
        assertEquals(SERIALIZER.deserialize(testKVTables.get(toKey(userName + "0", USERNAME_SERIALIZER)).join().getValue()), testData);

        for (int i = 0; i < TEST_MAX_KEYS; i++) {
            val delete = new Remove(toKey(userName + i, USERNAME_SERIALIZER));
            testKVTables.update(delete).join();
            assertFalse(testKVTables.exists(toKey(userName + i, USERNAME_SERIALIZER)).join());
        }
        // Deleting key value table and validating it.
        assertTrue(keyValueTableManager.deleteKeyValueTable(scopeName, keyValueTableName));
    }

    /**
     * This method writes and reads large events (4MB) and validates them.
     */
    @Test(timeout = 20000)
    private void checkWriteAndReadLargeEvent() {
        String scopeName = "largeevent-test-scope";
        String streamName = "largeevent-test-stream";
        createScopeAndStream(scopeName, streamName);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(streamName, new ByteBufferSerializer(), EventWriterConfig.builder().enableLargeEvents(true).build());

        byte[] payload = new byte[Serializer.MAX_EVENT_SIZE * 4];
        for (int i = 0; i < NUM_EVENTS; i++) {
            log.info("Writing event: {} ", i);
            // any exceptions while writing the event will fail the test.
            writer.writeEvent("", ByteBuffer.wrap(payload)).join();
            log.info("Wrote event: {} ", i);
            writer.flush();
        }
        String readerGroupId = getRandomID();
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scopeName, streamName)).build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<ByteBuffer> reader =  clientFactory.createReader("reader-1", readerGroupId, new ByteBufferSerializer(), ReaderConfig.builder().build());

        int readCount = 0;
        EventRead<ByteBuffer> event = null;
        do {
            event = reader.readNextEvent(10_000);
            log.debug("Read event: {}.", event.getEvent());
            if (event.getEvent() != null) {
                readCount++;
            }
            // try reading until all the written events are read, else the test will timeout.
        } while ((event.getEvent() != null || event.isCheckpoint()) && readCount < NUM_EVENTS);
        assertEquals("Read count should be equal to write count", NUM_EVENTS, readCount);
    }

    @Data
    private static class PaddedStringSerializer {
        private final int maxLength;

        ByteBuffer serialize(String s) {
            Preconditions.checkArgument(s.length() <= maxLength);
            s = Strings.padStart(s, this.maxLength, ' ');
            return ByteBuffer.wrap(s.getBytes(StandardCharsets.US_ASCII));
        }

        String deserialize(ByteBuffer b) {
            Preconditions.checkArgument(b.remaining() <= maxLength);
            String s = StandardCharsets.US_ASCII.decode(b).toString();
            s = s.trim();
            return s;
        }

    }

    public static void main(String[] args) throws DeleteScopeFailedException, TxnFailedException {
        String uri = System.getProperty("controllerUri");
        if (uri == null) {
            log.error("Input correct controller URI (e.g., \"tcp://localhost:9090\")");
            System.exit(1);
        }
        CompatibilityChecker compatibilityChecker = new CompatibilityChecker();
        compatibilityChecker.setUp(uri);
        compatibilityChecker.checkWriteAndReadEvent();
        compatibilityChecker.checkTruncationOfStream();
        compatibilityChecker.checkSealStream();
        compatibilityChecker.checkDeleteScope();
        compatibilityChecker.checkStreamDelete();
        compatibilityChecker.checkTransactionReadAndWrite();
        compatibilityChecker.checkTransactionAbort();
        compatibilityChecker.checkKeyValueTable();
        compatibilityChecker.checkWriteAndReadLargeEvent();
        compatibilityChecker.checkStreamTags();
        System.exit(0);
    }
}