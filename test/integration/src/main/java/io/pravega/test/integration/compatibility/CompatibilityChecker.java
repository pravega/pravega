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
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.Insert;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Put;
import io.pravega.client.tables.Remove;
import io.pravega.client.tables.TableKey;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.Lists.newArrayList;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static io.pravega.test.common.AssertExtensions.fail;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
    private static final int TOTAL_EVENTS = 2400;
    private static final int READ_TIMEOUT = 5 * 1000;
    private static final int CUT_SIZE = 400;
    private static final PaddedStringSerializer USERNAME_SERIALIZER = new PaddedStringSerializer(64);
    private static final UTF8StringSerializer SERIALIZER = new UTF8StringSerializer();
    private final ScheduledExecutorService streamCutExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "streamCutExecutor");
    private final ScheduledExecutorService readerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(4, "readerPool");
    private final ScheduledExecutorService chkPointExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "chkPointExecutor");
    private final AtomicLong timer = new AtomicLong();

    private URI controllerURI;
    private StreamManager streamManager;
    private StreamConfiguration streamConfig;
    private KeyValueTableManager keyValueTableManager;
    private ScheduledExecutorService executor;

    private Controller controller;
    private ConnectionPool connectionPool;
    private ClientConfig clientConfig;
    private void setUp(String uri) {
        controllerURI = URI.create(uri);
        clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        connectionPool = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(), connectionPool.getInternalExecutor());
        streamManager = StreamManager.create(controllerURI);
        streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        keyValueTableManager = KeyValueTableManager.create(controllerURI);
        executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "compatibility-test");
    }

   private void createScopeAndStream(String scopeName, String streamName) {
       streamManager.createScope(scopeName);
       streamManager.createStream(scopeName, streamName, streamConfig);
   }

    private void createScopeAndStream(String scopeName, String streamName, StreamConfiguration streamConfig) {
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
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
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
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);

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
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
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
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
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
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
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
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
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
            val insert = new Insert(toKey(userName + i, USERNAME_SERIALIZER), SERIALIZER.serialize(testData + i));
            testKVTables.update(insert).join();
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
                .forEachRemaining(tableKey -> {
                    for (val user : tableKey.getItems()) {
                        val key = USERNAME_SERIALIZER.deserialize(user.getPrimaryKey());
                        log.info("User: {} ", key);
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
     * This method writes and reads large events (8MB) and validates them.
     */
    @Test(timeout = 20000)
    private void checkWriteAndReadLargeEvent() {
        String scopeName = "largeevent-test-scope";
        String streamName = "largeevent-test-stream";
        createScopeAndStream(scopeName, streamName);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
        @Cleanup
        EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(streamName, new ByteBufferSerializer(), EventWriterConfig.builder().enableLargeEvents(true).build());

        byte[] payload = new byte[Serializer.MAX_EVENT_SIZE * 2];
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

    /**
     * This test verifies the correct operation of readers using StreamCuts. Concretely, the test creates two streams
     * with different number of segments and it writes some events (TOTAL_EVENTS / 2) in them. Then, the test creates a
     * list of StreamCuts that encompasses both streams every CUT_SIZE events. The test asserts that new groups of
     * readers can be initialized at these sequential StreamCut intervals and that only CUT_SIZE events are read. Also,
     * the test checks the correctness of different combinations of StreamCuts that have not been sequentially created.
     * After creating StreamCuts and tests the correctness of reads, the test also checks resetting a reader group to a
     * specific initial read point. Finally, this test checks reading different StreamCut combinations in both streams for
     * all events.
     */
    private void checkStreamCuts() {
        String scopeName = "streamCuts-test-scope";
        String streamOne = "streamCuts-test-streamOne";
        String streamTwo = "streamCuts-test-streamTwo";
        createScopeAndStream(scopeName, streamOne, streamConfig);
        streamManager.createStream(scopeName, streamTwo, streamConfig);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);

        String readerGroupId = getRandomID();
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                                                                .stream(Stream.of(scopeName, streamOne))
                                                                .stream(Stream.of(scopeName, streamTwo)).build();
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, controllerURI);
        readerGroupManager.createReaderGroup(readerGroupId, readerGroupConfig);
        @Cleanup
        EventStreamReader<String> reader =  clientFactory.createReader("reader-1", readerGroupId, new UTF8StringSerializer(), ReaderConfig.builder().build());

        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(readerGroupId);

        // Perform write of events, slice by slice StreamCuts test and combinations StreamCuts test.
        log.info("Write, slice by slice and combinations test before scaling.");
        final int parallelism = 3;
        // First, write half of total events before scaling (1/4 in each Stream).
        writeEvents(clientFactory, streamOne, TOTAL_EVENTS / 4, 0);
        writeEvents(clientFactory, streamTwo, TOTAL_EVENTS / 4, 0);
        log.info("Finished writing events to streams.");

        Map<Stream, StreamCut> initialPosition = new HashMap<>(readerGroup.getStreamCuts());
        log.info("Creating StreamCuts from: {}.", initialPosition);
        // Get StreamCuts for each slice from both Streams at the same time (may be different in each execution).
        List<Map<Stream, StreamCut>> streamSlices = getStreamCutSlices(readerGroup, TOTAL_EVENTS / 2, reader);
        streamSlices.add(0, initialPosition);
        log.info("Finished creating StreamCuts {}.", streamSlices);

        // Ensure that reader groups can correctly read slice by slice from different Streams.
        readSliceBySliceAndVerify(readerGroupManager, clientFactory, parallelism, streamSlices, scopeName, streamOne, streamTwo);
        log.info("Finished checking sequentially slice by slice.");

        // Perform different combinations of StreamCuts and verify that read event boundaries are still correct.
        combineSlicesAndVerify(readerGroupManager, clientFactory, parallelism, streamSlices, scopeName, streamOne, streamTwo);
        log.info("Finished checking StreamCut combinations.");
        // Test that a reader group can be reset correctly.
        ReaderGroupConfig firstSliceConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scopeName, streamOne))
                .stream(Stream.of(scopeName, streamTwo))
                .startingStreamCuts(initialPosition)
                .endingStreamCuts(streamSlices.get(streamSlices.size() - 1)).build();
        readerGroup.resetReaderGroup(firstSliceConfig);
        log.info("Resetting existing reader group {} to stream cut {}.", readerGroupId, initialPosition);
        final int readEvents = readAllEvents(readerGroupManager, clientFactory, readerGroup.getGroupName(), parallelism
        ).stream().map(CompletableFuture::join).reduce(Integer::sum).get();
        assertEquals("Expected read events: ", TOTAL_EVENTS / 2, readEvents);
    }

    /**
     * This method tests the functionality of reading and writing bytes to a stream using the ByteStreamClientFactory.
     * It creates a new scope and stream with the provided names and tests the ability to write a byte array to the stream,
     * read the bytes from the stream, and truncate data before a specified offset.
     * @throws Exception if there are any issues with creating or accessing the stream or if any of the tests fail.
     */
    private void checkByteStreamReadWrite() throws Exception {
        String scopeName = "byte-readwrite-test-scope";
        String streamName = "byte-readwrite-test-stream";

        createScopeAndStream(scopeName, streamName);
        @Cleanup
        ByteStreamClientFactory clientFactory = ByteStreamClientFactory.withScope(scopeName, clientConfig);
        @Cleanup
        ByteStreamWriter writer = clientFactory.createByteStreamWriter(streamName);
        byte[] value = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int headOffset = 0;
        writer.write(value);
        writer.flush();
        @Cleanup
        ByteStreamReader reader = clientFactory.createByteStreamReader(streamName);
        for (int i = 0; i < 10; i++) {
            assertEquals(i, reader.read());
        }
        assertEquals(headOffset, reader.fetchHeadOffset());
        assertEquals(value.length, reader.fetchTailOffset());
        headOffset = 3;
        writer.truncateDataBefore(headOffset);
        writer.write(value);
        writer.flush();
        assertEquals(headOffset, reader.fetchHeadOffset());
        assertEquals(value.length * 2, reader.fetchTailOffset());
        byte[] read = new byte[5];
        assertEquals(5, reader.read(read));
        assertArrayEquals(new byte[] { 0, 1, 2, 3, 4 }, read);
        assertEquals(2, reader.read(read, 2, 2));
        assertArrayEquals(new byte[] { 0, 1, 5, 6, 4 }, read);
        assertEquals(3, reader.read(read, 2, 3));
        assertArrayEquals(new byte[] { 0, 1, 7, 8, 9 }, read);
        assertArrayEquals(new byte[] { 0, 1, 7, 8, 9 }, read);

    }

    /**
     * This method validate the watermark feature of a stream.
     * The method does the following:
     *  1. Creates a stream with a fixed scaling policy of 5 partitions, and creates two writers for the stream.
     *  2. Writes events to the stream using the two writers concurrently.
     *  3. Scales the stream several times to generate complex positions.
     *  4. Fetches the watermarks from the stream and adds them to a queue.
     *  5. Verifies that at least two watermarks have been fetched from the stream within 100 seconds.
     *  6. Stops writing events to the stream.
     *  7. Reads events from the stream using the first and second watermarks fetched, and verifies that all events are below the watermark bounds.
     */
    @Test(timeout = 120000)
    private void checkWatermark() throws Exception {
        String scope = "scope";
        String stream = "watermarkTest";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();
        createScopeAndStream(scope, stream, config);

        Stream streamObj = Stream.of(scope, stream);

        // create 2 writers
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<Long> writer1 = clientFactory.createEventWriter(stream, javaSerializer,
                EventWriterConfig.builder().build());
        @Cleanup
        EventStreamWriter<Long> writer2 = clientFactory.createEventWriter(stream, javaSerializer,
                EventWriterConfig.builder().build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        CompletableFuture<Void> writer1Future = writeEventAsync(writer1, stopFlag);
        CompletableFuture<Void> writer2Future = writeEventAsync(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        scale(controller, streamObj, config);

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        @Cleanup
        ClientFactoryImpl syncClientFactory = new ClientFactoryImpl(scope,
                new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                        connectionFactory.getInternalExecutor()),
                connectionFactory);

        String markStream = NameUtils.getMarkStreamForStream(stream);
        @Cleanup
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);

        stopFlag.set(true);

        writer1Future.join();
        writer2Future.join();

        // read events from the stream
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, syncClientFactory);

        Watermark watermark0 = watermarks.take();
        Watermark watermark1 = watermarks.take();
        assertTrue(watermark0.getLowerTimeBound() <= watermark0.getUpperTimeBound());
        assertTrue(watermark1.getLowerTimeBound() <= watermark1.getUpperTimeBound());
        assertTrue(watermark0.getLowerTimeBound() < watermark1.getLowerTimeBound());

        Map<Segment, Long> positionMap0 = watermark0.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));
        Map<Segment, Long> positionMap1 = watermark1.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));

        StreamCut streamCutFirst = new StreamCutImpl(streamObj, positionMap0);
        StreamCut streamCutSecond = new StreamCutImpl(streamObj, positionMap1);
        Map<Stream, StreamCut> firstMarkStreamCut = Collections.singletonMap(streamObj, streamCutFirst);
        Map<Stream, StreamCut> secondMarkStreamCut = Collections.singletonMap(streamObj, streamCutSecond);

        // read from stream cut of first watermark
        String readerGroup = "watermarkTest-group";
        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                .startingStreamCuts(firstMarkStreamCut)
                .endingStreamCuts(secondMarkStreamCut)
                .disableAutomaticCheckpoints()
                .build());

        @Cleanup
        final EventStreamReader<Long> reader = clientFactory.createReader("myreader",
                readerGroup,
                javaSerializer,
                ReaderConfig.builder().build());

        EventRead<Long> event = reader.readNextEvent(10000L);
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        while (event.getEvent() != null && currentTimeWindow.getLowerTimeBound() == null && currentTimeWindow.getUpperTimeBound() == null) {
            event = reader.readNextEvent(10000L);
            currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        }

        assertNotNull(currentTimeWindow.getUpperTimeBound());

        // read all events and verify that all events are below the bounds
        while (event.getEvent() != null) {
            Long time = event.getEvent();
            log.info("timewindow = {} event = {}", currentTimeWindow, time);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || time >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || time <= currentTimeWindow.getUpperTimeBound());

            TimeWindow nextTimeWindow = reader.getCurrentTimeWindow(streamObj);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || nextTimeWindow.getLowerTimeBound() >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || nextTimeWindow.getUpperTimeBound() >= currentTimeWindow.getUpperTimeBound());
            currentTimeWindow = nextTimeWindow;

            event = reader.readNextEvent(10000L);
            if (event.isCheckpoint()) {
                event = reader.readNextEvent(10000L);
            }
        }

        assertNotNull(currentTimeWindow.getLowerTimeBound());
    }


    /**
     * This method tests the ability of a batch reader to read events from a stream.
     * It creates a scope and stream, writes events to the stream, and uses the BatchClientFactory
     * to read events from the stream's segments. It then checks the number of events read against
     * the total number of events written.
     * The code also scales the stream, writes more events, and reads from the stream again to ensure
     * the batch reader can handle scaling.
     */
    @Test(timeout = 120000)
    private void checkBatchClient() throws Exception {
        String scope = "batch-test-scope";
        String streamName = "batch-test-stream";
        StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build();
        createScopeAndStream(scope, streamName, streamConfig);
        Stream stream = Stream.of(scope, streamName);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        writeEvents(clientFactory, streamName, NUM_EVENTS, 0);
        @Cleanup
        val batchClientFactory = BatchClientFactory.withScope(scope, clientConfig);
        ArrayList<SegmentRange> segments = new ArrayList<>();
        Iterator<SegmentRange> iterator =  batchClientFactory.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator();

        while (iterator.hasNext()) {
            segments.add(iterator.next());
        }
        int totalReadEvents = readFromSegments(batchClientFactory, segments);
        assertEquals(totalReadEvents, NUM_EVENTS);

        scale(controller, stream, streamConfig);
        writeEvents(clientFactory, streamName, NUM_EVENTS, 0);
        ArrayList<SegmentRange> segmentsAfterScale = new ArrayList<>();

        iterator = batchClientFactory.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator();

        while (iterator.hasNext()) {
            segmentsAfterScale.add(iterator.next());
        }
        totalReadEvents = readFromSegments(batchClientFactory, segments);
        assertEquals(totalReadEvents, NUM_EVENTS);
    }

    private CompletableFuture<Void> writeEventAsync(EventStreamWriter<Long> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        AtomicLong currentTime = new AtomicLong();
        return Futures.loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
            currentTime.set(timer.incrementAndGet());
            return writer.writeEvent(count.toString(), currentTime.get())
                    .thenAccept(v -> {
                        if (count.incrementAndGet() % 3 == 0) {
                            writer.noteTime(currentTime.get());
                        }
                    });
        }, 1000L, executor), executor);
    }

    private void writeEvents(EventStreamClientFactory clientFactory, String streamName, int totalEvents, int initialPoint) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = initialPoint; i < totalEvents + initialPoint; i++) {
            writer.writeEvent(String.format("%03d", i)).join(); // this ensures the event size is constant.
            log.debug("Writing event: {} to stream {}.", streamName + i, streamName);
        }
        log.info("Writer {} finished writing {} events.", writer, totalEvents - initialPoint);
    }

    private <T extends Serializable> List<Map<Stream, StreamCut>> getStreamCutSlices(ReaderGroup readerGroup,
                                                                                     int totalEvents, EventStreamReader<T> reader1) {
        final AtomicReference<EventStreamReader<T>> reader = new AtomicReference<>();
        final AtomicReference<CompletableFuture<Map<Stream, StreamCut>>> streamCutFuture = new AtomicReference<>(CompletableFuture.completedFuture(null));
        reader.set(reader1);
        final List<CompletableFuture<Map<Stream, StreamCut>>> streamCutFutureList = new ArrayList<>();
        final AtomicInteger validEvents = new AtomicInteger();

        Futures.loop(
                () -> validEvents.get() < totalEvents,
                () -> CompletableFuture.runAsync(() -> {
                    try {
                        EventRead<T> event = reader.get().readNextEvent(READ_TIMEOUT);
                        if (event.getEvent() != null) {
                            log.info("Await and verify if the last StreamCut generation completed successfully {}",
                                    Futures.await(streamCutFuture.get(), 10_000));
                            assertTrue("StreamCut generation did not complete", Futures.await(streamCutFuture.get(), 10_000));
                            validEvents.incrementAndGet();
                            log.debug("Read event result in getStreamCutSlices: {}. Valid events: {}.", event.getEvent(), validEvents);
                            // Get a StreamCut each defined number of events.
                            if (validEvents.get() % CUT_SIZE == 0 && validEvents.get() > 0) {
                                CompletableFuture<Map<Stream, StreamCut>> streamCutsFuture =
                                        readerGroup.generateStreamCuts(streamCutExecutor);
                                streamCutFuture.set(streamCutsFuture);
                                // wait for 5 seconds to force reader group state update, so that we can ensure StreamCut for every
                                // CUT_SIZE number of events.
                                Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(5));
                                log.info("Adding a StreamCut positioned at event {}", validEvents);
                                streamCutFutureList.add(streamCutsFuture);
                            }
                        } else {
                            log.warn("Read unexpected null event at {}.", validEvents);
                        }
                    } catch (ReinitializationRequiredException e) {
                        log.warn("Reinitialization of readers required.", e);
                    }
                }, executor),
                executor).join();
        reader.get().close();

        // Now return all the streamCuts generated.
        return Futures.getAndHandleExceptions(Futures.allOfWithResults(streamCutFutureList), t -> {
            log.error("StreamCut generation did not complete", t);
            throw new AssertionError("StreamCut generation did not complete", t);
        });
    }

    private void scale(Controller controller, Stream stream, StreamConfiguration configuration) {
        // perform several scales
        int numOfSegments = configuration.getScalingPolicy().getMinNumSegments();
        double delta = 1.0 / numOfSegments;
        for (long segmentNumber = 0; segmentNumber < numOfSegments - 1; segmentNumber++) {
            double rangeLow = segmentNumber * delta;
            double rangeHigh = (segmentNumber + 1) * delta;
            double rangeMid = (rangeHigh + rangeLow) / 2;

            Map<Double, Double> map = new HashMap<>();
            map.put(rangeLow, rangeMid);
            map.put(rangeMid, rangeHigh);
            controller.scaleStream(stream, Collections.singletonList(segmentNumber), map, executor).getFuture().join();
        }
    }

    private void fetchWatermarks(RevisionedStreamClient<Watermark> watermarkReader, LinkedBlockingQueue<Watermark> watermarks, AtomicBoolean stop) throws Exception {
        AtomicReference<Revision> revision = new AtomicReference<>(watermarkReader.fetchOldestRevision());

        Futures.loop(() -> !stop.get(), () -> Futures.delayedTask(() -> {
            Iterator<Map.Entry<Revision, Watermark>> marks = watermarkReader.readFrom(revision.get());
            if (marks.hasNext()) {
                Map.Entry<Revision, Watermark> next = marks.next();
                watermarks.add(next.getValue());
                revision.set(next.getKey());
            }
            return null;
        }, Duration.ofSeconds(5), executor), executor);
    }

    private List<CompletableFuture<Integer>> readAllEvents(ReaderGroupManager rgMgr, EventStreamClientFactory clientFactory, String rGroupId,
                                                           int readerCount) {
        return IntStream.range(0, readerCount)
                .mapToObj(i -> CompletableFuture.supplyAsync(() -> createReaderAndReadEvents(rgMgr, clientFactory, rGroupId, i),
                        readerExecutor))
                .collect(Collectors.toList());

    }

    private <T extends Serializable> int createReaderAndReadEvents(ReaderGroupManager rgMgr, EventStreamClientFactory clientFactory,
                                                                   String rGroupId, int readerIndex) {
        // create a reader.
        EventStreamReader<T> reader = clientFactory.createReader(rGroupId + "-" + readerIndex, rGroupId, new JavaSerializer<>(),
                ReaderConfig.builder().build());
        EventRead<T> event = null;
        int validEvents = 0;
        AtomicBoolean sealedSegmentUpdated = new AtomicBoolean(false);
        try {
            do {
                try {
                    event = reader.readNextEvent(READ_TIMEOUT);
                    log.debug("Read event result in readEvents: {}.", event.getEvent());
                    if (event.getEvent() == null && !event.isCheckpoint() && !sealedSegmentUpdated.get()) {
                        // initiate a checkpoint to ensure all sealed segments are acquired by the reader.
                        ReaderGroup readerGroup = rgMgr.getReaderGroup(rGroupId);
                        readerGroup.initiateCheckpoint("chkPoint", chkPointExecutor)
                                .whenComplete((checkpoint, t) -> {
                                    if (t != null) {
                                        log.error("Checkpoint operation failed", t);
                                    } else {
                                        log.info("Checkpoint {} completed", checkpoint);
                                        sealedSegmentUpdated.set(true);
                                    }
                                });
                    }
                    if (event.getEvent() != null) {
                        validEvents++;
                    }
                } catch (ReinitializationRequiredException e) {
                    log.error("Reinitialization Exception while reading event using readerId: {}", reader, e);
                    fail("Reinitialization Exception is not expected");
                }
            } while (event.getEvent() != null || event.isCheckpoint() || !sealedSegmentUpdated.get());
        } finally {
            reader.close();
        }
        return validEvents;
    }

    /**
     * Check that all the stream slices represented by consecutive StreamCut pairs can be read correctly.
     */
    private void readSliceBySliceAndVerify(ReaderGroupManager manager, EventStreamClientFactory clientFactory, int parallelSegments,
                                           List<Map<Stream, StreamCut>> streamSlices, String scope, String streamOne, String streamTwo) {
        int readEvents;
        for (int i = 1; i < streamSlices.size(); i++) {
            log.debug("Reading events between starting StreamCut {} and ending StreamCut {}", streamSlices.get(i-1), streamSlices.get(i));
            ReaderGroupConfig configBuilder = ReaderGroupConfig.builder().stream(Stream.of(scope, streamOne))
                    .stream(Stream.of(scope, streamTwo))
                    .startingStreamCuts(streamSlices.get(i - 1))
                    .endingStreamCuts(streamSlices.get(i)).build();

            // Create a new reader group per stream cut slice and read in parallel only events within the cut.
            final String readerGroupId = getRandomID();
            manager.createReaderGroup(readerGroupId, configBuilder);
            readEvents = readAllEvents(manager, clientFactory, readerGroupId, parallelSegments).stream()
                    .map(CompletableFuture::join)
                    .reduce(Integer::sum).get();
            log.debug("Read events by group {}: {}.", readerGroupId, readEvents);
            assertEquals("Expected events read: ", CUT_SIZE, readEvents);
        }
    }

    /**
     * This method performs slices in streams of non-consecutive StreamCuts. For instance, say that we generate 5 cuts
     * in this order: C1, C2, C3, C4, C5. We then read slices of a stream formed like this:
     * [C1, C3), [C1, C4), [C1, C5)
     * [C2, C4), [C2, C5)
     * [C3, C5)
     * Note that all the consecutive slices have been previously tested, so we avoid them to shorten test execution.
     * Moreover, take into account that a increase in the number of slices greatly lengthen execution time.
     *
     * @param manager Group manager for this scope.
     * @param clientFactory Client factory to instantiate new readers.
     * @param parallelSegments Number of parallel segments that indicates the number of parallel readers to instantiate.
     * @param streamSlices StreamCuts lists to be combined and tested via bounded processing.
     */
    private void combineSlicesAndVerify(ReaderGroupManager manager, EventStreamClientFactory clientFactory, int parallelSegments,
                                        List<Map<Stream, StreamCut>> streamSlices, String scope, String streamOne, String streamTwo) {

        for (int i = 0; i < streamSlices.size() - 1; i++) {
            List<Map<Stream, StreamCut>> combinationSlices = new ArrayList<>(streamSlices).subList(i, streamSlices.size());
            ReaderGroupConfig.ReaderGroupConfigBuilder configBuilder = ReaderGroupConfig.builder()
                    .stream(Stream.of(scope, streamOne))
                    .stream(Stream.of(scope, streamTwo))
                    .startingStreamCuts(combinationSlices.remove(0));

            // Remove the contiguous StreamCut to the starting one, as the slice [CN, CN+1) has been already tested.
            combinationSlices.remove(0);

            // The minimum slice we are going to test is twice the size of CUT_SIZE.
            int readEvents, combinationCutSize = 2;
            for (Map<Stream, StreamCut> endingPoint : combinationSlices) {
                configBuilder = configBuilder.endingStreamCuts(endingPoint);

                // Create a new reader group per stream cut slice and read in parallel only events within the cut.
                final String readerGroupId =  getRandomID();
                manager.createReaderGroup(readerGroupId, configBuilder.build());
                log.debug("Reading events between starting StreamCut {} and ending StreamCut {}",
                        configBuilder.build().getStartingStreamCuts(), endingPoint);
                readEvents = readAllEvents(manager, clientFactory, readerGroupId, parallelSegments).stream()
                        .map(CompletableFuture::join)
                        .reduce(Integer::sum).get();
                log.debug("Read events by group {}: {}.", readerGroupId, readEvents);
                assertEquals("Expected events read: ", combinationCutSize * CUT_SIZE, readEvents);
                combinationCutSize++;
            }
        }
    }

    private int readFromSegments(BatchClientFactory batchClient, List<SegmentRange> segments) throws Exception {
        UTF8StringSerializer serializer = new UTF8StringSerializer();
        int count = segments
                .stream()
                .mapToInt(segment -> {
                    SegmentIterator<String> segmentIterator = batchClient.readSegment(segment, serializer);
                    int numEvents = 0;
                    try {
                        String id = String.format("%s", Thread.currentThread().getId());
                        while (segmentIterator.hasNext()) {
                            String event = segmentIterator.next();
                            numEvents++;
                        }
                    } finally {
                        segmentIterator.close();
                    }
                    return numEvents;
                }).sum();
        return count;
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
            return StandardCharsets.US_ASCII.decode(b).toString().trim();
        }

    }

    public static void main(String[] args) throws Exception {
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
        compatibilityChecker.checkStreamCuts();
        compatibilityChecker.checkWatermark();
        compatibilityChecker.checkByteStreamReadWrite();
        compatibilityChecker.checkBatchClient();
        System.exit(0);
    }
}