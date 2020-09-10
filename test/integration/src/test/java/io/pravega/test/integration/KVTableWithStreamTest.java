/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.grpc.stub.StreamObserver;
import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Version;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.client.tables.impl.KeyValueTableTestBase;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;

import lombok.SneakyThrows;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Random;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration test for {@link KeyValueTable}s along with {@link Stream}s to check proper functioning
 * of both primitives when used together using real Segment Store, Controller and connection.
 */
@Slf4j
public class KVTableWithStreamTest extends KeyValueTableTestBase {

    private static final String ENDPOINT = "localhost";
    private static final String SCOPE = "Scope";
    private static final KeyValueTableConfiguration DEFAULT_CONFIG = KeyValueTableConfiguration.builder()
            .partitionCount(5).build();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private ServiceBuilder serviceBuilder;
    private TableStore tableStore;
    private StreamSegmentStore streamSegmentStore;
    private PravegaConnectionListener serverListener = null;
    private ConnectionPool connectionPool;
    private TestingServer zkTestServer = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller;
    private ControllerServiceImpl controllerService;
    private RequestTracker requestTracker = new RequestTracker(true);
    private KeyValueTableFactory keyValueTableFactory;
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = ENDPOINT;
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final int startKeyRange = 10000;
    private final int endKeyRange = 99999;
    private final int valueSize = 10312;
    private Random random = new Random(100);

    @Before
    public void setup() throws Exception {
        super.setup();

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        this.tableStore = serviceBuilder.createTableStoreService();
        this.streamSegmentStore = serviceBuilder.createStreamSegmentService();

        this.serverListener = new PravegaConnectionListener(false, servicePort, this.streamSegmentStore,
                this.tableStore, executorService());
        this.serverListener.startListening();

        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();

        //4. Create Scope
        this.controller.createScope(SCOPE).get();
        ClientConfig clientConfig = ClientConfig.builder().build();
        SocketConnectionFactoryImpl connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        this.connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        this.keyValueTableFactory = new KeyValueTableFactoryImpl(SCOPE, this.controller, this.connectionPool);

        //5. setting up controller service instance
        this.controllerService = new ControllerServiceImpl(controllerWrapper.getControllerService(),
                GrpcAuthHelper.getDisabledAuthHelper(), requestTracker, true, 2);
    }

    @After
    public void tearDown() throws Exception {
        this.controller.close();
        this.connectionPool.close();
        this.controllerWrapper.close();
        this.serverListener.close();
        this.serviceBuilder.close();
        this.zkTestServer.close();
        this.keyValueTableFactory.close();
    }

    /**
     * Create Stream and KVTable with same name under same scope.
     */
    @Test
    public void testCreateStreamKVTable() throws ExecutionException, InterruptedException {
        String newScope = "NewScope";
        KeyValueTableInfo newKVT = newKeyValueTableName(newScope);
        String streamName = newKVT.getKeyValueTableName();
        int streamSegments = 1;
        String readerGroupName = "readerGroup";
        String readerName = "reader";
        String routingKey = "routingKey";
        String streamEvent = "Stream event";
        int numEvents = 100;

        final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(streamSegments);
        final StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .build();

        assertTrue("The requested Scope must get created", this.controller.createScope(newScope).join());

        assertTrue("The requested Stream must get created", this.controller.createStream(newScope,
                streamName, config).join());

        assertTrue("The requested KVT must get created",
                this.controller.createKeyValueTable(newScope, newKVT.getKeyValueTableName(),
                        DEFAULT_CONFIG).join());

        assertEquals("Table partition value should be same as passed", DEFAULT_CONFIG.getPartitionCount(),
                Futures.getAndHandleExceptions(this.controller.getCurrentSegmentsForKeyValueTable(newScope, newKVT.getKeyValueTableName()),
                        RuntimeException::new).getSegments().size());

        assertEquals("The stream segment count should be same as passed", streamSegments,
                Futures.getAndHandleExceptions(this.controller.getCurrentSegments(newScope, streamName),
                        RuntimeException::new).getSegments().size());

        // To read and write few sample events on the stream having same name as the KVTable.
        readWriteOnStream(newScope, streamName, numEvents, routingKey, streamEvent, readerGroupName, readerName);

        String keyFamily = "KeyFamily1";
        // inserts and fetches few Table entries for validate basic functioning of KVTable
        insertAndGetFromKVTable(newScope, newKVT, keyFamily);

        // Cleans up stream resources by sealing existing stream segments & then deletes the stream.
        cleanUpStream(newScope, streamName);
        // deleting the scope.
        cleanUpScope(newScope);
    }

    /**
     * Tests Creation and deletion of KVTables and Streams +
     * under different scopes
     */
    @Test
    public void testDeleteKVTableStream() throws ExecutionException, InterruptedException {
        String newScope1 = "TestScope1-" + randomAlphanumeric(5);
        String newScope2 = "TestScope2-" + randomAlphanumeric(5);
        KeyValueTableInfo newKVT1 = newKeyValueTableName(newScope1);
        KeyValueTableInfo newKVT2 = newKeyValueTableName(newScope2);
        KeyValueTableConfiguration config = KeyValueTableConfiguration.builder().partitionCount(5).build();
        String streamName1 = newKVT1.getKeyValueTableName();
        String streamName2 = newKVT2.getKeyValueTableName();
        String readerGroupName = "readerGroup";
        String readerName = "reader";
        String routingKey = "routingKey";
        String streamEvent = "Stream event";
        int numEvents = 100;

        assertTrue("The requested Scope must get created", this.controller.createScope(newScope1).join());
        // Creating KVT1 under Scope1.
        assertTrue("The requested KVT must get created",
                this.controller.createKeyValueTable(newScope1, newKVT1.getKeyValueTableName(),
                        config).join());

        assertEquals("Table partition value should be same as passed", DEFAULT_CONFIG.getPartitionCount(),
                Futures.getAndHandleExceptions(this.controller.getCurrentSegmentsForKeyValueTable(newScope1,
                        newKVT1.getKeyValueTableName()), RuntimeException::new).getSegments().size());

        val tableSegments1 = this.controller.getCurrentSegmentsForKeyValueTable(newScope1,
                newKVT1.getKeyValueTableName()).join();

        for (val s : tableSegments1.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT).join();
        }

        // Case-1: Deleting KeyValueTable where Scope does not exist.
        ResultObserver<DeleteKVTableStatus> result1 = new ResultObserver<>();
        this.controllerService.deleteKeyValueTable(ModelHelper.createKeyValueTableInfo(newScope2,
                newKVT1.getKeyValueTableName()), result1);
        assertEquals("The requested table to delete must not be found for invalid Scope name",
                DeleteKVTableStatus.Status.TABLE_NOT_FOUND, result1.get().getStatus());

        // Case-2: Delete KeyValueTable where KVTable does not exist within the Scope.
        ResultObserver<DeleteKVTableStatus> result2 = new ResultObserver<>();
        assertTrue("The requested Scope must get created", this.controller.createScope(newScope2).join());
        this.controllerService.deleteKeyValueTable(ModelHelper.createKeyValueTableInfo(newScope1,
                newKVT2.getKeyValueTableName()), result2);
        assertEquals("The requested table to delete must not be found for invalid Table name",
                DeleteKVTableStatus.Status.TABLE_NOT_FOUND, result2.get().getStatus());

        final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(5);
        final StreamConfiguration strmConfig = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
                .build();

        // Creating Stream1 under Scope1.
        assertTrue("The requested Stream must get created", this.controller.createStream(newScope1,
                streamName1, strmConfig).join());

        val strmSegments1 = this.controller.getCurrentSegments(newScope1, streamName1).join();
        for (val s : strmSegments1.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.streamSegmentStore.getStreamSegmentInfo(s.getScopedName(), TIMEOUT).join();
        }

        // Case-3: KVTable and its table segments should get deleted but Stream and its stream segments should exist and +
        // we should be able to read/write from them.
        ResultObserver<DeleteKVTableStatus> result3 = new ResultObserver<>();
        this.controllerService.deleteKeyValueTable(ModelHelper.createKeyValueTableInfo(newScope1,
                newKVT1.getKeyValueTableName()), result3);
        assertEquals("The requested table to delete must not be found for invalid Scope name",
                DeleteKVTableStatus.Status.SUCCESS, result3.get().getStatus());

        for (val s : tableSegments1.getSegments()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "Segment " + s + " has not been deleted.",
                    () -> this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new
                            ByteArraySegment(new byte[1])), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }

        for (val s : strmSegments1.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.streamSegmentStore.getStreamSegmentInfo(s.getScopedName(), TIMEOUT).join();
        }

        // To read and write few sample events on the stream having same name as the KVTable.
        readWriteOnStream(newScope1, streamName1, numEvents, routingKey, streamEvent, readerGroupName, readerName);

        // Creating Stream2 under Scope2.
        assertTrue("The requested Stream must get created", this.controller.createStream(newScope2,
                streamName2, strmConfig).join());

        val strmSegments2 = this.controller.getCurrentSegments(newScope2, streamName2).join();
        for (val s : strmSegments2.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.streamSegmentStore.getStreamSegmentInfo(s.getScopedName(), TIMEOUT).join();
        }

        // Creating KVT2 under Scope2.
        assertTrue("The requested KVT must get created",
                this.controller.createKeyValueTable(newScope2, newKVT2.getKeyValueTableName(),
                        config).join());

        assertEquals("Table partition value should be same as passed", config.getPartitionCount(),
                Futures.getAndHandleExceptions(this.controller.getCurrentSegmentsForKeyValueTable(newScope2,
                        newKVT2.getKeyValueTableName()), RuntimeException::new).getSegments().size());

        val tableSegments2 = this.controller.getCurrentSegmentsForKeyValueTable(newScope2,
                newKVT2.getKeyValueTableName()).join();

        for (val s : tableSegments2.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT).join();
        }

        // Cleans up stream resources by sealing existing stream segments, deleting stream and scope.
        cleanUpStream(newScope2, streamName2);

        // Case-4: Stream and its stream segments should get deleted but KVTable and its table segments should +
        // exist and we should be able to read/write from them.
        for (val s : strmSegments2.getSegments()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "Segment " + s + " has not been deleted.",
                    () -> this.streamSegmentStore.getStreamSegmentInfo(s.getScopedName(), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }

        for (val s : tableSegments2.getSegments()) {
            // We know there's nothing in these segments. But if the segments hadn't been created, then this will throw
            // an exception.
            this.tableStore.get(s.getKVTScopedName(), Collections.singletonList(new ByteArraySegment(new byte[1])), TIMEOUT).join();
        }

        String keyFamily = "KeyFamily1";
        // inserts and fetches few Table entries for validate basic functioning of KVTable
        insertAndGetFromKVTable(newScope2, newKVT2, keyFamily);
    }

    /**
     * Test listing KVTables under a scope with only Streams and under empty Scope.
     */
    @Test
    public void testListKVTables() throws ExecutionException, InterruptedException {
        final String newScope1 = "TestScope1";
        final String newScope2 = "TestScope2";
        final String newStream1 = "TestStream1-" + randomAlphanumeric(5);
        final String newStream2 = "TestStream2-" + randomAlphanumeric(5);
        final int streamSegments = 5;

        final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(streamSegments);
        final StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy).build();

        // test Scope1.
        assertTrue("The requested Scope must get created", this.controller.createScope(newScope1).join());
        assertTrue("The requested Stream must get created", this.controller.createStream(newScope1,
                newStream1, config).join());
        assertTrue("The requested Stream must get created", this.controller.createStream(newScope1,
                newStream2, config).join());

        Iterator<Stream> streamItr1 = this.controller.listStreams(newScope1).asIterator();
        assertTrue("Stream list iterator1 should not be empty", streamItr1.hasNext());
        Set<Stream> streamSet = new HashSet<>();
        while (streamItr1.hasNext()) {
            Stream stream = streamItr1.next();
            assertEquals("Iterated stream must be under same scope", newScope1,
                    stream.getScope());
            if (!(stream.getStreamName().contains("_MARK"))) {
                streamSet.add(stream);
            }
        }
        log.info("Stream set: {}", streamSet);
        assertEquals("The Stream set must have two streams only", 2, streamSet.size());
        assertTrue(streamSet.stream().anyMatch(x -> x.getStreamName().equals(newStream1)));
        assertTrue(streamSet.stream().anyMatch(x -> x.getStreamName().equals(newStream2)));

        assertEquals("The stream segment count should be same as passed", streamSegments,
                Futures.getAndHandleExceptions(this.controller.getCurrentSegments(newScope1, newStream1),
                        RuntimeException::new).getSegments().size());
        assertEquals("The stream segment count should be same as passed", streamSegments,
                Futures.getAndHandleExceptions(this.controller.getCurrentSegments(newScope1, newStream2),
                        RuntimeException::new).getSegments().size());

        Iterator<KeyValueTableInfo> tableItr1 = this.controller.listKeyValueTables(newScope1).asIterator();
        assertFalse("Table List Iterator 1 with streams should not have any table entries in it",
                tableItr1.hasNext());
        AssertExtensions.assertThrows(NoSuchElementException.class, () -> tableItr1.next());

        // test Scope2.
        assertTrue("The requested Scope must get created", this.controller.createScope(newScope2).join());
        Iterator<Stream> streamItr2 = this.controller.listStreams(newScope2).asIterator();
        assertFalse("Stream list iterator2 should be empty", streamItr2.hasNext());

        Iterator<KeyValueTableInfo> tableItr2 = this.controller.listKeyValueTables(newScope2).asIterator();
        assertFalse("Table List Iterator 2 without streams should not have any table entries in it",
                tableItr2.hasNext());
        AssertExtensions.assertThrows(NoSuchElementException.class, () -> tableItr2.next());

        // Cleans up stream resources by sealing existing stream segments, deleting stream and scope.
        cleanUpStream(newScope1, newStream1);
        cleanUpStream(newScope1, newStream2);
        cleanUpScope(newScope1);
        cleanUpScope(newScope2);
    }

    private void insertAndGetFromKVTable(String scope, KeyValueTableInfo kvt, String keyFamily) {
        @Cleanup
        KeyValueTableFactory newKeyValueTableFactory = new KeyValueTableFactoryImpl(scope, this.controller, this.connectionPool);
        @Cleanup
        KeyValueTable<Integer, String> newKeyValueTable = newKeyValueTableFactory.forKeyValueTable(kvt.getKeyValueTableName(),
                KEY_SERIALIZER, VALUE_SERIALIZER, KeyValueTableClientConfiguration.builder().build());

        LinkedHashMap<Integer, String> entryMap1 = new LinkedHashMap<>();
        Integer[] keyArray = new Integer[10];
        String[] valueArray1 = new String[10];
        // Creating 10 new entries to insert in the Table.
        for (int i = 0; i < keyArray.length; i++) {
            keyArray[i] = getKeyID();
            valueArray1[i] = getValue();
            entryMap1.put(keyArray[i], valueArray1[i]);
        }

        List<Version> versionList1 = newKeyValueTable.putAll(keyFamily, entryMap1.entrySet()).join();
        log.info("Version list1: {}", versionList1);
        assertNotNull("Version list1 should not be empty", versionList1);
        assertEquals("Version list1 size should be same as entry list1 size",
                versionList1.size(), entryMap1.size());

        List<Integer> keyList = new ArrayList<>();
        for (int i = 0; i < keyArray.length; i++) {
            keyList.add(keyArray[i]);
        }
        log.info("Key list: {}", keyList);
        List<TableEntry<Integer, String>> getEntryList1 = newKeyValueTable.getAll(keyFamily, keyList).join();
        log.info("Get Entry List1: {}", getEntryList1);
        assertNotNull("Get Entry List1 should not be empty", getEntryList1);
        assertEquals("Get Entry List1 size should be same as get entry list1 size",
                getEntryList1.size(), entryMap1.size());
        List<Integer> entryKeyList = new ArrayList<>();
        for (Integer key: entryMap1.keySet()) {
            entryKeyList.add(key);
        }
        log.info("entryKeyList: {}", entryKeyList);
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("Corresponding key for getEntryList should be as inserted",
                    keyList.get(i), getEntryList1.get(i).getKey().getKey());
            assertEquals("Corresponding key for entryList should be as inserted",
                    entryKeyList.get(i), getEntryList1.get(i).getKey().getKey());
        }

        // Comparing each & every entry values of both entryList and getEntryList.
        for (int i = 0; i < keyArray.length; i++) {
            assertEquals("All string values of both entryList1 and getEntryList1 should be equal",
                    entryMap1.get(entryKeyList.get(i)), getEntryList1.get(i).getValue());
        }
        log.info("KVPair operations succeeds over KVTable");
    }

    private void cleanUpStream(String scope, String stream)
            throws ExecutionException, InterruptedException {
        // seal the stream.
        log.info("Sealing stream {}", stream);
        CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(scope, stream);
        assertTrue(sealStreamStatus.get());
        // delete the stream.
        log.info("Deleting stream {}", stream);
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(scope, stream);
        assertTrue(deleteStreamStatus.get());
    }

    private void cleanUpScope(String scope) throws ExecutionException, InterruptedException {
        log.info("Deleting scope {}", scope);
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(scope);
        assertTrue(deleteScopeStatus.get());
    }

    private void readWriteOnStream(String newScope, String streamName, int numEvents, String routingKey,
                                   String streamEvent, String readerGroupName, String readerName) {
        int writeCount = 0;
        int readCount = 0;
        try (ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(newScope, this.controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(newScope, this.controller, clientFactory)) {

            log.info("Creating writer");
            @Cleanup
            final EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                    new JavaSerializer<>(), EventWriterConfig.builder().build());

            // start writing events to the stream.
            for (int i = 0; i < numEvents; i++) {
                log.info("Writing event {}", streamEvent);
                writer.writeEvent(routingKey, streamEvent).join();
                writeCount++;
                writer.flush();
            }
            log.info("Closing writer {}", writer);
            writer.close();

            // create a reader group.
            log.info("Creating Reader group : {}", readerGroupName);

            readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder()
                    .stream(Stream.of(newScope, streamName)).build());
            log.info("Reader group name {} ", readerGroupManager.getReaderGroup(readerGroupName).getGroupName());
            log.info("Reader group scope {}", readerGroupManager.getReaderGroup(readerGroupName).getScope());

            log.info("Creating reader");
            @Cleanup
            final EventStreamReader<String> reader = clientFactory.createReader(readerName,
                    readerGroupName,
                    new JavaSerializer<>(),
                    ReaderConfig.builder().build());

            // start reading events from the stream.
            while (!(readCount == writeCount)) {
                final String eventRead = reader.readNextEvent(SECONDS.toMillis(2)).getEvent();
                log.info("Reading event {}", eventRead);
                if (eventRead != null) {
                    // update if event read is not null.
                    readCount++;
                }
            }
            log.info("Closing reader {}", reader);
            reader.close();

            //delete readergroup
            log.info("Deleting readergroup {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }
        assertEquals("Number of events written & read should be equal", writeCount, readCount);
        log.info("Read write test on stream succeeds");
    }
    private Integer getKeyID() { return random.nextInt(endKeyRange - startKeyRange) + startKeyRange; }

    private String getValue() {
        String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        StringBuilder sb = new StringBuilder(valueSize);

        for (int i = 0; i < valueSize; i++) {
            int index = (int) (alphaNumericString.length() * random.nextDouble());
            sb.append(alphaNumericString.charAt(index));
        }
        return sb.toString();
    }

    private KeyValueTableInfo newKeyValueTableName() {
        return new KeyValueTableInfo(SCOPE, String.format("KVT-%d", System.nanoTime()));
    }

    private KeyValueTableInfo newKeyValueTableName(String scopeName) {
        return new KeyValueTableInfo(scopeName, String.format("KVT-%d", System.nanoTime()));
    }

    @Override
    protected KeyValueTable<Integer, String> createKeyValueTable() {
        return createKeyValueTable(KEY_SERIALIZER, VALUE_SERIALIZER);
    }

    @Override
    protected <K, V> KeyValueTable<K, V> createKeyValueTable(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        val kvt = newKeyValueTableName();
        boolean created = this.controller.createKeyValueTable(kvt.getScope(), kvt.getKeyValueTableName(), DEFAULT_CONFIG).join();
        Assert.assertTrue(created);
        return this.keyValueTableFactory.forKeyValueTable(kvt.getKeyValueTableName(), keySerializer, valueSerializer,
                KeyValueTableClientConfiguration.builder().build());
    }

    static class ResultObserver<T> implements StreamObserver<T> {
        private T result = null;
        private Throwable error;
        private final AtomicBoolean completed = new AtomicBoolean(false);

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
            synchronized (this) {
                error = t;
                completed.set(true);
                this.notifyAll();
            }
        }

        @Override
        public void onCompleted() {
            synchronized (this) {
                completed.set(true);
                this.notifyAll();
            }
        }

        @SneakyThrows
        public T get() {
            synchronized (this) {
                while (!completed.get()) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        return null;
                    }
                }
            }
            if (error != null) {
                throw error;
            } else {
                return result;
            }
        }
    }
}
