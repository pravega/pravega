/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.segment.StreamSegmentNameUtils;
import com.emc.pravega.common.util.AsyncMap;
import com.emc.pravega.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.AttributeUpdate;
import com.emc.pravega.service.contracts.AttributeUpdateType;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.OperationLog;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.SegmentMetadataComparer;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentMapOperation;
import com.emc.pravega.service.server.logs.operations.TransactionMapOperation;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.IntentionalException;
import com.emc.pravega.testcommon.ThreadPooledTestSuite;
import com.google.common.util.concurrent.Service;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for StreamSegmentMapper class.
 */
public class StreamSegmentMapperTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 123;
    private static final int ATTRIBUTE_COUNT = 10;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the ability of the StreamSegmentMapper to create a new StreamSegment.
     */
    @Test
    public void testCreateNewStreamSegment() {
        final int segmentCount = 10;
        final int transactionsPerSegment = 5;

        @Cleanup
        TestContext context = new TestContext();
        setupOperationLog(context);

        HashSet<String> storageSegments = new HashSet<>();
        setupStorageCreateHandler(context, storageSegments);
        setupStorageGetHandler(context, storageSegments, segmentName -> new StreamSegmentInformation(segmentName, 0, false, false, new ImmutableDate()));

        // Create some Segments and Transaction and verify they are properly created and registered.
        for (int i = 0; i < segmentCount; i++) {
            String segmentName = getName(i);
            val segmentAttributes = createAttributes(ATTRIBUTE_COUNT);
            context.mapper.createNewStreamSegment(segmentName, segmentAttributes, TIMEOUT).join();
            assertStreamSegmentCreated(segmentName, segmentAttributes, context);

            for (int j = 0; j < transactionsPerSegment; j++) {
                val transactionAttributes = createAttributes(ATTRIBUTE_COUNT);
                String transactionName = context.mapper.createNewTransactionStreamSegment(segmentName, UUID.randomUUID(), transactionAttributes, TIMEOUT).join();
                assertTransactionCreated(transactionName, segmentName, transactionAttributes, context);
            }
        }
    }

    /**
     * Tests the ability of the StreamSegmentMapper to create a new StreamSegment if there are Storage and/or OperationLog Failures.
     */
    @Test
    public void testCreateNewStreamSegmentWithFailures() {
        final String segmentName = "NewSegment";
        final long segmentId = 1;

        @Cleanup
        TestContext context = new TestContext();
        setupOperationLog(context); // Operation Log works just fine.

        HashSet<String> storageSegments = new HashSet<>();

        // 1. Create fails with StreamSegmentExistsException.
        context.storage.createHandler = name -> FutureHelpers.failedFuture(new StreamSegmentExistsException("intentional"));
        AssertExtensions.assertThrows(
                "createNewStreamSegment did not fail when Segment already exists.",
                () -> context.mapper.createNewStreamSegment(segmentName, null, TIMEOUT),
                ex -> ex instanceof StreamSegmentExistsException);
        Assert.assertEquals("Segment was registered in the metadata even if it failed to be created (StreamSegmentExistsException).", ContainerMetadata.NO_STREAM_SEGMENT_ID, context.metadata.getStreamSegmentId(segmentName));

        // 2. Create fails with random exception.
        context.storage.createHandler = name -> FutureHelpers.failedFuture(new IntentionalException());
        AssertExtensions.assertThrows(
                "createNewStreamSegment did not fail when random exception was thrown.",
                () -> context.mapper.createNewStreamSegment(segmentName, null, TIMEOUT),
                ex -> ex instanceof IntentionalException);
        Assert.assertEquals("Segment was registered in the metadata even if it failed to be created (IntentionalException).", ContainerMetadata.NO_STREAM_SEGMENT_ID, context.metadata.getStreamSegmentId(segmentName));

        // Manually create the StreamSegment and test the Transaction creation.
        storageSegments.add(segmentName);
        context.metadata.mapStreamSegmentId(segmentName, segmentId);

        // 3. Create-Transaction fails with StreamSegmentExistsException.
        context.storage.createHandler = name -> FutureHelpers.failedFuture(new StreamSegmentExistsException("intentional"));
        AssertExtensions.assertThrows(
                "createNewTransactionStreamSegment did not fail when Segment already exists.",
                () -> context.mapper.createNewTransactionStreamSegment(segmentName, UUID.randomUUID(), null, TIMEOUT),
                ex -> ex instanceof StreamSegmentExistsException);
        Assert.assertEquals("Transaction was registered in the metadata even if it failed to be created (StreamSegmentExistsException).", 1, context.metadata.getAllStreamSegmentIds().size());

        // 4. Create-Transaction fails with random exception.
        context.storage.createHandler = name -> FutureHelpers.failedFuture(new IntentionalException());
        AssertExtensions.assertThrows(
                "createNewTransactionStreamSegment did not fail when random exception was thrown.",
                () -> context.mapper.createNewTransactionStreamSegment(segmentName, UUID.randomUUID(), null, TIMEOUT),
                ex -> ex instanceof IntentionalException);
        Assert.assertEquals("Transaction was registered in the metadata even if it failed to be created (IntentionalException).", 1, context.metadata.getAllStreamSegmentIds().size());

        // Check how this behaves when Storage works, but the OperationLog cannot process the operations.
        context.operationLog.addHandler = op -> FutureHelpers.failedFuture(new TimeoutException("intentional"));
        setupStorageCreateHandler(context, storageSegments);
        setupStorageGetHandler(context, storageSegments, sn -> new StreamSegmentInformation(sn, 0, false, false, new ImmutableDate()));

        // 5. When Creating a Transaction.
        AssertExtensions.assertThrows(
                "createNewTransactionStreamSegment did not fail when OperationLog threw an exception.",
                () -> context.mapper.createNewTransactionStreamSegment(segmentName, UUID.randomUUID(), null, TIMEOUT),
                ex -> ex instanceof TimeoutException);
        Assert.assertEquals("Transaction was registered in the metadata even if it failed to be processed by the OperationLog.", 1, context.metadata.getAllStreamSegmentIds().size());
        Assert.assertEquals("Transaction was not created in Storage even if the failure was post-storage (in OperationLog processing).", 2, storageSegments.size());

        // 6. When creating a new StreamSegment.
        AssertExtensions.assertThrows(
                "createNewStreamSegment did not fail when OperationLog threw an exception.",
                () -> context.mapper.createNewStreamSegment(segmentName + "foo", null, TIMEOUT),
                ex -> ex instanceof TimeoutException);
        Assert.assertEquals("Segment was registered in the metadata even if it failed to be processed by the OperationLog.", 1, context.metadata.getAllStreamSegmentIds().size());
        Assert.assertEquals("Segment was not created in Storage even if the failure was post-storage (in OperationLog processing).", 3, storageSegments.size());
    }

    /**
     * Tests the ability of the StreamSegmentMapper to generate/return the Id of an existing StreamSegment.
     */
    @Test
    public void testGetOrAssignStreamSegmentId() {
        final int segmentCount = 10;
        final int transactionsPerSegment = 5;

        @Cleanup
        TestContext context = new TestContext();

        HashSet<String> storageSegments = new HashSet<>();
        for (int i = 0; i < segmentCount; i++) {
            String segmentName = getName(i);
            storageSegments.add(segmentName);
            setAttributes(segmentName, storageSegments.size() % ATTRIBUTE_COUNT, context);

            for (int j = 0; j < transactionsPerSegment; j++) {
                // There is a small chance of a name conflict here, but we don't care. As long as we get at least one
                // Transaction per segment, we should be fine.
                String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
                storageSegments.add(transactionName);
                setAttributes(transactionName, storageSegments.size() % ATTRIBUTE_COUNT, context);
            }
        }

        // We setup all necessary handlers, except the one for create. We do not need to create new Segments here.
        setupOperationLog(context);
        Predicate<String> isSealed = segmentName -> segmentName.hashCode() % 2 == 0;
        Function<String, Long> getInitialLength = segmentName -> (long) Math.abs(segmentName.hashCode());
        setupStorageGetHandler(context, storageSegments, segmentName -> new StreamSegmentInformation(segmentName, getInitialLength.apply(segmentName), isSealed.test(segmentName), false, new ImmutableDate()));

        // First, map all the parents (stand-alone segments).
        for (String name : storageSegments) {
            if (StreamSegmentNameUtils.getParentStreamSegmentName(name) == null) {
                long id = context.mapper.getOrAssignStreamSegmentId(name, TIMEOUT).join();
                Assert.assertNotEquals("No id was assigned for StreamSegment " + name, ContainerMetadata.NO_STREAM_SEGMENT_ID, id);
                SegmentMetadata sm = context.metadata.getStreamSegmentMetadata(id);
                Assert.assertNotNull("No metadata was created for StreamSegment " + name, sm);
                long expectedLength = getInitialLength.apply(name);
                boolean expectedSeal = isSealed.test(name);
                Assert.assertEquals("Metadata does not have the expected length for StreamSegment " + name, expectedLength, sm.getDurableLogLength());
                Assert.assertEquals("Metadata does not have the expected value for isSealed for StreamSegment " + name, expectedSeal, sm.isSealed());

                val segmentState = context.stateStore.get(name, TIMEOUT).join();
                Map<UUID, Long> expectedAttributes = segmentState == null ? null : segmentState.getAttributes();
                SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in metadata for StreamSegment " + name, expectedAttributes, sm);
            }
        }

        // Now, map all the Transactions.
        for (String name : storageSegments) {
            String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(name);
            if (parentName != null) {
                long id = context.mapper.getOrAssignStreamSegmentId(name, TIMEOUT).join();
                Assert.assertNotEquals("No id was assigned for Transaction " + name, ContainerMetadata.NO_STREAM_SEGMENT_ID, id);
                SegmentMetadata sm = context.metadata.getStreamSegmentMetadata(id);
                Assert.assertNotNull("No metadata was created for Transaction " + name, sm);
                long expectedLength = getInitialLength.apply(name);
                boolean expectedSeal = isSealed.test(name);
                Assert.assertEquals("Metadata does not have the expected length for Transaction " + name, expectedLength, sm.getDurableLogLength());
                Assert.assertEquals("Metadata does not have the expected value for isSealed for Transaction " + name, expectedSeal, sm.isSealed());

                val segmentState = context.stateStore.get(name, TIMEOUT).join();
                Map<UUID, Long> expectedAttributes = segmentState == null ? null : segmentState.getAttributes();
                SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in metadata for Transaction " + name, expectedAttributes, sm);

                // Check parenthood.
                Assert.assertNotEquals("No parent defined in metadata for Transaction " + name, ContainerMetadata.NO_STREAM_SEGMENT_ID, sm.getParentId());
                long parentId = context.metadata.getStreamSegmentId(parentName);
                Assert.assertEquals("Unexpected parent defined in metadata for Transaction " + name, parentId, sm.getParentId());
            }
        }
    }

    /**
     * Tests the behavior of getOrAssignStreamSegmentId when the requested StreamSegment has been deleted.
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWhenDeleted() {
        final int segmentCount = 1;

        HashSet<String> storageSegments = new HashSet<>();
        for (int i = 0; i < segmentCount; i++) {
            storageSegments.add(getName(i));
        }

        // We setup all necessary handlers, except the one for create. We do not need to create new Segments here.
        @Cleanup
        TestContext context = new TestContext();
        setupOperationLog(context);
        setupStorageGetHandler(context, storageSegments, segmentName -> new StreamSegmentInformation(segmentName, 0, false, false, new ImmutableDate()));

        // Map all the segments, then delete them, then verify behavior.
        for (String name : storageSegments) {
            context.mapper.getOrAssignStreamSegmentId(name, TIMEOUT).join();
            context.metadata.deleteStreamSegment(name);
            AssertExtensions.assertThrows(
                    "getOrAssignStreamSegmentId did not return appropriate exception when the segment has been deleted.",
                    () -> context.mapper.getOrAssignStreamSegmentId(name, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the ability of the StreamSegmentMapper to generate/return the Id of an existing StreamSegment, when dealing
     * with Storage failures (or inexistent StreamSegments).
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWithFailures() {
        final String segmentName = "Segment";
        final String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());

        HashSet<String> storageSegments = new HashSet<>();
        storageSegments.add(segmentName);
        storageSegments.add(transactionName);

        @Cleanup
        TestContext context = new TestContext();
        setupOperationLog(context);

        // 1. Unable to access storage.
        context.storage.getInfoHandler = sn -> FutureHelpers.failedFuture(new IntentionalException());
        AssertExtensions.assertThrows(
                "getOrAssignStreamSegmentId did not throw the right exception when the Storage access failed.",
                () -> context.mapper.getOrAssignStreamSegmentId(segmentName, TIMEOUT),
                ex -> ex instanceof IntentionalException);
        AssertExtensions.assertThrows(
                "getOrAssignStreamSegmentId did not throw the right exception when the Storage access failed.",
                () -> context.mapper.getOrAssignStreamSegmentId(transactionName, TIMEOUT),
                ex -> ex instanceof IntentionalException);

        // 2a. StreamSegmentNotExists (Stand-Alone segment)
        setupStorageGetHandler(context, storageSegments, sn -> new StreamSegmentInformation(sn, 0, false, false, new ImmutableDate()));
        AssertExtensions.assertThrows(
                "getOrAssignStreamSegmentId did not throw the right exception for a non-existent stand-alone StreamSegment.",
                () -> context.mapper.getOrAssignStreamSegmentId(segmentName + "foo", TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // 2b. Transaction does not exist.
        final String inexistentTransactionName = StreamSegmentNameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
        AssertExtensions.assertThrows(
                "getOrAssignStreamSegmentId did not throw the right exception for a non-existent Transaction.",
                () -> context.mapper.getOrAssignStreamSegmentId(inexistentTransactionName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // 2c. Transaction exists, but not its parent.
        final String noValidParentTransactionName = StreamSegmentNameUtils.getTransactionNameFromId("foo", UUID.randomUUID());
        storageSegments.add(noValidParentTransactionName);
        AssertExtensions.assertThrows(
                "getOrAssignStreamSegmentId did not throw the right exception for a Transaction with an inexistent parent.",
                () -> context.mapper.getOrAssignStreamSegmentId(noValidParentTransactionName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // 2d. Attribute fetch failure.
        val testStateStore = new TestStateStore();
        val badMapper = new StreamSegmentMapper(context.metadata, context.operationLog, testStateStore, context.storage, executorService());
        val segmentName2 = segmentName + "2";
        val transactionName2 = StreamSegmentNameUtils.getTransactionNameFromId(segmentName2, UUID.randomUUID());
        context.storage.getInfoHandler = sn -> CompletableFuture.completedFuture(new StreamSegmentInformation(sn, 0, false, false, new ImmutableDate()));
        testStateStore.getHandler = () -> FutureHelpers.failedFuture(new IntentionalException("intentional"));

        AssertExtensions.assertThrows(
                "getOrAssignStreamSegmentId did not throw the right exception for a Segment when attributes could not be retrieved.",
                () -> badMapper.getOrAssignStreamSegmentId(segmentName2, TIMEOUT),
                ex -> ex instanceof IntentionalException);

        AssertExtensions.assertThrows(
                "getOrAssignStreamSegmentId did not throw the right exception for a Transaction when attributes could not be retrieved.",
                () -> badMapper.getOrAssignStreamSegmentId(transactionName2, TIMEOUT),
                ex -> ex instanceof IntentionalException);
    }

    /**
     * Tests the ability of the StreamSegmentMapper to generate/return the Id of an existing StreamSegment, with concurrent requests.
     */
    @Test
    public void testGetOrAssignStreamSegmentIdWithConcurrency() throws Exception {
        // We setup a delay in the OperationLog process. We only do this for a stand-alone StreamSegment because the process
        // is driven by the same code for Transactions as well.
        final String segmentName = "Segment";
        final long segmentId = 12345;

        HashSet<String> storageSegments = new HashSet<>();
        storageSegments.add(segmentName);

        @Cleanup
        TestContext context = new TestContext();
        setupStorageGetHandler(context, storageSegments, sn -> new StreamSegmentInformation(sn, 0, false, false, new ImmutableDate()));
        CompletableFuture<Long> initialAddFuture = new CompletableFuture<>();
        AtomicBoolean operationLogInvoked = new AtomicBoolean(false);
        context.operationLog.addHandler = op -> {
            if (!(op instanceof StreamSegmentMapOperation)) {
                return FutureHelpers.failedFuture(new IllegalArgumentException("unexpected operation"));
            }
            if (operationLogInvoked.getAndSet(true)) {
                return FutureHelpers.failedFuture(new IllegalStateException("multiple calls to OperationLog.add"));
            }

            // Need to set SegmentId on operation.
            ((StreamSegmentMapOperation) op).setStreamSegmentId(segmentId);
            return initialAddFuture;
        };

        CompletableFuture<Long> firstCall = context.mapper.getOrAssignStreamSegmentId(segmentName, TIMEOUT);
        CompletableFuture<Long> secondCall = context.mapper.getOrAssignStreamSegmentId(segmentName, TIMEOUT);
        Thread.sleep(20);
        Assert.assertFalse("getOrAssignStreamSegmentId (first call) returned before OperationLog finished.", firstCall.isDone());
        Assert.assertFalse("getOrAssignStreamSegmentId (second call) returned before OperationLog finished.", secondCall.isDone());
        initialAddFuture.complete(1L);
        long firstCallResult = firstCall.get(100, TimeUnit.MILLISECONDS);
        long secondCallResult = secondCall.get(100, TimeUnit.MILLISECONDS);

        Assert.assertEquals("Two concurrent calls to getOrAssignStreamSegmentId for the same StreamSegment returned different ids.", firstCallResult, secondCallResult);
    }

    private String getName(long segmentId) {
        return String.format("Segment_%d", segmentId);
    }

    private Collection<AttributeUpdate> createAttributes(int count) {
        Collection<AttributeUpdate> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            AttributeUpdateType ut = AttributeUpdateType.values()[i % AttributeUpdateType.values().length];
            result.add(new AttributeUpdate(UUID.randomUUID(), ut, i));
        }

        return result;
    }

    private void setAttributes(String segmentName, int count, TestContext context) {
        if (count != 0) {
            val attributes = createAttributes(count)
                    .stream()
                    .collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
            context.stateStore.put(
                    segmentName,
                    new SegmentState(new StreamSegmentInformation(segmentName, 0, false, false, attributes, new ImmutableDate())),
                    TIMEOUT).join();
        }
    }

    private void assertStreamSegmentCreated(String segmentName, Collection<AttributeUpdate> attributeUpdates, TestContext context) {
        SegmentProperties storageProperties = context.storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
        Assert.assertNotNull("No segment has been created in the Storage for " + segmentName, storageProperties);

        long segmentId = context.metadata.getStreamSegmentId(segmentName);
        Assert.assertNotEquals("Segment '" + segmentName + "' has not been registered in the metadata.", ContainerMetadata.NO_STREAM_SEGMENT_ID, segmentId);
        SegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
        Assert.assertNotNull("Segment '" + segmentName + "' has not been registered in the metadata.", sm);
        Assert.assertEquals("Wrong segment name in metadata .", segmentName, sm.getName());
        val attributes = attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
        AssertExtensions.assertMapEquals("Wrong attributes.", attributes, sm.getAttributes());
    }

    private void assertTransactionCreated(String transactionName, String segmentName, Collection<AttributeUpdate> attributes, TestContext context) {
        assertStreamSegmentCreated(transactionName, attributes, context);
        long parentId = context.metadata.getStreamSegmentId(segmentName);
        long transactionId = context.metadata.getStreamSegmentId(transactionName);
        SegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
        Assert.assertNotEquals("Transaction StreamSegment is not mapped to any parent.", ContainerMetadata.NO_STREAM_SEGMENT_ID, transactionMetadata.getParentId());
        Assert.assertEquals("Transaction StreamSegment is not mapped to the correct parent.", parentId, transactionMetadata.getParentId());
    }

    private void setupOperationLog(TestContext context) {
        AtomicLong seqNo = new AtomicLong();
        context.operationLog.addHandler = op -> {
            if (op instanceof StreamSegmentMapOperation) {
                StreamSegmentMapOperation mapOp = (StreamSegmentMapOperation) op;
                mapOp.setStreamSegmentId(seqNo.incrementAndGet());
                UpdateableSegmentMetadata segmentMetadata = context.metadata.mapStreamSegmentId(mapOp.getStreamSegmentName(), mapOp.getStreamSegmentId());
                segmentMetadata.setStorageLength(0);
                segmentMetadata.setDurableLogLength(mapOp.getLength());
                if (mapOp.isSealed()) {
                    segmentMetadata.markSealed();
                }

                segmentMetadata.updateAttributes(mapOp.getAttributes());
            } else if (op instanceof TransactionMapOperation) {
                TransactionMapOperation mapOp = (TransactionMapOperation) op;
                mapOp.setStreamSegmentId(seqNo.incrementAndGet());
                UpdateableSegmentMetadata segmentMetadata = context.metadata.mapStreamSegmentId(mapOp.getStreamSegmentName(), mapOp.getStreamSegmentId(), mapOp.getParentStreamSegmentId());
                segmentMetadata.setStorageLength(0);
                segmentMetadata.setDurableLogLength(mapOp.getLength());
                if (mapOp.isSealed()) {
                    segmentMetadata.markSealed();
                }

                segmentMetadata.updateAttributes(mapOp.getAttributes());
            }

            return CompletableFuture.completedFuture(seqNo.incrementAndGet());
        };
    }

    private void setupStorageCreateHandler(TestContext context, HashSet<String> storageSegments) {
        context.storage.createHandler = segmentName -> {
            synchronized (storageSegments) {
                if (storageSegments.contains(segmentName)) {
                    return FutureHelpers.failedFuture(new StreamSegmentExistsException(segmentName));
                } else {
                    storageSegments.add(segmentName);
                    return CompletableFuture.completedFuture(new StreamSegmentInformation(segmentName, 0, false, false, new ImmutableDate()));
                }
            }
        };
    }

    private void setupStorageGetHandler(TestContext context, HashSet<String> storageSegments, Function<String, SegmentProperties> infoGetter) {
        context.storage.getInfoHandler = segmentName -> {
            synchronized (storageSegments) {
                if (!storageSegments.contains(segmentName)) {
                    return FutureHelpers.failedFuture(new StreamSegmentNotExistsException(segmentName));
                } else {
                    return CompletableFuture.completedFuture(infoGetter.apply(segmentName));
                }
            }
        };
    }

    //region TestContext

    private class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata metadata;
        final TestStorage storage;
        final TestOperationLog operationLog;
        final InMemoryStateStore stateStore;
        final StreamSegmentMapper mapper;

        TestContext() {
            this.storage = new TestStorage();
            this.operationLog = new TestOperationLog();
            this.metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            this.stateStore = new InMemoryStateStore();
            this.mapper = new StreamSegmentMapper(this.metadata, this.operationLog, this.stateStore, this.storage, executorService());
        }

        @Override
        public void close() {
            this.storage.close();
            this.operationLog.close();
        }
    }

    //endregion

    //region TestOperationLog

    private static class TestOperationLog implements OperationLog {
        Function<Operation, CompletableFuture<Long>> addHandler;

        @Override
        public CompletableFuture<Long> add(Operation operation, Duration timeout) {
            return addHandler.apply(operation);
        }

        //region Unimplemented Methods

        @Override
        public int getId() {
            return -1;
        }

        @Override
        public void close() {

        }

        @Override
        public CompletableFuture<Void> truncate(long upToSequence, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout) {
            return null;
        }

        @Override
        public CompletableFuture<Void> operationProcessingBarrier(Duration timeout) {
            return null;
        }

        @Override
        public Service startAsync() {
            return null;
        }

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public State state() {
            return null;
        }

        @Override
        public Service stopAsync() {
            return null;
        }

        @Override
        public void awaitRunning() {

        }

        @Override
        public void awaitRunning(long timeout, TimeUnit unit) throws TimeoutException {

        }

        @Override
        public void awaitTerminated() {

        }

        @Override
        public void awaitTerminated(long timeout, TimeUnit unit) throws TimeoutException {

        }

        @Override
        public Throwable failureCause() {
            return null;
        }

        @Override
        public void addListener(Listener listener, Executor executor) {

        }

        //endregion
    }

    //endregion

    //region TestStorage

    private static class TestStorage implements Storage {
        Function<String, CompletableFuture<SegmentProperties>> createHandler;
        Function<String, CompletableFuture<SegmentProperties>> getInfoHandler;

        @Override
        public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
            return this.createHandler.apply(streamSegmentName);
        }

        @Override
        public CompletableFuture<Void> open(String streamSegmentName) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            return this.getInfoHandler.apply(streamSegmentName);
        }

        //region Unimplemented methods

        @Override
        public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
            throw new NotImplementedException();
        }

        @Override
        public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
            throw new NotImplementedException();
        }

        @Override
        public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
            throw new NotImplementedException();
        }

        @Override
        public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
            throw new NotImplementedException();
        }

        @Override
        public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName, Duration timeout) {
            throw new NotImplementedException();
        }

        @Override
        public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
            throw new NotImplementedException();
        }

        @Override
        public void close() {

        }

        //endregion
    }

    //endregion

    //region TestStateStore

    private static class TestStateStore implements AsyncMap<String, SegmentState> {
        Supplier<CompletableFuture<SegmentState>> getHandler;

        @Override
        public CompletableFuture<SegmentState> get(String key, Duration timeout) {
            return this.getHandler.get();
        }

        @Override
        public CompletableFuture<Void> remove(String key, Duration timeout) {
            // Not needed.
            return null;
        }

        @Override
        public CompletableFuture<Void> put(String key, SegmentState value, Duration timeout) {
            // Not needed.
            return null;
        }
    }

    //endregion
}
