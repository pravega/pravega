/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.service.logs;

import io.pravega.common.Exceptions;
import io.pravega.common.util.ImmutableDate;
import io.pravega.common.util.SequencedItemList;
import io.pravega.server.segmentstore.contracts.ReadResult;
import io.pravega.server.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.server.segmentstore.service.ContainerMetadata;
import io.pravega.server.segmentstore.service.DataCorruptionException;
import io.pravega.server.segmentstore.service.MetadataBuilder;
import io.pravega.server.segmentstore.service.ReadIndex;
import io.pravega.server.segmentstore.service.UpdateableContainerMetadata;
import io.pravega.server.segmentstore.service.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.server.segmentstore.service.logs.operations.MergeTransactionOperation;
import io.pravega.server.segmentstore.service.logs.operations.Operation;
import io.pravega.server.segmentstore.service.logs.operations.StorageOperation;
import io.pravega.server.segmentstore.service.logs.operations.StreamSegmentAppendOperation;
import io.pravega.server.segmentstore.service.logs.operations.StreamSegmentMapOperation;
import io.pravega.server.segmentstore.storage.mocks.InMemoryCache;
import io.pravega.test.common.AssertExtensions;
import java.io.InputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for MemoryStateUpdater class.
 */
public class MemoryStateUpdaterTests {
    /**
     * Tests the functionality of the process() method.
     */
    @Test
    public void testProcess() throws Exception {
        int segmentCount = 10;
        int operationCountPerType = 5;

        // Add to MTL + Add to ReadIndex (append; beginMerge).
        SequencedItemList<Operation> opLog = new SequencedItemList<>();
        ArrayList<TestReadIndex.MethodInvocation> methodInvocations = new ArrayList<>();
        TestReadIndex readIndex = new TestReadIndex(methodInvocations::add);
        @Cleanup
        InMemoryCache cache = new InMemoryCache("0");
        MemoryStateUpdater updater = new MemoryStateUpdater(opLog, readIndex);
        ArrayList<Operation> operations = populate(updater, segmentCount, operationCountPerType);

        // Verify they were properly processed.
        Assert.assertEquals("Unexpected number of items added to ReadIndex.", operations.size() - segmentCount * operationCountPerType, methodInvocations.size());

        Iterator<Operation> logIterator = opLog.read(-1, operations.size());
        int currentIndex = -1;
        int currentReadIndex = -1;
        while (logIterator.hasNext()) {
            currentIndex++;
            Operation expected = operations.get(currentIndex);
            Operation actual = logIterator.next();
            if (expected instanceof StorageOperation) {
                currentReadIndex++;
                TestReadIndex.MethodInvocation invokedMethod = methodInvocations.get(currentReadIndex);
                if (expected instanceof StreamSegmentAppendOperation) {
                    Assert.assertTrue("StreamSegmentAppendOperation was not added as a CachedStreamSegmentAppendOperation to the Memory Log.", actual instanceof CachedStreamSegmentAppendOperation);
                    StreamSegmentAppendOperation appendOp = (StreamSegmentAppendOperation) expected;
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was not added to the ReadIndex.", TestReadIndex.APPEND, invokedMethod.methodName);
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", appendOp.getStreamSegmentId(), invokedMethod.args.get("streamSegmentId"));
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", appendOp.getStreamSegmentOffset(), invokedMethod.args.get("offset"));
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", appendOp.getData(), invokedMethod.args.get("data"));
                } else if (expected instanceof MergeTransactionOperation) {
                    MergeTransactionOperation mergeOp = (MergeTransactionOperation) expected;
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was not added to the ReadIndex.", TestReadIndex.BEGIN_MERGE, invokedMethod.methodName);
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", mergeOp.getStreamSegmentId(), invokedMethod.args.get("targetStreamSegmentId"));
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", mergeOp.getStreamSegmentOffset(), invokedMethod.args.get("offset"));
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.", mergeOp.getTransactionSegmentId(), invokedMethod.args.get("sourceStreamSegmentId"));
                }
            }
        }

        // Test DataCorruptionException.
        AssertExtensions.assertThrows(
                "MemoryStateUpdater accepted an operation that was out of order.",
                () -> updater.process(new MergeTransactionOperation(1, 2)), // This does not have a SequenceNumber set, so it should trigger a DCE.
                ex -> ex instanceof DataCorruptionException);
    }

    /**
     * Tests the ability of the MemoryStateUpdater to delegate Enter/Exit recovery mode to the read index.
     */
    @Test
    public void testRecoveryMode() throws Exception {
        // Check it's properly delegated to Read index.
        SequencedItemList<Operation> opLog = new SequencedItemList<>();
        ArrayList<TestReadIndex.MethodInvocation> methodInvocations = new ArrayList<>();
        TestReadIndex readIndex = new TestReadIndex(methodInvocations::add);
        MemoryStateUpdater updater = new MemoryStateUpdater(opLog, readIndex);

        UpdateableContainerMetadata metadata1 = new MetadataBuilder(1).build();
        updater.enterRecoveryMode(metadata1);
        updater.exitRecoveryMode(true);

        Assert.assertEquals("Unexpected number of method invocations.", 2, methodInvocations.size());
        TestReadIndex.MethodInvocation enterRecovery = methodInvocations.get(0);
        Assert.assertEquals("ReadIndex.enterRecoveryMode was not called when expected.", TestReadIndex.ENTER_RECOVERY_MODE, enterRecovery.methodName);
        Assert.assertEquals("ReadIndex.enterRecoveryMode was called with the wrong arguments.", metadata1, enterRecovery.args.get("recoveryMetadataSource"));

        TestReadIndex.MethodInvocation exitRecovery = methodInvocations.get(1);
        Assert.assertEquals("ReadIndex.exitRecoveryMode was not called when expected.", TestReadIndex.EXIT_RECOVERY_MODE, exitRecovery.methodName);
        Assert.assertEquals("ReadIndex.exitRecoveryMode was called with the wrong arguments.", true, exitRecovery.args.get("successfulRecovery"));
    }

    /**
     * Tests the functionality of the flush() method, and that it can trigger future reads on the ReadIndex.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFlush() throws Exception {
        int segmentCount = 10;
        int operationCountPerType = 5;

        // Add to MTL + Add to ReadIndex (append; beginMerge).
        SequencedItemList<Operation> opLog = new SequencedItemList<>();
        ArrayList<TestReadIndex.MethodInvocation> methodInvocations = new ArrayList<>();
        TestReadIndex readIndex = new TestReadIndex(methodInvocations::add);
        AtomicInteger flushCallbackCallCount = new AtomicInteger();
        MemoryStateUpdater updater = new MemoryStateUpdater(opLog, readIndex, flushCallbackCallCount::incrementAndGet);
        ArrayList<Operation> operations = populate(updater, segmentCount, operationCountPerType);

        methodInvocations.clear(); // We've already tested up to here.
        updater.flush();
        Assert.assertEquals("Unexpected number of calls to the ReadIndex triggerFutureReads method.", 1, methodInvocations.size());
        Assert.assertEquals("Unexpected number of calls to the flushCallback provided in the constructor.", 1, flushCallbackCallCount.get());
        TestReadIndex.MethodInvocation mi = methodInvocations.get(0);
        Assert.assertEquals("No call to ReadIndex.triggerFutureReads() after call to flush().", TestReadIndex.TRIGGER_FUTURE_READS, mi.methodName);
        Collection<Long> triggerSegmentIds = (Collection<Long>) mi.args.get("streamSegmentIds");
        HashSet<Long> expectedSegmentIds = new HashSet<>();
        for (Operation op : operations) {
            if (op instanceof StorageOperation) {
                expectedSegmentIds.add(((StorageOperation) op).getStreamSegmentId());
            }
        }

        AssertExtensions.assertContainsSameElements("ReadIndex.triggerFutureReads() was called with the wrong set of StreamSegmentIds.", expectedSegmentIds, triggerSegmentIds);
    }

    private ArrayList<Operation> populate(MemoryStateUpdater updater, int segmentCount, int operationCountPerType) throws DataCorruptionException {
        ArrayList<Operation> operations = new ArrayList<>();
        long offset = 0;
        for (int i = 0; i < segmentCount; i++) {
            for (int j = 0; j < operationCountPerType; j++) {
                StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(
                        new StreamSegmentInformation("a", i * j, false, false, new ImmutableDate()));
                mapOp.setStreamSegmentId(i);
                operations.add(mapOp);
                StreamSegmentAppendOperation appendOp = new StreamSegmentAppendOperation(i, Integer.toString(i).getBytes(), null);
                appendOp.setStreamSegmentOffset(offset);
                offset += appendOp.getData().length;
                operations.add(appendOp);
                operations.add(new MergeTransactionOperation(i, j));
            }
        }

        for (int i = 0; i < operations.size(); i++) {
            operations.get(i).setSequenceNumber(i);
            updater.process(operations.get(i));
        }

        return operations;
    }

    private static class TestReadIndex implements ReadIndex {
        static final String APPEND = "append";
        static final String BEGIN_MERGE = "beginMerge";
        static final String COMPLETE_MERGE = "completeMerge";
        static final String READ = "read";
        static final String READ_DIRECT = "readDirect";
        static final String TRIGGER_FUTURE_READS = "triggerFutureReads";
        static final String CLEANUP = "cleanup";
        static final String ENTER_RECOVERY_MODE = "enterRecoveryMode";
        static final String EXIT_RECOVERY_MODE = "exitRecoveryMode";

        private final Consumer<MethodInvocation> methodInvokeCallback;
        private boolean closed;

        TestReadIndex(Consumer<MethodInvocation> methodInvokeCallback) {
            this.methodInvokeCallback = methodInvokeCallback;
        }

        @Override
        public void append(long segmentId, long offset, byte[] data) {
            invoke(new MethodInvocation(APPEND)
                    .withArg("streamSegmentId", segmentId)
                    .withArg("offset", offset)
                    .withArg("data", data));
        }

        @Override
        public void beginMerge(long targetStreamSegmentId, long offset, long sourceStreamSegmentId) {
            invoke(new MethodInvocation(BEGIN_MERGE)
                    .withArg("targetStreamSegmentId", targetStreamSegmentId)
                    .withArg("offset", offset)
                    .withArg("sourceStreamSegmentId", sourceStreamSegmentId));
        }

        @Override
        public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) {
            invoke(new MethodInvocation(COMPLETE_MERGE)
                    .withArg("targetStreamSegmentId", targetStreamSegmentId)
                    .withArg("sourceStreamSegmentId", sourceStreamSegmentId));
        }

        @Override
        public InputStream readDirect(long streamSegmentId, long offset, int length) {
            invoke(new MethodInvocation(READ_DIRECT)
                    .withArg("offset", offset)
                    .withArg("length", length));
            return null;
        }

        @Override
        public ReadResult read(long streamSegmentId, long offset, int maxLength, Duration timeout) {
            invoke(new MethodInvocation(READ)
                    .withArg("offset", offset)
                    .withArg("maxLength", maxLength));
            return null;
        }

        @Override
        public void triggerFutureReads(Collection<Long> streamSegmentIds) {
            invoke(new MethodInvocation(TRIGGER_FUTURE_READS)
                    .withArg("streamSegmentIds", streamSegmentIds));
        }

        @Override
        public void clear() {
            throw new IllegalStateException("Not Implemented");
        }

        @Override
        public void cleanup(Iterator<Long> segmentIds) {
            invoke(new MethodInvocation(CLEANUP));
        }

        @Override
        public void enterRecoveryMode(ContainerMetadata recoveryMetadataSource) {
            invoke(new MethodInvocation(ENTER_RECOVERY_MODE)
                    .withArg("recoveryMetadataSource", recoveryMetadataSource));
        }

        @Override
        public void exitRecoveryMode(boolean successfulRecovery) {
            invoke(new MethodInvocation(EXIT_RECOVERY_MODE)
                    .withArg("successfulRecovery", successfulRecovery));
        }

        @Override
        public void close() {
            this.closed = true;
        }

        private void invoke(MethodInvocation methodInvocation) {
            Exceptions.checkNotClosed(this.closed, this);
            if (this.methodInvokeCallback != null) {
                this.methodInvokeCallback.accept(methodInvocation);
            }
        }

        static class MethodInvocation {
            final String methodName;
            final AbstractMap<String, Object> args;

            MethodInvocation(String name) {
                this.methodName = name;
                this.args = new HashMap<>();
            }

            MethodInvocation withArg(String name, Object value) {
                this.args.put(name, value);
                return this;
            }
        }
    }
}
