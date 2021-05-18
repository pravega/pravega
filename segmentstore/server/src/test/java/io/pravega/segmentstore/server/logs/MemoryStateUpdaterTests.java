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
package io.pravega.segmentstore.server.logs;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.ServiceHaltException;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.invocation.InvocationOnMock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for MemoryStateUpdater class.
 */
public class MemoryStateUpdaterTests extends ThreadPooledTestSuite {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the functionality of the process() method.
     */
    @Test
    public void testProcess() throws Exception {
        int segmentCount = 10;
        int operationCountPerType = 5;

        // Add to MTL + Add to ReadIndex (append; beginMerge).
        InMemoryLog opLog = new InMemoryLog();
        val readIndex = mock(ReadIndex.class);

        val triggerSegmentIds = new ArrayList<Long>();
        doAnswer(x -> {
            triggerSegmentIds.clear();
            triggerSegmentIds.addAll(x.getArgument(0));
            return null;
        }).when(readIndex).triggerFutureReads(anyCollection());

        val invocations = new ArrayList<InvocationOnMock>();
        doAnswer(invocations::add).when(readIndex).append(anyLong(), anyLong(), any());
        doAnswer(invocations::add).when(readIndex).beginMerge(anyLong(), anyLong(), anyLong());

        MemoryStateUpdater updater = new MemoryStateUpdater(opLog, readIndex);
        ArrayList<Operation> operations = populate(updater, segmentCount, operationCountPerType);

        // Verify they were properly processed.
        Queue<Operation> logIterator = opLog.poll(operations.size());
        int currentIndex = -1;
        val invocationIterator = invocations.iterator();
        while (!logIterator.isEmpty()) {
            currentIndex++;
            Operation expected = operations.get(currentIndex);
            Operation actual = logIterator.poll();
            if (expected instanceof StorageOperation) {
                val invokedMethod = invocationIterator.next();
                if (expected instanceof StreamSegmentAppendOperation) {
                    Assert.assertTrue("StreamSegmentAppendOperation was not added as a CachedStreamSegmentAppendOperation to the Memory Log.", actual instanceof CachedStreamSegmentAppendOperation);
                    StreamSegmentAppendOperation appendOp = (StreamSegmentAppendOperation) expected;
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was not added to the ReadIndex.",
                            "append", invokedMethod.getMethod().getName());
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.",
                            appendOp.getStreamSegmentId(), (long) invokedMethod.getArgument(0));
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.",
                            appendOp.getStreamSegmentOffset(), (long) invokedMethod.getArgument(1));
                    Assert.assertEquals("Append with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.",
                            appendOp.getData(), invokedMethod.getArgument(2));
                } else if (expected instanceof MergeSegmentOperation) {
                    MergeSegmentOperation mergeOp = (MergeSegmentOperation) expected;
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was not added to the ReadIndex.",
                            "beginMerge", invokedMethod.getMethod().getName());
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.",
                            mergeOp.getStreamSegmentId(), (long) invokedMethod.getArgument(0));
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.",
                            mergeOp.getStreamSegmentOffset(), (long) invokedMethod.getArgument(1));
                    Assert.assertEquals("Merge with SeqNo " + expected.getSequenceNumber() + " was added to the ReadIndex with wrong arguments.",
                            mergeOp.getSourceSegmentId(), (long) invokedMethod.getArgument(2));
                }
            }
        }

        // Verify triggerFutureReads args.
        val expectedSegmentIds = operations.stream()
                .filter(op -> op instanceof SegmentOperation)
                .map(op -> ((SegmentOperation) op).getStreamSegmentId())
                .collect(Collectors.toSet());

        AssertExtensions.assertContainsSameElements("ReadIndex.triggerFutureReads() was called with the wrong set of StreamSegmentIds.",
                expectedSegmentIds, triggerSegmentIds);

        // Test DataCorruptionException.
        AssertExtensions.assertThrows(
                "MemoryStateUpdater accepted an operation that was out of order.",
                () -> updater.process(new MergeSegmentOperation(1, 2)),
                ex -> ex instanceof DataCorruptionException);
    }

    /**
     * Tests the functionality of the {@link MemoryStateUpdater#process} method with critical errors.
     */
    @Test
    public void testProcessWithErrors() {
        final int corruptAtIndex = 10;
        final int segmentCount = 10;
        final int operationCountPerType = 5;

        // Add to MTL + Add to ReadIndex (append; beginMerge).
        val opLog = new OperationLogTestBase.CorruptedMemoryOperationLog(corruptAtIndex);
        val readIndex = mock(ReadIndex.class);
        MemoryStateUpdater updater = new MemoryStateUpdater(opLog, readIndex);

        AssertExtensions.assertThrows(
                "Expected a DataCorruptionException",
                () -> populate(updater, segmentCount, operationCountPerType),
                ex -> ex instanceof DataCorruptionException);

        // Verify they were properly processed.
        verify(readIndex, never()).triggerFutureReads(anyCollection());

        Queue<Operation> logIterator = opLog.poll(corruptAtIndex * 2);
        int addCount = 0;
        while (!logIterator.isEmpty()) {
            addCount++;
            logIterator.poll();
        }

        Assert.assertEquals("Unexpected number of operations added to the log.", corruptAtIndex - 1, addCount);

        // The rest of the checks is done in the populate() method: it verifies that the callback is invoked for every
        // operation, including the failed ones.
    }

    /**
     * Tests the ability of the MemoryStateUpdater to delegate Enter/Exit recovery mode to the read index.
     */
    @Test
    public void testRecoveryMode() throws Exception {
        // Check it's properly delegated to Read index.
        InMemoryLog opLog = new InMemoryLog();
        val readIndex = mock(ReadIndex.class);
        MemoryStateUpdater updater = new MemoryStateUpdater(opLog, readIndex);

        UpdateableContainerMetadata metadata1 = new MetadataBuilder(1).build();
        updater.enterRecoveryMode(metadata1);
        updater.cleanupReadIndex();
        updater.exitRecoveryMode(true);

        verify(readIndex).enterRecoveryMode(metadata1);
        verify(readIndex).cleanup(null);
        verify(readIndex).trimCache();
        verify(readIndex).exitRecoveryMode(true);
    }

    /**
     * Tests {@link MemoryStateUpdater#registerReadListener} and {@link MemoryStateUpdater#notifyLogRead()}.
     */
    @Test
    public void testReadListeners() {
        val updater = new MemoryStateUpdater(new InMemoryLog(), mock(ReadIndex.class));
        val l1 = mock(ThrottleSourceListener.class);
        when(l1.isClosed()).thenReturn(false);
        updater.registerReadListener(l1);
        verify(l1).isClosed();

        updater.notifyLogRead();
        verify(l1).notifyThrottleSourceChanged();
    }

    private ArrayList<Operation> populate(MemoryStateUpdater updater, int segmentCount, int operationCountPerType) throws ServiceHaltException {
        ArrayList<Operation> operations = new ArrayList<>();
        long offset = 0;
        for (int i = 0; i < segmentCount; i++) {
            for (int j = 0; j < operationCountPerType; j++) {
                StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(
                         StreamSegmentInformation.builder().name("a").length( i * j).build());
                mapOp.setStreamSegmentId(i);
                operations.add(mapOp);
                StreamSegmentAppendOperation appendOp = new StreamSegmentAppendOperation(i, new ByteArraySegment(Integer.toString(i).getBytes()), null);
                appendOp.setStreamSegmentOffset(offset);
                offset += appendOp.getData().getLength();
                operations.add(appendOp);
                operations.add(new MergeSegmentOperation(i, j));
            }
        }

        for (int i = 0; i < operations.size(); i++) {
            operations.get(i).setSequenceNumber(i);
        }

        val processedOperations = new ArrayList<Operation>();
        try {
            updater.process(operations.iterator(), processedOperations::add);
        } finally {
            // Regardless whether we complete this method or not, it guarantees that the callback is invoked for every
            // operation it was passed, even if it didn't get processed yet.
            AssertExtensions.assertListEquals("Unexpected operations processed.", operations, processedOperations, Objects::equals);
        }

        return operations;
    }
}
