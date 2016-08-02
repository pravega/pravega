/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.nautilus.testcommon.AssertExtensions;
import lombok.Cleanup;
import org.junit.Assert;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Base class for all Operation Log-based classes (i.e., DurableLog and OperationProcessor).
 */
public abstract class OperationLogTestBase {
    protected static final Duration TIMEOUT = Duration.ofSeconds(30);

    protected void performMetadataChecks(Collection<Long> streamSegmentIds, Collection<Long> invalidStreamSegmentIds, AbstractMap<Long, Long> batches, Collection<LogTestHelpers.OperationWithCompletion> operations, ContainerMetadata metadata, boolean expectBatchesMerged, boolean expectSegmentsSealed) {
        // Verify that batches are merged
        for (long batchId : batches.keySet()) {
            SegmentMetadata batchMetadata = metadata.getStreamSegmentMetadata(batchId);
            if (invalidStreamSegmentIds.contains(batchId)) {
                Assert.assertTrue("Unexpected data for a Batch that was invalid.", batchMetadata == null || batchMetadata.getDurableLogLength() == 0);
            } else {
                Assert.assertEquals("Unexpected batch seal state for batch " + batchId, expectBatchesMerged, batchMetadata.isSealed());
                Assert.assertEquals("Unexpected batch merge state for batch " + batchId, expectBatchesMerged, batchMetadata.isMerged());
            }
        }

        // Verify the end state of each stream segment (length, sealed).
        AbstractMap<Long, Integer> expectedLengths = LogTestHelpers.getExpectedLengths(operations);
        for (long streamSegmentId : streamSegmentIds) {
            SegmentMetadata segmentMetadata = metadata.getStreamSegmentMetadata(streamSegmentId);
            if (invalidStreamSegmentIds.contains(streamSegmentId)) {
                Assert.assertTrue("Unexpected data for a StreamSegment that was invalid.", segmentMetadata == null || segmentMetadata.getDurableLogLength() == 0);
            } else {
                Assert.assertEquals("Unexpected seal state for StreamSegment " + streamSegmentId, expectSegmentsSealed, segmentMetadata.isSealed());
                Assert.assertEquals("Unexpected length for StreamSegment " + streamSegmentId, (int) expectedLengths.getOrDefault(streamSegmentId, 0), segmentMetadata.getDurableLogLength());
            }
        }
    }

    protected void performReadIndexChecks(Collection<LogTestHelpers.OperationWithCompletion> operations, ReadIndex readIndex) throws Exception {
        AbstractMap<Long, Integer> expectedLengths = LogTestHelpers.getExpectedLengths(operations);
        AbstractMap<Long, InputStream> expectedData = LogTestHelpers.getExpectedContents(operations);
        for (Map.Entry<Long, InputStream> e : expectedData.entrySet()) {
            int expectedLength = expectedLengths.getOrDefault(e.getKey(), -1);
            @Cleanup
            ReadResult readResult = readIndex.read(e.getKey(), 0, expectedLength, TIMEOUT);
            int readLength = 0;
            while (readResult.hasNext()) {
                ReadResultEntryContents entry = readResult.next().getContent().join();
                int length = entry.getLength();
                readLength += length;
                int streamSegmentOffset = expectedLengths.getOrDefault(e.getKey(), 0);
                expectedLengths.put(e.getKey(), streamSegmentOffset + length);
                AssertExtensions.assertStreamEquals(String.format("Unexpected data returned from ReadIndex. StreamSegmentId = %d, Offset = %d.", e.getKey(), streamSegmentOffset), e.getValue(), entry.getData(), length);
            }

            Assert.assertEquals("Not enough bytes were read from the ReadIndex for StreamSegment " + e.getKey(), expectedLength, readLength);
        }
    }

    protected static class FailedStreamSegmentAppendOperation extends StreamSegmentAppendOperation {
        private final boolean failAtBeginning;

        public FailedStreamSegmentAppendOperation(StreamSegmentAppendOperation base, boolean failAtBeginning) {
            super(base.getStreamSegmentId(), base.getData(), base.getAppendContext());
            this.failAtBeginning = failAtBeginning;
        }

        @Override
        protected void serializeContent(DataOutputStream target) throws IOException {
            if (!this.failAtBeginning) {
                super.serializeContent(target);
            }

            throw new IOException("intentional failure");
        }
    }

    protected static class CorruptedMemoryOperationLog extends MemoryOperationLog {
        private final long corruptAtIndex;
        private final AtomicLong addCount;

        public CorruptedMemoryOperationLog(int corruptAtIndex) {
            this.corruptAtIndex = corruptAtIndex;
            this.addCount = new AtomicLong();
        }

        @Override
        public boolean addIf(Operation item, Predicate<Operation> lastItemChecker) {
            if (this.addCount.incrementAndGet() == this.corruptAtIndex) {
                // Still add the item, but report that we haven't added it.
                return false;
            }

            return super.addIf(item, lastItemChecker);
        }
    }
}
