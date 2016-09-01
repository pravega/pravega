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

package com.emc.pravega.service.server.writer;

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.CloseableExecutorService;
import com.emc.pravega.service.server.ConfigHelpers;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.StreamSegmentNameUtils;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.logs.operations.BatchMapOperation;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentMapOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.server.mocks.InMemoryCache;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * Unit tests for the StorageWriter class.
 */
public class StorageWriterTests {
    private static final int CONTAINER_ID = 1;
    private static final int SEGMENT_COUNT = 1;
    private static final int BATCHES_PER_SEGMENT = 0;
    private static final int APPENDS_PER_SEGMENT = 1000;
    private static final int THREAD_POOL_SIZE = 200;
    private static final WriterConfig DEFAULT_CONFIG = ConfigHelpers.createWriterConfig(1000, 1000);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    public void testMisc() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        // Start the writer.
        context.writer.startAsync();
        Thread.sleep(100);

        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> batchesBySegment = createBatches(segmentIds, context);
        ArrayList<Long> batchIds = new ArrayList<>();
        batchesBySegment.values().forEach(batchIds::addAll);

        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Append data
        CompletableFuture.runAsync(() -> {
            appendData(segmentIds, segmentContents, context);
            appendData(batchIds, segmentContents, context);

            // Merge batches
            sealSegments(batchIds, context);
            mergeBatches(batchIds, context);

            // Seal the parents
            sealSegments(segmentIds, context);
        }, context.executor.get()).join();

        Thread.sleep(3000);
    }

    private void mergeBatches(Iterable<Long> batchIds, TestContext context) {
        for (long batchId : batchIds) {
            UpdateableSegmentMetadata batchMetadata = context.metadata.getStreamSegmentMetadata(batchId);
            UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(batchMetadata.getParentId());
            Assert.assertFalse("Batch already merged", batchMetadata.isMerged());
            Assert.assertTrue("Batch not sealed prior to merger", batchMetadata.isSealed());
            Assert.assertFalse("Parent is sealed already merged", parentMetadata.isSealed());

            // Create the Merge Op
            MergeBatchOperation op = new MergeBatchOperation(parentMetadata.getId(), batchMetadata.getId());
            op.setLength(batchMetadata.getLength());
            op.setStreamSegmentOffset(parentMetadata.getDurableLogLength());
            op.setSequenceNumber(context.metadata.nextOperationSequenceNumber());

            // Update metadata
            parentMetadata.setDurableLogLength(parentMetadata.getDurableLogLength() + batchMetadata.getDurableLogLength());
            batchMetadata.markMerged();

            // Process the merge op
            context.dataSource.add(op);
        }
    }

    private void sealSegments(Collection<Long> segmentIds, TestContext context) {
        for (long segmentId : segmentIds) {
            UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
            segmentMetadata.markSealed();
            StreamSegmentSealOperation sealOp = new StreamSegmentSealOperation(segmentId);
            sealOp.setStreamSegmentOffset(segmentMetadata.getDurableLogLength());
            sealOp.setSequenceNumber(context.metadata.nextOperationSequenceNumber());
            context.dataSource.add(sealOp);
        }
    }

    private void appendData(Collection<Long> segmentIds, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            AppendContext appendContext = new AppendContext(UUID.randomUUID(), i);
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(segmentMetadata.getName(), segmentId, i, writeId);
                writeId++;

                // Make sure we increase the DurableLogLength prior to appending; the Writer checks for this.
                long offset = segmentMetadata.getDurableLogLength();
                segmentMetadata.setDurableLogLength(offset + data.length);
                StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(segmentId, data, appendContext);
                op.setSequenceNumber(context.metadata.nextOperationSequenceNumber());
                op.setStreamSegmentOffset(offset);
                if (writeId % 2 == 0) {
                    CacheKey key = new CacheKey(segmentId, offset);
                    context.cache.insert(key, data);
                    context.dataSource.add(new CachedStreamSegmentAppendOperation(op, key));
                } else {
                    context.dataSource.add(op);
                }

                recordAppend(segmentId, data, segmentContents);
            }
        }
    }

    private <T> void recordAppend(T segmentIdentifier, byte[] data, HashMap<T, ByteArrayOutputStream> segmentContents) {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentIdentifier, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentIdentifier, contents);
        }
        try {
            contents.write(data);
        } catch (IOException ex) {
            Assert.fail(ex.toString());
        }
    }

    private ArrayList<Long> createSegments(TestContext context) {
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String name = getSegmentName(i);
            context.metadata.mapStreamSegmentId(name, i);
            initializeSegment(i, context);
            segmentIds.add((long) i);

            // Add the operation to the log.
            StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(context.storage.getStreamSegmentInfo(name, TIMEOUT).join());
            mapOp.setStreamSegmentId((long) i);
            mapOp.setSequenceNumber(context.metadata.nextOperationSequenceNumber());
            context.dataSource.add(mapOp);
        }

        return segmentIds;
    }

    private HashMap<Long, ArrayList<Long>> createBatches(Collection<Long> segmentIds, TestContext context) {
        // Create the batches.
        HashMap<Long, ArrayList<Long>> batches = new HashMap<>();
        long batchId = Integer.MAX_VALUE;
        for (long parentId : segmentIds) {
            ArrayList<Long> segmentBatches = new ArrayList<>();
            batches.put(parentId, segmentBatches);
            SegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);
            for (int i = 0; i < BATCHES_PER_SEGMENT; i++) {
                String batchName = StreamSegmentNameUtils.generateBatchStreamSegmentName(parentMetadata.getName());
                context.metadata.mapStreamSegmentId(batchName, batchId, parentId);
                initializeSegment(batchId, context);
                segmentBatches.add(batchId);

                // Add the operation to the log.
                BatchMapOperation mapOp = new BatchMapOperation(parentId, context.storage.getStreamSegmentInfo(batchName, TIMEOUT).join());
                mapOp.setStreamSegmentId(batchId);
                mapOp.setSequenceNumber(context.metadata.nextOperationSequenceNumber());
                context.dataSource.add(mapOp);

                batchId++;
            }
        }

        return batches;
    }

    private void initializeSegment(long segmentId, TestContext context) {
        UpdateableSegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        metadata.setDurableLogLength(0);
        metadata.setStorageLength(0);
        context.storage.create(metadata.getName(), TIMEOUT).join();
    }

    private byte[] getAppendData(String segmentName, long segmentId, int segmentAppendSeq, int writeId) {
        return String.format("SegmentName=%s,SegmentId=_%d,AppendSeq=%d,WriteId=%d", segmentName, segmentId, segmentAppendSeq, writeId).getBytes();
    }

    private String getSegmentName(int i) {
        return "Segment_" + i;
    }

    // region TestContext

    private static class TestContext implements AutoCloseable {
        final CloseableExecutorService executor;
        final UpdateableContainerMetadata metadata;
        final TestWriterDataSource dataSource;
        final Storage storage;
        final Cache cache;
        final StorageWriter writer;

        TestContext(WriterConfig config) {
            this.executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
            this.metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            this.storage = new InMemoryStorage(this.executor.get());
            this.cache = new InMemoryCache(Integer.toString(CONTAINER_ID));
            this.dataSource = new TestWriterDataSource(this.metadata, this.cache, this.executor.get(), false); // We assign sequence numbers manually.
            this.writer = new StorageWriter(config, this.metadata, this.dataSource, this.storage, this.executor.get());
        }

        @Override
        public void close() {
            this.writer.close();
            this.dataSource.close();
            this.cache.close();
            this.storage.close();
            this.executor.close();
        }
    }

    // endregion
}
