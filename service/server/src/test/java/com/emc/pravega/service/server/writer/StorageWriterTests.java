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
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.PropertyBag;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.StreamSegmentNameUtils;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.logs.operations.BatchMapOperation;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentMapOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.server.mocks.InMemoryCache;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import lombok.Cleanup;
import lombok.val;
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
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for the StorageWriter class.
 */
public class StorageWriterTests {
    private static final int CONTAINER_ID = 1;
    private static final int SEGMENT_COUNT = 10;
    private static final int BATCHES_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 1000;
    private static final int METADATA_CHECKPOINT_FREQUENCY = 50;
    private static final int THREAD_POOL_SIZE = 100;
    private static final WriterConfig DEFAULT_CONFIG = ConfigHelpers.createWriterConfig(
            PropertyBag.create()
                       .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_BYTES, 1000)
                       .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_MILLIS, 1000)
                       .with(WriterConfig.PROPERTY_MIN_READ_TIMEOUT_MILLIS, 10));

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

        CompletableFuture<Void> ackEverything = new CompletableFuture<>();
        CompletableFuture<Void> producingComplete = new CompletableFuture<>();
        context.dataSource.setAcknowledgeCallback(args -> {
            if (producingComplete.isDone() && args.getAckSequenceNumber() >= context.metadata.getOperationSequenceNumber()) {
                ackEverything.complete(null);
            }
        });

        // Append data
        CompletableFuture.runAsync(() -> {
            appendData(segmentIds, segmentContents, context);
            appendData(batchIds, segmentContents, context);

            // Merge batches
            sealSegments(batchIds, context);
            mergeBatches(batchIds, segmentContents, context);

            // Seal the parents
            sealSegments(segmentIds, context);
            metadataCheckpoint(context);
            producingComplete.complete(null);
        }, context.executor.get());

        // Wait for producing and the writer to complete their jobs.
        CompletableFuture.allOf(producingComplete, ackEverything).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        for (long segmentId : segmentContents.keySet()) {
            SegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
            Assert.assertNotNull("No metadata for segment " + segmentId, metadata);
            Assert.assertEquals("Not expecting a batch segment " + segmentId, ContainerMetadata.NO_STREAM_SEGMENT_ID, metadata.getParentId());
            Assert.assertEquals("Not all bytes were copied to Storage for segment " + segmentId, metadata.getDurableLogLength(), metadata.getStorageLength());

            byte[] expected = segmentContents.get(segmentId).toByteArray();
            byte[] actual = new byte[expected.length];
            int actualLength = context.storage.read(metadata.getName(), 0, actual, 0, actual.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read from Storage for segment " + segmentId, metadata.getStorageLength(), actualLength);
            Assert.assertArrayEquals("Unexpected data written to storage for segment " + segmentId, expected, actual);
        }
    }

    private void mergeBatches(Iterable<Long> batchIds, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) {
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

            // Update metadata
            parentMetadata.setDurableLogLength(parentMetadata.getDurableLogLength() + batchMetadata.getDurableLogLength());
            batchMetadata.markMerged();

            // Process the merge op
            context.dataSource.add(op);

            try {
                segmentContents.get(parentMetadata.getId()).write(segmentContents.get(batchMetadata.getId()).toByteArray());
            } catch (IOException ex) {
                throw new AssertionError(ex);
            }

            segmentContents.remove(batchId);
        }
    }

    private void metadataCheckpoint(TestContext context) {
        context.dataSource.add(new MetadataCheckpointOperation());
    }

    private void sealSegments(Collection<Long> segmentIds, TestContext context) {
        for (long segmentId : segmentIds) {
            UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
            segmentMetadata.markSealed();
            StreamSegmentSealOperation sealOp = new StreamSegmentSealOperation(segmentId);
            sealOp.setStreamSegmentOffset(segmentMetadata.getDurableLogLength());
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
        return String.format("SegmentName=%s,SegmentId=_%d,AppendSeq=%d,WriteId=%d\n", segmentName, segmentId, segmentAppendSeq, writeId).getBytes();
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

            val dataSourceConfig = new TestWriterDataSource.DataSourceConfig();
            dataSourceConfig.autoInsertCheckpointFrequency = METADATA_CHECKPOINT_FREQUENCY;
            this.dataSource = new TestWriterDataSource(this.metadata, this.cache, this.executor.get(), dataSourceConfig);
            this.writer = new StorageWriter(config, this.dataSource, this.storage, this.executor.get());
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
