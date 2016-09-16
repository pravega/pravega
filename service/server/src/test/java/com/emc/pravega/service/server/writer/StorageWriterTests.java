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
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.CloseableExecutorService;
import com.emc.pravega.service.server.ConfigHelpers;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.PropertyBag;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.StreamSegmentNameUtils;
import com.emc.pravega.service.server.TestStorage;
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
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.ErrorInjector;
import com.emc.pravega.testcommon.IntentionalException;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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
                       .with(WriterConfig.PROPERTY_MIN_READ_TIMEOUT_MILLIS, 10)
                       .with(WriterConfig.PROPERTY_MAX_READ_TIMEOUT_MILLIS, 250));

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    /**
     * Tests a normal, happy case, when the Writer needs to process operations in the "correct" order, from a DataSource
     * that does not produce any errors (i.e., Timeouts) and to a Storage that works perfectly.
     * See testWriter() for more details about testing flow.
     */
    @Test
    public void testNormalFlow() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        testWriter(context);
    }

    /**
     * Tests the StorageWriter in a scenario where the DataSource throws random exceptions. Simulated errors are for
     * the following operations:
     * * Read (sync + async)
     * * Acknowledge (sync + async)
     * * GetAppendData (sync)
     */
    @Test
    public void testWithDataSourceTransientErrors() throws Exception {
        final int failReadSyncEvery = 2;
        final int failReadAsyncEvery = 3;
        final int failAckSyncEvery = 2;
        final int failAckAsyncEvery = 3;
        final int failGetAppendDataEvery = 99;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        Supplier<Exception> exceptionSupplier = IntentionalException::new;

        // Simulated Read errors.
        context.dataSource.setReadSyncErrorInjector(new ErrorInjector<>(count -> count % failReadSyncEvery == 0, exceptionSupplier));
        context.dataSource.setReadAsyncErrorInjector(new ErrorInjector<>(count -> count % failReadAsyncEvery == 0, exceptionSupplier));

        // Simulated ack/truncate errors.
        context.dataSource.setAckSyncErrorInjector(new ErrorInjector<>(count -> count % failAckSyncEvery == 0, exceptionSupplier));
        context.dataSource.setAckAsyncErrorInjector(new ErrorInjector<>(count -> count % failAckAsyncEvery == 0, exceptionSupplier));

        // Simulated data retrieval errors.
        context.dataSource.setGetAppendDataErrorInjector(new ErrorInjector<>(count -> count % failGetAppendDataEvery == 0, exceptionSupplier));

        testWriter(context);
    }

    /**
     * Tests the StorageWriter in a scenario where some sort of fatal error occurs from the DataSource. In this case,
     * the cache lookup for a CachedStreamSegmentAppendOperation returns null.
     */
    @Test
    public void testWithDataSourceFatalErrors() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create a bunch of segments and batches.
        ArrayList<Long> segmentIds = createSegments(context);

        // Use a few Futures to figure out when we are done filling up the DataSource and when everything is ack-ed back.
        CompletableFuture<Void> ackEverything = new CompletableFuture<>();
        CompletableFuture<Void> producingComplete = new CompletableFuture<>();
        context.dataSource.setAcknowledgeCallback(args -> {
            if (producingComplete.isDone() && args.getAckSequenceNumber() >= context.metadata.getOperationSequenceNumber()) {
                ackEverything.complete(null);
            }
        });

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendData(segmentIds, segmentContents, context);
        producingComplete.complete(null);

        // We clear up the cache after we have added the operations in the data source - this will cause the writer
        // to pick them up and end up failing when attempting to fetch the cache contents.
        context.cache.reset();

        context.writer.startAsync().awaitRunning();

        AssertExtensions.assertThrows(
                "StorageWriter did not fail when a fatal data retrieval error occurred.",
                () -> ServiceShutdownListener.awaitShutdown(context.writer, TIMEOUT, true),
                ex -> ex instanceof IllegalStateException);
        Assert.assertTrue("Unexpected failure cause for StorageWriter.", ExceptionHelpers.getRealException(context.writer.failureCause()) instanceof DataCorruptionException);
    }

    /**
     * Tests the StorageWriter in a Scenario where the Storage component throws non-corruption exceptions (i.e., not badOffset)
     */
    @Test
    public void testWithStorageTransientErrors() throws Exception {
        final int failWriteSyncEvery = 4;
        final int failWriteAsyncEvery = 6;
        final int failSealSyncEvery = 4;
        final int failSealAsyncEvery = 6;
        final int failConcatAsyncEvery = 6;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        Supplier<Exception> exceptionSupplier = IntentionalException::new;

        // Simulated Write errors.
        context.storage.setWriteSyncErrorInjector(new ErrorInjector<>(count -> count % failWriteSyncEvery == 0, exceptionSupplier));
        context.storage.setWriteAsyncErrorInjector(new ErrorInjector<>(count -> count % failWriteAsyncEvery == 0, exceptionSupplier));

        // Simulated Seal errors.
        context.storage.setSealSyncErrorInjector(new ErrorInjector<>(count -> count % failSealSyncEvery == 0, exceptionSupplier));
        context.storage.setSealAsyncErrorInjector(new ErrorInjector<>(count -> count % failSealAsyncEvery == 0, exceptionSupplier));

        // Simulated Concat errors.
        context.storage.setConcatAsyncErrorInjector(new ErrorInjector<>(count -> count % failConcatAsyncEvery == 0, exceptionSupplier));

        testWriter(context);
    }

    /**
     * Tests the StorageWriter in a Scenario where the Storage component throws data corruption exceptions (i.e., badOffset,
     * and after reconciliation, the data is still corrupt).
     */
    @Test
    public void testWithStorageCorruptionErrors() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create a bunch of segments and batches.
        ArrayList<Long> segmentIds = createSegments(context);

        // Use a few Futures to figure out when we are done filling up the DataSource and when everything is ack-ed back.
        CompletableFuture<Void> ackEverything = new CompletableFuture<>();
        CompletableFuture<Void> producingComplete = new CompletableFuture<>();
        context.dataSource.setAcknowledgeCallback(args -> {
            if (producingComplete.isDone() && args.getAckSequenceNumber() >= context.metadata.getOperationSequenceNumber()) {
                ackEverything.complete(null);
            }
        });

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendData(segmentIds, segmentContents, context);
        producingComplete.complete(null);

        byte[] corruptionData = "foo".getBytes();
        Supplier<Exception> exceptionSupplier = () -> {
            // Corrupt data.
            for (long segmentId : segmentIds) {
                String name = context.metadata.getStreamSegmentMetadata(segmentId).getName();
                long length = context.storage.getStreamSegmentInfo(name, TIMEOUT).join().getLength();
                context.storage.write(name, length, new ByteArrayInputStream(corruptionData), corruptionData.length, TIMEOUT).join();
            }

            // Return some other kind of exception.
            return new TimeoutException();
        };

        // We only try to corrupt data once.
        AtomicBoolean corruptionHappened = new AtomicBoolean();
        context.storage.setWriteAsyncErrorInjector(new ErrorInjector<>(c -> !corruptionHappened.getAndSet(true), exceptionSupplier));

        context.writer.startAsync().awaitRunning();

        AssertExtensions.assertThrows(
                "StorageWriter did not fail when a fatal data corruption error occurred.",
                () -> ServiceShutdownListener.awaitShutdown(context.writer, TIMEOUT, true),
                ex -> ex instanceof IllegalStateException);
        Assert.assertTrue("Unexpected failure cause for StorageWriter.", ExceptionHelpers.getRealException(context.writer.failureCause()) instanceof DataCorruptionException);
    }

    /**
     * Tests the StorageWriter in a Scenario where the Storage component throws data corruption exceptions (i.e., badOffset,
     * but after reconciliation, the data is not corrupt).
     */
    @Test
    public void testWithStorageRecoverableCorruptionErrors() throws Exception {
        // TODO: implement once the proper code is implemented in StorageWriter/SegmentAggregator.
    }

    /**
     * Tests the StorageWriter in a Scenario where it needs to gracefully recover from a Container failure, and not all
     * previously written data has been acknowledged to the DataSource.
     */
    @Test
    public void testRecovery() throws Exception {

    }

    /**
     * Tests the writer as it is setup in the given context.
     * General test flow:
     * 1. Add Appends (Cached/non-cached) to both Parent and Batch segments
     * 2. Seal and merge the batches
     * 3. Seal the parent segments.
     * 4. Wait for everything to be ack-ed and check the result.
     *
     * @param context The TestContext to use.
     */
    private void testWriter(TestContext context) throws Exception {
        context.writer.startAsync();

        // Create a bunch of segments and batches.
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> batchesBySegment = createBatches(segmentIds, context);
        ArrayList<Long> batchIds = new ArrayList<>();
        batchesBySegment.values().forEach(batchIds::addAll);

        // Use a few Futures to figure out when we are done filling up the DataSource and when everything is ack-ed back.
        CompletableFuture<Void> ackEverything = new CompletableFuture<>();
        CompletableFuture<Void> producingComplete = new CompletableFuture<>();
        context.dataSource.setAcknowledgeCallback(args -> {
            if (producingComplete.isDone() && args.getAckSequenceNumber() >= context.metadata.getOperationSequenceNumber()) {
                ackEverything.complete(null);
            }
        });

        // Append data.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendData(segmentIds, segmentContents, context);
        appendData(batchIds, segmentContents, context);

        // Merge batches.
        sealSegments(batchIds, context);
        mergeBatches(batchIds, segmentContents, context);

        // Seal the parents.
        sealSegments(segmentIds, context);
        metadataCheckpoint(context);
        producingComplete.complete(null);

        // Wait for the writer to complete its job.
        ackEverything.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify all batches are deleted.
        for (long batchId : batchIds) {
            SegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(batchId);
            Assert.assertTrue("Batch not marked as deleted in metadata: " + batchId, metadata.isDeleted());
            AssertExtensions.assertThrows(
                    "Batch segment was not deleted from storage after being merged: " + batchId,
                    () -> context.storage.getStreamSegmentInfo(metadata.getName(), TIMEOUT).join(),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }

        // Verify segment contents.
        for (long segmentId : segmentContents.keySet()) {
            SegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
            Assert.assertNotNull("Setup error: No metadata for segment " + segmentId, metadata);
            Assert.assertEquals("Setup error: Not expecting a batch segment in the final list: " + segmentId, ContainerMetadata.NO_STREAM_SEGMENT_ID, metadata.getParentId());

            Assert.assertEquals("Metadata does not indicate that all bytes were copied to Storage for segment " + segmentId, metadata.getDurableLogLength(), metadata.getStorageLength());
            Assert.assertEquals("Metadata.Sealed disagrees with Metadata.SealedInStorage for segment " + segmentId, metadata.isSealed(), metadata.isSealedInStorage());

            SegmentProperties sp = context.storage.getStreamSegmentInfo(metadata.getName(), TIMEOUT).join();
            Assert.assertEquals("Metadata.StorageLength disagrees with Storage.Length for segment " + segmentId, metadata.getStorageLength(), sp.getLength());
            Assert.assertEquals("Metadata.Sealed/SealedInStorage disagrees with Storage.Sealed for segment " + segmentId, metadata.isSealedInStorage(), sp.isSealed());

            byte[] expected = segmentContents.get(segmentId).toByteArray();
            byte[] actual = new byte[expected.length];
            int actualLength = context.storage.read(metadata.getName(), 0, actual, 0, actual.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read from Storage for segment " + segmentId, metadata.getStorageLength(), actualLength);
            Assert.assertArrayEquals("Unexpected data written to storage for segment " + segmentId, expected, actual);
        }
    }

    //region Helpers

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

    //endregion

    // region TestContext

    private static class TestContext implements AutoCloseable {
        final CloseableExecutorService executor;
        final UpdateableContainerMetadata metadata;
        final TestWriterDataSource dataSource;
        final TestStorage storage;
        final Cache cache;
        final StorageWriter writer;

        TestContext(WriterConfig config) {
            this.executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
            this.metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            this.storage = new TestStorage(new InMemoryStorage(this.executor.get()));
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
