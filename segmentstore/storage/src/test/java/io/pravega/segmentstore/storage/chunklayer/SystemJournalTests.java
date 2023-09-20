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

package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemorySnapshotInfoStore;
import io.pravega.segmentstore.storage.mocks.InMemoryTaskQueueManager;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests for testing bootstrap functionality with {@link SystemJournal}.
 */
public class SystemJournalTests extends ThreadPooledTestSuite {

    private static final int THREAD_POOL_SIZE = 10;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(60);

    @Override
    @Before
    public void before() throws Exception {
        super.before();
        InMemorySnapshotInfoStore.clear();
    }

    @Override
    @After
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
    }

    protected ChunkMetadataStore getMetadataStore() throws Exception {
        val metadataStore = new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());
        metadataStore.setReadCallback( transactionData -> {
            Assert.assertFalse("Attempt to read pinned metadata from store", transactionData.getKey().contains("_system/containers"));
            return CompletableFuture.completedFuture(null);
        });
        return metadataStore;
    }

    protected ChunkStorage getChunkStorage() throws Exception {
        return new InMemoryChunkStorage(executorService());
    }

    private ChunkedSegmentStorageConfig.ChunkedSegmentStorageConfigBuilder getDefaultConfigBuilder(SegmentRollingPolicy policy) {
        return ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .selfCheckEnabled(true)
                .selfCheckForSnapshotEnabled(true)
                .garbageCollectionDelay(Duration.ZERO)
                .storageMetadataRollingPolicy(policy);
    }

    protected String[] getSystemSegments(String systemSegmentName) {
        return new String[]{systemSegmentName};
    }

    @Test
    public void testInitialization() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = 42;
        @Cleanup
        val garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector.initialize(new InMemoryTaskQueueManager()).join();
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy).build();

        // Init
        SystemJournal journal = new SystemJournal(containerId, chunkStorage, metadataStore, garbageCollector, config, executorService());

        //Assert.assertEquals(epoch, journal.getEpoch());
        Assert.assertEquals(containerId, journal.getContainerId());
        Assert.assertEquals(policy.getMaxLength(), journal.getConfig().getStorageMetadataRollingPolicy().getMaxLength());
        //Assert.assertEquals(epoch, journal.getEpoch());
        Assert.assertEquals(0, journal.getCurrentFileIndex().get());

        Assert.assertEquals(NameUtils.INTERNAL_CONTAINER_PREFIX, journal.getSystemSegmentsPrefix());
        Assert.assertArrayEquals(SystemJournal.getChunkStorageSystemSegments(containerId), journal.getSystemSegments());
        journal.initialize();
    }

    @Test
    public void testInitializationInvalidArgs() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = 42;
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy).build();

        @Cleanup
        val garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        AssertExtensions.assertThrows("Should not allow null chunkStorage",
                () -> new SystemJournal(containerId, null, metadataStore, garbageCollector, config, executorService()),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null metadataStore",
                () -> new SystemJournal(containerId, chunkStorage, null, garbageCollector, config, executorService()),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null policy",
                () -> new SystemJournal(containerId, chunkStorage, metadataStore, null, config, executorService()),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null config",
                () -> new SystemJournal(containerId, chunkStorage, metadataStore, garbageCollector, null, executorService()),
                ex -> ex instanceof NullPointerException);
    }

    @Test
    public void testCommitInvalidArgs() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = 42;
        int maxLength = 8;
        long epoch = 1;
        @Cleanup
        val garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector.initialize(new InMemoryTaskQueueManager()).join();
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy).build();
        val journal = new SystemJournal(containerId, chunkStorage, metadataStore, garbageCollector, config, executorService());

        AssertExtensions.assertThrows("commitRecords() should throw",
                () -> journal.commitRecord(null),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("commitRecords() should throw",
                () -> journal.commitRecords(null),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("commitRecords() should throw",
                () -> journal.commitRecords(new ArrayList<>()),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testIsSystemSegment() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = 42;
        @Cleanup
        val garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector.initialize(new InMemoryTaskQueueManager()).join();
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy).build();
        val journal = new SystemJournal(containerId, chunkStorage, metadataStore, garbageCollector, config, executorService());
        Assert.assertFalse(journal.isStorageSystemSegment("foo"));
        Assert.assertFalse(journal.isStorageSystemSegment("_system/foo"));

        Assert.assertTrue(journal.isStorageSystemSegment(NameUtils.getStorageMetadataSegmentName(containerId)));
        Assert.assertTrue(journal.isStorageSystemSegment(NameUtils.getAttributeSegmentName(NameUtils.getStorageMetadataSegmentName(containerId))));
        Assert.assertTrue(journal.isStorageSystemSegment(NameUtils.getMetadataSegmentName(containerId)));
        Assert.assertTrue(journal.isStorageSystemSegment(NameUtils.getAttributeSegmentName(NameUtils.getMetadataSegmentName(containerId))));

    }

    @Test
    public void testSystemSegmentNoConcatAllowed() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = 42;
        int maxLength = 8;
        long epoch = 1;

        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));

        @Cleanup
        val garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector.initialize(new InMemoryTaskQueueManager()).join();
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy).build();
        val journal = new SystemJournal(containerId, chunkStorage, metadataStore, garbageCollector, config, executorService());
        val systemSegmentName = NameUtils.getAttributeSegmentName(NameUtils.getMetadataSegmentName(containerId));
        Assert.assertTrue(journal.isStorageSystemSegment(systemSegmentName));
        // Init
        long offset = 0;

        // Start container with epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStore, executorService(), config);

        segmentStorage.initialize(epoch);
        segmentStorage.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        segmentStorage.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage);
        segmentStorage.create("test", null).get();

        AssertExtensions.assertFutureThrows("concat() should throw",
                segmentStorage.concat(SegmentStorageHandle.writeHandle(systemSegmentName), 0, "test", null),
                ex -> ex instanceof IllegalStateException);
        AssertExtensions.assertFutureThrows("concat() should throw",
                segmentStorage.concat(SegmentStorageHandle.writeHandle("test"), 0, systemSegmentName, null),
                ex -> ex instanceof IllegalStateException);
    }

    private void deleteGarbage(ChunkedSegmentStorage segmentStorage) {
        val testTaskQueue = (InMemoryTaskQueueManager) segmentStorage.getGarbageCollector().getTaskQueue();
        val list = testTaskQueue.drain(segmentStorage.getGarbageCollector().getTaskQueueName(), 1000);
        segmentStorage.getGarbageCollector().processBatch(list).join();
    }

    /**
     * Tests a scenario when there is only one fail over.
     * The test adds a few chunks to the system segments and then fails over.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithOneFailover() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();

        int containerId = 42;
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy).build();

        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));

        // Init
        long offset = 0;

        // Start container with epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage1 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreBeforeCrash, executorService(), config);

        segmentStorage1.initialize(epoch);
        segmentStorage1.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage1.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage1);
        checkSystemSegmentsLayout(segmentStorage1);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        val h = segmentStorage1.openWrite(systemSegmentName).join();
        val b1 = "Hello".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;
        val b2 = " World".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        // Step 2
        // Start container with epoch 2
        epoch++;

        @Cleanup
        ChunkedSegmentStorage segmentStorage2 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
        segmentStorage2.initialize(epoch);
        segmentStorage2.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage2.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage2);
        checkSystemSegmentsLayout(segmentStorage2);

        // Validate
        val info = segmentStorage2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = segmentStorage2.openRead(systemSegmentName).join();
        segmentStorage2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    /**
     * Tests a scenario when there are two fail overs.
     * The test adds a few chunks to the system segments and then fails over.
     * After fail over the zombie instance continues to write junk data to both system segment and journal file.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithTwoFailovers() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        int containerId = 42;
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(8);
        val config = getDefaultConfigBuilder(policy).build();
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        long offset = 0;

        // Epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage1 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreBeforeCrash, executorService(), config);

        segmentStorage1.initialize(epoch);
        segmentStorage1.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage1.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage1);
        checkSystemSegmentsLayout(segmentStorage1);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        val h = segmentStorage1.openWrite(systemSegmentName).join();
        val b1 = "Hello".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;

        checkSystemSegmentsLayout(segmentStorage1);

        // Epoch 2
        epoch++;

        @Cleanup
        ChunkedSegmentStorage segmentStorage2 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
        segmentStorage2.initialize(epoch);
        segmentStorage2.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage2.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage2);
        checkSystemSegmentsLayout(segmentStorage2);

        val h2 = segmentStorage2.openWrite(systemSegmentName).join();

        // Write Junk Data to from first instance.
        segmentStorage1.write(h, offset, new ByteArrayInputStream("junk".getBytes()), 4, null).join();

        val b2 = " World".getBytes();
        segmentStorage2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        checkSystemSegmentsLayout(segmentStorage2);
        val info = segmentStorage2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = segmentStorage2.openRead(systemSegmentName).join();
        segmentStorage2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    /**
     * Test migration from non-atomic to atomic
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithOneMigration() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();

        int containerId = 42;
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config1 = getDefaultConfigBuilder(policy).build();

        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));

        // Init
        long offset = 0;

        // Start container with epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage1 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreBeforeCrash, executorService(), config1);

        segmentStorage1.initialize(epoch);
        segmentStorage1.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage1.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage1);
        checkSystemSegmentsLayout(segmentStorage1);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        val h = segmentStorage1.openWrite(systemSegmentName).join();
        val b1 = "Hello".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;
        val b2 = " World".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        // Step 2
        // Start container with epoch 2
        epoch++;

        val config2 = getDefaultConfigBuilder(policy).build();
        @Cleanup
        ChunkedSegmentStorage segmentStorage2 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config2);
        segmentStorage2.initialize(epoch);
        segmentStorage2.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage2.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage2);
        checkSystemSegmentsLayout(segmentStorage2);

        // Validate
        val info = segmentStorage2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = segmentStorage2.openRead(systemSegmentName).join();
        segmentStorage2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }


    /**
     * Tests a scenario when journal chunks may be partially written during failure.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithPartialDataWrite() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        if (!chunkStorage.supportsAppend()) {
            return;
        }
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        int containerId = 42;
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(4);
        val config = getDefaultConfigBuilder(policy).build();
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        long offset = 0;

        // Epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage1 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStore, executorService(), config);

        segmentStorage1.initialize(epoch);
        segmentStorage1.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage1.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage1);
        checkSystemSegmentsLayout(segmentStorage1);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        val h = segmentStorage1.openWrite(systemSegmentName).join();
        val b1 = "Hello".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;

        // Inject a fault by adding some garbage at the end
        checkSystemSegmentsLayout(segmentStorage1);
        val chunkFileName = NameUtils.getSystemJournalFileName(segmentStorage1.getSystemJournal().getContainerId(),
                segmentStorage1.getSystemJournal().getEpoch(),
                segmentStorage1.getSystemJournal().getCurrentFileIndex().get());
        val chunkInfo = chunkStorage.getInfo(chunkFileName);
        chunkStorage.write(ChunkHandle.writeHandle(chunkFileName), chunkInfo.get().getLength(), 1, new ByteArrayInputStream(new byte[1])).get();

        // This next write will encounter partially written chunk and is expected to start a new chunk.
        val b2 = " World".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        checkSystemSegmentsLayout(segmentStorage1);

        // Step 2
        // Start container with epoch 2
        epoch++;
        @Cleanup
        ChunkedSegmentStorage segmentStorage2 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
        segmentStorage2.initialize(epoch);
        segmentStorage2.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage2.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage2);
        checkSystemSegmentsLayout(segmentStorage2);

        // Validate
        val info = segmentStorage2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = segmentStorage2.openRead(systemSegmentName).join();
        segmentStorage2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    /**
     * Tests a scenario when there are multiple fail overs overs.
     * The test adds a few chunks to the system segments and then fails over.
     * After fail over the zombie instances continue to write junk data to both system segment and journal file.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithMultipleFailovers() throws Exception {
        val containerId = 42;
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        val policy = new SegmentRollingPolicy(100);
        val config = getDefaultConfigBuilder(policy)
                .selfCheckEnabled(true)
                .build();

        testSimpleBootstrapWithMultipleFailovers(containerId, chunkStorage, config, null);
    }

    private void testSimpleBootstrapWithMultipleFailovers(int containerId, ChunkStorage chunkStorage, ChunkedSegmentStorageConfig config, Consumer<Long> faultInjection) throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 0;
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        long offset = 0;
        ChunkedSegmentStorage oldChunkedSegmentStorage = null;
        SegmentHandle oldHandle = null;
        for (int i = 1; i < 10; i++) {
            // Epoch 2
            epoch++;
            ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
            cleanupHelper.add(metadataStoreAfterCrash);

            ChunkedSegmentStorage segmentStorageInLoop = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
            cleanupHelper.add(segmentStorageInLoop);

            segmentStorageInLoop.initialize(epoch);
            segmentStorageInLoop.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

            segmentStorageInLoop.bootstrap(snapshotInfoStore).join();
            deleteGarbage(segmentStorageInLoop);
            checkSystemSegmentsLayout(segmentStorageInLoop);

            val h = segmentStorageInLoop.openWrite(systemSegmentName).join();

            if (null != oldChunkedSegmentStorage) {
                oldChunkedSegmentStorage.write(oldHandle, offset, new ByteArrayInputStream("junk".getBytes()), 4, null).join();
            }

            val b1 = "Test".getBytes();
            segmentStorageInLoop.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
            offset += b1.length;
            val b2 = Integer.toString(i).getBytes();
            segmentStorageInLoop.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
            offset += b2.length;

            oldChunkedSegmentStorage = segmentStorageInLoop;
            oldHandle = h;
        }

        if (null != faultInjection) {
            faultInjection.accept(epoch);
        }

        epoch++;
        @Cleanup
        ChunkMetadataStore metadataStoreFinal = getMetadataStore();
        @Cleanup
        ChunkedSegmentStorage segmentStorageFinal = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreFinal, executorService(), config);
        segmentStorageFinal.initialize(epoch);
        segmentStorageFinal.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        segmentStorageFinal.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorageFinal);
        checkSystemSegmentsLayout(segmentStorageFinal);

        val info = segmentStorageFinal.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(offset, info.getLength());
        byte[] out = new byte[Math.toIntExact(offset)];
        val hr = segmentStorageFinal.openRead(systemSegmentName).join();
        segmentStorageFinal.read(hr, 0, out, 0, Math.toIntExact(offset), null).join();
        val expected = "Test1Test2Test3Test4Test5Test6Test7Test8Test9";
        val actual = new String(out);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSimpleBootstrapWithMissingSnapshot() throws Exception {
        val containerId = 42;
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        val policy = new SegmentRollingPolicy(100);
        val config = getDefaultConfigBuilder(policy)
                .selfCheckEnabled(true)
                .build();

        try {
            testSimpleBootstrapWithMultipleFailovers(containerId, chunkStorage, config, epoch -> {
                val snapShotFile = NameUtils.getSystemJournalSnapshotFileName(containerId, epoch, 1);
                chunkStorage.delete(ChunkHandle.writeHandle(snapShotFile)).join();
            });
        } catch (Exception e) {
            val ex = Exceptions.unwrap(e);
            Assert.assertTrue(Exceptions.unwrap(e) instanceof IllegalStateException
                    && ex.getMessage().contains("Chunk pointed by SnapshotInfo could not be read"));
        }
    }

    @Test
    public void testSimpleBootstrapWithOneFailoverForAutoRecovery() throws Exception {
        int containerId = 42;
        val data = new InMemorySnapshotInfoStore();
        int maxLength = 8;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy)
                .maxJournalUpdatesPerSnapshot(1) // Force snapshot
                .selfCheckForSnapshotEnabled(true)
                .build();
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();

        testSimpleBootstrapWithOneFailoverWithFaultInjection(containerId, config, chunkStorage, data, epoch -> {
            InMemorySnapshotInfoStore.clear();
        });
    }

    @Test
    public void testMissingSnapshotInfoFileWithNoAutoRecovery() throws Exception {
        int containerId = 42;
        val data = new InMemorySnapshotInfoStore();
        int maxLength = 8;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy)
                .maxJournalUpdatesPerSnapshot(1) // Force snapshot
                .selfCheckForSnapshotEnabled(true)
                .build();
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();

        testSimpleBootstrapWithOneFailoverWithFaultInjection(containerId, config, chunkStorage, data, epoch -> {
            chunkStorage.delete(ChunkHandle.writeHandle(NameUtils.getSystemJournalSnapshotInfoFileName(containerId)));
        });
    }

    /**
     * Test when there are errors while writing snapshot info file. number of failures is lesser than max attempts.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testAutoRecoveryWithFlakyWrites() throws Exception {
        testSimpleBootstrapWithOneFailoverForAutoRecoveryWithFlakyWrites(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxJournalWriteAttempts() / 2 );
    }

    /**
     * Test when there are errors while writing snapshot info file. number of failures is greater than max attempts.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testAutoRecoveryWithFlakyWritesRetryExhausted() throws Exception {
        testSimpleBootstrapWithOneFailoverForAutoRecoveryWithFlakyWrites(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.getMaxJournalWriteAttempts() + 2);
    }

    public void testSimpleBootstrapWithOneFailoverForAutoRecoveryWithFlakyWrites(int failureCount) throws Exception {
        int containerId = 42;
        val data = new InMemorySnapshotInfoStore();
        int maxLength = 8;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy)
                .maxJournalUpdatesPerSnapshot(1) // Force snapshot
                .selfCheckForSnapshotEnabled(true)
                .build();

        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        AtomicInteger callCount = new AtomicInteger();
        FlakyChunkStorage flakyChunkStorage = new FlakyChunkStorage((BaseChunkStorage) chunkStorage, executorService());
        flakyChunkStorage.getInterceptor().getFlakyPredicates().add(FlakinessPredicate.builder()
                .method("doWrite.before")
                .matchPredicate(n -> callCount.incrementAndGet() < failureCount)
                .matchRegEx(NameUtils.getSystemJournalSnapshotInfoFileName(containerId))
                .action(() -> {
                    throw new IOException("Intentional");
                })
                .build());
        testSimpleBootstrapWithOneFailoverWithFaultInjection(containerId, config, flakyChunkStorage, data, epoch -> {
            InMemorySnapshotInfoStore.clear();
        });
    }

    public void testSimpleBootstrapWithOneFailoverWithFaultInjection(int containerId,
                                                                     ChunkedSegmentStorageConfig config,
                                                                     ChunkStorage chunkStorage,
                                                                     InMemorySnapshotInfoStore data,
                                                                     Consumer<Long> faultInjection) throws Exception {
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();

        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));

        // Init
        long offset = 0;

        // Start container with epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage1 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreBeforeCrash, executorService(), config);

        segmentStorage1.initialize(epoch);
        segmentStorage1.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage1.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage1);
        checkSystemSegmentsLayout(segmentStorage1);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        val h = segmentStorage1.openWrite(systemSegmentName).join();
        val b1 = "Hello".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;
        val b2 = " World".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        // Step 2
        // Start container with epoch 2
        epoch++;

        if (null != faultInjection) {
            faultInjection.accept(epoch);
        }

        @Cleanup
        ChunkedSegmentStorage segmentStorage2 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
        segmentStorage2.initialize(epoch);
        segmentStorage2.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage2.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage2);
        checkSystemSegmentsLayout(segmentStorage2);

        // Validate
        val info = segmentStorage2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = segmentStorage2.openRead(systemSegmentName).join();
        segmentStorage2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    /**
     * Tests a scenario when there are multiple fail overs overs.
     * The test adds a few chunks to the system segments and then fails over.
     * After fail over the zombie instances continue to write junk data to both system segment and journal file.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithMultipleFailoversWithTruncate() throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();

        int containerId = 42;
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 0;
        val policy = new SegmentRollingPolicy(1024);
        val config = getDefaultConfigBuilder(policy).build();
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        long offset = 0;
        long offsetToTruncateAt = 0;
        ChunkedSegmentStorage oldChunkedSegmentStorage = null;
        SegmentHandle oldHandle = null;
        long oldOffset = 0;

        for (int i = 1; i < 10; i++) {
            // Epoch 2
            epoch++;
            ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
            cleanupHelper.add(metadataStoreAfterCrash);

            ChunkedSegmentStorage segmentStorageInLoop = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
            cleanupHelper.add(segmentStorageInLoop);

            segmentStorageInLoop.initialize(epoch);
            segmentStorageInLoop.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

            segmentStorageInLoop.bootstrap(snapshotInfoStore).join();
            deleteGarbage(segmentStorageInLoop);
            checkSystemSegmentsLayout(segmentStorageInLoop);

            val h = segmentStorageInLoop.openWrite(systemSegmentName).join();

            if (null != oldChunkedSegmentStorage) {
                // Add some junk to previous instance after failover
                oldChunkedSegmentStorage.write(oldHandle, oldOffset, new ByteArrayInputStream("junk".getBytes()), 4, null).join();
            } else {
                // Only first time.
                for (int j = 1; j < 10; j++) {
                    val b0 = "JUNK".getBytes();
                    segmentStorageInLoop.write(h, offset, new ByteArrayInputStream(b0), b0.length, null).join();
                    offset += b0.length;
                }
            }
            offsetToTruncateAt += 4;
            val b1 = "Test".getBytes();
            segmentStorageInLoop.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
            offset += b1.length;
            val b2 = Integer.toString(i).getBytes();
            segmentStorageInLoop.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
            offset += b2.length;

            segmentStorageInLoop.truncate(h, offsetToTruncateAt, null).join();
            TestUtils.checkSegmentBounds(metadataStoreAfterCrash, h.getSegmentName(), offsetToTruncateAt, offset);

            val length = Math.toIntExact(offset - offsetToTruncateAt);
            byte[] readBytes = new byte[length];
            val bytesRead = segmentStorageInLoop.read(h, offsetToTruncateAt, readBytes, 0, length, null).get();
            Assert.assertEquals(length, readBytes.length);

            String s = new String(readBytes);

            //Add some garbage
            if (null != oldChunkedSegmentStorage) {
                oldChunkedSegmentStorage.write(oldHandle, oldOffset + 4, new ByteArrayInputStream("junk".getBytes()), 4, null).join();
            }

            // Save these instances so that you can write some junk after bootstrap.
            oldChunkedSegmentStorage = segmentStorageInLoop;
            oldHandle = h;
            oldOffset = offset;
        }

        epoch++;
        @Cleanup
        ChunkMetadataStore metadataStoreFinal = getMetadataStore();

        ChunkedSegmentStorage segmentStorageFinal = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreFinal, executorService(), config);
        cleanupHelper.add(segmentStorageFinal);
        segmentStorageFinal.initialize(epoch);
        segmentStorageFinal.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        segmentStorageFinal.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorageFinal);
        checkSystemSegmentsLayout(segmentStorageFinal);

        val info = segmentStorageFinal.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(offset, info.getLength());
        Assert.assertEquals(offsetToTruncateAt, info.getStartOffset());
        byte[] out = new byte[Math.toIntExact(offset - offsetToTruncateAt)];
        val hr = segmentStorageFinal.openRead(systemSegmentName).join();
        segmentStorageFinal.read(hr, offsetToTruncateAt, out, 0, Math.toIntExact(offset - offsetToTruncateAt), null).join();
        val expected = "Test1Test2Test3Test4Test5Test6Test7Test8Test9";
        val actual = new String(out);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSimpleBootstrapWithTruncateInsideSecondChunk() throws Exception {
        String initialGarbage = "JUNKJUNKJUNK";
        String garbageAfterFailure = "junk";
        String validWriteBeforeFailure = "Hello";
        String validWriteAfterFailure = " World";
        int maxLength = 8;

        testBootstrapWithTruncate(initialGarbage, garbageAfterFailure, validWriteBeforeFailure, validWriteAfterFailure, maxLength);
    }

    @Test
    public void testSimpleBootstrapWithTruncateInsideFirstChunk() throws Exception {
        String initialGarbage = "JUNK";
        String garbageAfterFailure = "junk";
        String validWriteBeforeFailure = "Hello";
        String validWriteAfterFailure = " World";
        int maxLength = 8;

        testBootstrapWithTruncate(initialGarbage, garbageAfterFailure, validWriteBeforeFailure, validWriteAfterFailure, maxLength);
    }

    @Test
    public void testSimpleBootstrapWithTruncateOnChunkBoundary() throws Exception {
        String initialGarbage = "JUNKJUNK";
        String garbageAfterFailure = "junk";
        String validWriteBeforeFailure = "Hello";
        String validWriteAfterFailure = " World";
        int maxLength = 8;

        testBootstrapWithTruncate(initialGarbage, garbageAfterFailure, validWriteBeforeFailure, validWriteAfterFailure, maxLength);
    }

    @Test
    public void testSimpleBootstrapWithTruncateSingleChunk() throws Exception {
        String initialGarbage = "JUNKJUNK";
        String garbageAfterFailure = "junk";
        String validWriteBeforeFailure = "Hello";
        String validWriteAfterFailure = " World";
        int maxLength = 80;

        testBootstrapWithTruncate(initialGarbage, garbageAfterFailure, validWriteBeforeFailure, validWriteAfterFailure, maxLength);
    }

    /**
     * Tests two failures with truncates and zombie stores writing junk data.
     *
     * @param initialGarbageThatIsTruncated Garbage to write that is later truncated away.
     * @param garbageAfterFailure           Garbage to write from the first zombie instance.
     * @param validWriteBeforeFailure       First part of the valid data written before the failure.
     * @param validWriteAfterFailure        Second part of the valid data written after the failure.
     * @param maxLength                     Max length for the segment policy.
     * @throws Exception Throws exception in case of any error.
     */
    private void testBootstrapWithTruncate(String initialGarbageThatIsTruncated, String garbageAfterFailure,
                                           String validWriteBeforeFailure, String validWriteAfterFailure,
                                           int maxLength) throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();

        int containerId = 42;
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy).build();
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        long offset = 0;

        // Epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage1 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreBeforeCrash, executorService(), config);
        segmentStorage1.initialize(epoch);
        segmentStorage1.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        // Bootstrap
        segmentStorage1.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage1);
        checkSystemSegmentsLayout(segmentStorage1);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        val h = segmentStorage1.openWrite(systemSegmentName).join();
        val b1 = validWriteBeforeFailure.getBytes();
        byte[] garbage1 = initialGarbageThatIsTruncated.getBytes();
        int garbage1Length = garbage1.length;
        segmentStorage1.write(h, offset, new ByteArrayInputStream(garbage1), garbage1Length, null).join();
        offset += garbage1Length;
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;
        segmentStorage1.truncate(h, garbage1Length, null).join();

        // Epoch 2
        epoch++;
        @Cleanup
        ChunkedSegmentStorage segmentStorage2 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
        segmentStorage2.initialize(epoch);
        segmentStorage2.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        segmentStorage2.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage2);
        checkSystemSegmentsLayout(segmentStorage2);

        val h2 = segmentStorage2.openWrite(systemSegmentName).join();

        // Write Junk Data to both system segment and journal file.
        val innerGarbageBytes = garbageAfterFailure.getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(innerGarbageBytes), innerGarbageBytes.length, null).join();

        val b2 = validWriteAfterFailure.getBytes();
        segmentStorage2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        val info = segmentStorage2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length + garbage1Length, info.getLength());
        Assert.assertEquals(garbage1Length, info.getStartOffset());
        byte[] out = new byte[b1.length + b2.length];
        val hr = segmentStorage2.openRead(systemSegmentName).join();
        segmentStorage2.read(hr, garbage1Length, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals(validWriteBeforeFailure + validWriteAfterFailure, new String(out));
    }

    /**
     * Tests a scenario when there are two fail overs.
     * The test adds a few chunks to the system segments and then fails over.
     * It also truncates system segments.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithTwoTruncates() throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        int containerId = 42;
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(8);
        val config = getDefaultConfigBuilder(policy).build();

        long offset = 0;

        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        // Epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage1 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreBeforeCrash, executorService(), config);
        segmentStorage1.initialize(epoch);
        segmentStorage1.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        // Bootstrap
        segmentStorage1.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage1);
        checkSystemSegmentsLayout(segmentStorage1);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        val h = segmentStorage1.openWrite(systemSegmentName).join();
        segmentStorage1.write(h, offset, new ByteArrayInputStream("JUNKJUNKJUNK".getBytes()), 12, null).join();
        offset += 12;
        val b1 = "Hello".getBytes();
        segmentStorage1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;
        segmentStorage1.truncate(h, 6, null).join();

        // Epoch 2
        epoch++;
        @Cleanup
        ChunkedSegmentStorage segmentStorage2 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
        segmentStorage2.initialize(epoch);
        segmentStorage2.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();

        segmentStorage2.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage2);
        checkSystemSegmentsLayout(segmentStorage2);

        val h2 = segmentStorage2.openWrite(systemSegmentName).join();

        // Write Junk data to both system segment and journal file.
        segmentStorage1.write(h, offset, new ByteArrayInputStream("junk".getBytes()), 4, null).join();

        val b2 = " World".getBytes();
        segmentStorage2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        segmentStorage2.truncate(h, 12, null).join();

        val info = segmentStorage2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length + 12, info.getLength());
        Assert.assertEquals(12, info.getStartOffset());
        byte[] out = new byte[b1.length + b2.length];
        val hr = segmentStorage2.openRead(systemSegmentName).join();
        segmentStorage2.read(hr, 12, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    /**
     * Test simple chunk addition.
     * We failover two times to test correct interaction between snapshot and system logs.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleChunkAddition() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();

        int containerId = 42;
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        @Cleanup
        val garbageCollector1 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreBeforeCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector1.initialize(new InMemoryTaskQueueManager()).join();
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(2);
        val config = getDefaultConfigBuilder(policy).build();

        // Inital set of additions
        SystemJournal systemJournalBefore = new SystemJournal(containerId, chunkStorage, metadataStoreBeforeCrash, garbageCollector1, config, executorService());

        systemJournalBefore.bootstrap(epoch, snapshotInfoStore).join();

        String lastChunk = null;
        long totalBytesWritten = 0;
        for (int i = 0; i < 10; i++) {
            String newChunk = "chunk" + i;
            val h = chunkStorage.createWithContent(newChunk, Math.toIntExact(policy.getMaxLength()), new ByteArrayInputStream(new byte[Math.toIntExact(policy.getMaxLength())])).get();
            totalBytesWritten += policy.getMaxLength();
            val journalRecords = new ArrayList<SystemJournal.SystemJournalRecord>();
            journalRecords.add(SystemJournal.ChunkAddedRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(policy.getMaxLength() * i)
                    .newChunkName(newChunk)
                    .oldChunkName(lastChunk)
                    .build());
            journalRecords.add(SystemJournal.AppendRecord.builder()
                    .segmentName(systemSegmentName)
                    .chunkName(newChunk)
                    .offset(0)
                    .length(Math.toIntExact(policy.getMaxLength()))
                    .build());
            systemJournalBefore.commitRecords(journalRecords).join();
            lastChunk = newChunk;
        }
        Assert.assertEquals(policy.getMaxLength() * 10, totalBytesWritten);

        // Failover
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        @Cleanup
        val garbageCollector2 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector2.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash, garbageCollector2, config, executorService());

        systemJournalAfter.bootstrap(epoch + 1, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash, systemSegmentName, policy.getMaxLength(), 10);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash, systemSegmentName, 0, totalBytesWritten);

        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash2 = getMetadataStore();
        @Cleanup
        val garbageCollector3 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash2,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector3.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter2 = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash2, garbageCollector3, config, executorService());

        systemJournalAfter2.bootstrap(epoch + 2, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash2, systemSegmentName, policy.getMaxLength(), 10);
    }

    /**
     * Test simple series of operations.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleOperationSequence() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();

        int containerId = 42;
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        @Cleanup
        val garbageCollector1 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreBeforeCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector1.initialize(new InMemoryTaskQueueManager()).join();
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(2);
        val config = getDefaultConfigBuilder(policy).build();

        // Inital set of additions
        SystemJournal systemJournalBefore = new SystemJournal(containerId, chunkStorage, metadataStoreBeforeCrash, garbageCollector1, config, executorService());

        systemJournalBefore.bootstrap(epoch, snapshotInfoStore).join();

        String lastChunk = null;
        long totalBytesWritten = 0;
        for (int i = 0; i < 10; i++) {
            String newChunk = "chunk" + i;
            val h = chunkStorage.createWithContent(newChunk, Math.toIntExact(policy.getMaxLength()), new ByteArrayInputStream(new byte[Math.toIntExact(policy.getMaxLength())])).get();
            totalBytesWritten += policy.getMaxLength();
            val journalRecords = new ArrayList<SystemJournal.SystemJournalRecord>();
            journalRecords.add(SystemJournal.ChunkAddedRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(policy.getMaxLength() * i)
                    .newChunkName(newChunk)
                    .oldChunkName(lastChunk)
                    .build());
            journalRecords.add(SystemJournal.AppendRecord.builder()
                    .segmentName(systemSegmentName)
                    .chunkName(newChunk)
                    .offset(0)
                    .length(Math.toIntExact(policy.getMaxLength()))
                    .build());
            systemJournalBefore.commitRecords(journalRecords).join();
            lastChunk = newChunk;
        }
        Assert.assertEquals(policy.getMaxLength() * 10, totalBytesWritten);

        // Failover
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        @Cleanup
        val garbageCollector2 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector2.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash, garbageCollector2, config, executorService());

        systemJournalAfter.bootstrap(epoch + 1, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash, systemSegmentName, policy.getMaxLength(), 10);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash, systemSegmentName, 0, totalBytesWritten);

        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash2 = getMetadataStore();
        @Cleanup
        val garbageCollector3 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash2,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector3.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter2 = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash2, garbageCollector3, config, executorService());

        systemJournalAfter2.bootstrap(epoch + 2, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash2, systemSegmentName, policy.getMaxLength(), 10);
    }

    @Test
    public void testSimpleMigrationSequence() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();

        int containerId = 42;
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        @Cleanup
        val garbageCollector1 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreBeforeCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector1.initialize(new InMemoryTaskQueueManager()).join();
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(2);
        val config = getDefaultConfigBuilder(policy).build();

        String lastChunk = null;
        long totalBytesWritten = 0;
        // Step 1 : Create journal files.
        val journalRecords = new ArrayList<SystemJournal.SystemJournalRecord>();
        ArrayList<SystemJournal.SegmentSnapshotRecord> segmentSnapshotRecords = new ArrayList<>();
        for (val sysSegment: SystemJournal.getChunkStorageSystemSegments(containerId)) {
            segmentSnapshotRecords.add(
                SystemJournal.SegmentSnapshotRecord.builder()
                    .segmentMetadata(
                         SegmentMetadata.builder()
                            .name(sysSegment)
                            .ownerEpoch(epoch)
                            .maxRollinglength(config.getStorageMetadataRollingPolicy().getMaxLength())
                        .build()
                        .setStorageSystemSegment(true)
                        .setActive(true))
                    .chunkMetadataCollection(new Vector<>())
                .build());
        }
        val snapshotRecord = SystemJournal.SystemSnapshotRecord.builder()
                .segmentSnapshotRecords(segmentSnapshotRecords)
                .epoch(1)
                .fileIndex(1)
                .build();
        journalRecords.add(snapshotRecord);

        // Write a segment with 10 chunks.
        // Only write ChunkAddedRecord to the
        for (int i = 0; i < 10; i++) {
            String newChunk = "chunk" + i;
            val h = chunkStorage.createWithContent(newChunk, Math.toIntExact(policy.getMaxLength()), new ByteArrayInputStream(new byte[Math.toIntExact(policy.getMaxLength())])).get();
            totalBytesWritten += policy.getMaxLength();
            journalRecords.add(SystemJournal.ChunkAddedRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(policy.getMaxLength() * i)
                    .newChunkName(newChunk)
                    .oldChunkName(lastChunk)
                    .build());
            lastChunk = newChunk;
        }
        Assert.assertEquals(policy.getMaxLength() * 10, totalBytesWritten);

        // Write to journal
        val serializer = new SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer();
        ByteArraySegment journalContents = serializer.serialize(SystemJournal.SystemJournalRecordBatch.builder()
                .systemJournalRecords(journalRecords)
                .build());
        chunkStorage.createWithContent(NameUtils.getSystemJournalFileName(containerId, epoch, 1),
                journalContents.getLength(), journalContents.getReader()).join();

        // Initial set of additions
        SystemJournal systemJournalBefore = new SystemJournal(containerId, chunkStorage, metadataStoreBeforeCrash, garbageCollector1, config, executorService());

        systemJournalBefore.bootstrap(epoch + 1, snapshotInfoStore).join();

        // Failover
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        @Cleanup
        val garbageCollector2 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector2.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash, garbageCollector2, config, executorService());

        systemJournalAfter.bootstrap(epoch + 2, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash, systemSegmentName, policy.getMaxLength(), 10);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash, systemSegmentName, 0, totalBytesWritten);

        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash2 = getMetadataStore();
        @Cleanup
        val garbageCollector3 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash2,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector3.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter2 = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash2, garbageCollector3, config, executorService());

        systemJournalAfter2.bootstrap(epoch + 3, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash2, systemSegmentName, policy.getMaxLength(), 10);
    }

    @Test
    public void testSimpleMigrationSequenceWithSnapshot() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();

        int containerId = 42;
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));

        @Cleanup
        val garbageCollector1 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreBeforeCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector1.initialize(new InMemoryTaskQueueManager()).join();
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(2);
        val config = getDefaultConfigBuilder(policy).build();

        String lastChunk = null;
        long totalBytesWritten = 0;
        // Step 1 : Create journal files.
        val journalRecords = new ArrayList<SystemJournal.SystemJournalRecord>();
        ArrayList<SystemJournal.SegmentSnapshotRecord> segmentSnapshotRecords = new ArrayList<>();
        for (val sysSegment: SystemJournal.getChunkStorageSystemSegments(containerId)) {
            segmentSnapshotRecords.add(
                    SystemJournal.SegmentSnapshotRecord.builder()
                            .segmentMetadata(
                                    SegmentMetadata.builder()
                                            .name(sysSegment)
                                            .ownerEpoch(epoch)
                                            .maxRollinglength(config.getStorageMetadataRollingPolicy().getMaxLength())
                                            .build()
                                            .setStorageSystemSegment(true)
                                            .setActive(true))
                            .chunkMetadataCollection(new Vector<>())
                            .build());
        }
        val snapshotRecord = SystemJournal.SystemSnapshotRecord.builder()
                .segmentSnapshotRecords(segmentSnapshotRecords)
                .epoch(1)
                .fileIndex(1)
                .build();

        // Write a segment with 10 chunks.
        // Only write ChunkAddedRecord to the
        for (int i = 0; i < 10; i++) {
            String newChunk = "chunk" + i;
            val h = chunkStorage.createWithContent(newChunk, Math.toIntExact(policy.getMaxLength()), new ByteArrayInputStream(new byte[Math.toIntExact(policy.getMaxLength())])).get();
            totalBytesWritten += policy.getMaxLength();
            journalRecords.add(SystemJournal.ChunkAddedRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(policy.getMaxLength() * i)
                    .newChunkName(newChunk)
                    .oldChunkName(lastChunk)
                    .build());
            lastChunk = newChunk;
        }
        Assert.assertEquals(policy.getMaxLength() * 10, totalBytesWritten);

        // Write snapshot
        val snapshotSerializer = new SystemJournal.SystemSnapshotRecord.Serializer();
        ByteArraySegment snapshotContent = snapshotSerializer.serialize(snapshotRecord);
        chunkStorage.createWithContent(NameUtils.getSystemJournalSnapshotFileName(containerId, epoch, 1),
                snapshotContent.getLength(), snapshotContent.getReader()).join();

        data.setSnapshotId(containerId, SnapshotInfo.builder().epoch(epoch).snapshotId(1).build()).join();

        // Write to journal
        val serializer = new SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer();
        ByteArraySegment journalContents = serializer.serialize(SystemJournal.SystemJournalRecordBatch.builder()
                .systemJournalRecords(journalRecords)
                .build());
        chunkStorage.createWithContent(NameUtils.getSystemJournalFileName(containerId, epoch, 2),
                journalContents.getLength(), journalContents.getReader()).join();

        // Initial set of additions
        SystemJournal systemJournalBefore = new SystemJournal(containerId, chunkStorage, metadataStoreBeforeCrash, garbageCollector1, config, executorService());

        systemJournalBefore.bootstrap(epoch + 1, snapshotInfoStore).join();

        // Failover
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        @Cleanup
        val garbageCollector2 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector2.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash, garbageCollector2, config, executorService());

        systemJournalAfter.bootstrap(epoch + 2, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash, systemSegmentName, policy.getMaxLength(), 10);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash, systemSegmentName, 0, totalBytesWritten);

        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash2 = getMetadataStore();
        @Cleanup
        val garbageCollector3 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash2,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector3.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter2 = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash2, garbageCollector3, config, executorService());

        systemJournalAfter2.bootstrap(epoch + 3, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash2, systemSegmentName, policy.getMaxLength(), 10);
    }

    /**
     * Test simple chunk truncation.
     * We failover two times to test correct interaction between snapshot and system logs.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleTruncation() throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();

        int containerId = 42;
        val data = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> data.setSnapshotId(containerId, snapshotId),
                () -> data.getSnapshotId(containerId));
        @Cleanup
        val garbageCollector1 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreBeforeCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector1.initialize(new InMemoryTaskQueueManager()).join();
        String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[0];
        long epoch = 1;
        val policy = new SegmentRollingPolicy(2);
        val config = getDefaultConfigBuilder(policy).build();

        // Step 1: Initial set of additions
        SystemJournal systemJournalBefore = new SystemJournal(containerId, chunkStorage, metadataStoreBeforeCrash, garbageCollector1, config, executorService());
        systemJournalBefore.bootstrap(epoch, snapshotInfoStore).join();

        String lastChunk = null;
        long totalBytesWritten = 0;
        for (int i = 0; i < 10; i++) {
            String newChunk = "chunk" + i;
            val h = chunkStorage.createWithContent(newChunk, Math.toIntExact(policy.getMaxLength()), new ByteArrayInputStream(new byte[Math.toIntExact(policy.getMaxLength())])).get();
            totalBytesWritten += policy.getMaxLength();
            val journalRecords = new ArrayList<SystemJournal.SystemJournalRecord>();
            journalRecords.add(SystemJournal.ChunkAddedRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(policy.getMaxLength() * i)
                    .newChunkName(newChunk)
                    .oldChunkName(lastChunk)
                    .build());
            journalRecords.add(SystemJournal.AppendRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(0)
                    .chunkName(newChunk)
                    .length(Math.toIntExact(policy.getMaxLength()))
                    .build());
            systemJournalBefore.commitRecords(journalRecords).join();
            lastChunk = newChunk;
        }
        Assert.assertEquals(policy.getMaxLength() * 10, totalBytesWritten);

        // Step 2: First failover, truncate first 5 chunks.
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        @Cleanup
        val garbageCollector2 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector2.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash, garbageCollector2, config, executorService());

        systemJournalAfter.bootstrap(2, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash, systemSegmentName, policy.getMaxLength(), 10);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash, systemSegmentName, 0, totalBytesWritten);

        // Truncate first five chunks
        for (int i = 0; i <= 10; i++) {
            val firstChunkIndex = i / policy.getMaxLength();
            systemJournalAfter.commitRecord(SystemJournal.TruncationRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(i)
                    .firstChunkName("chunk" + firstChunkIndex)
                    .startOffset(policy.getMaxLength() * firstChunkIndex)
                    .build()).join();
        }

        // Step 3: Second failover, truncate last 5 chunks.
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash2 = getMetadataStore();
        @Cleanup
        val garbageCollector3 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash2,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector3.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter2 = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash2, garbageCollector3, config, executorService());

        systemJournalAfter2.bootstrap(3, snapshotInfoStore).join();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash2, systemSegmentName, policy.getMaxLength(), 5);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash2, systemSegmentName, 10, 20);

        // Truncate last five chunks
        for (int i = 10; i <= 20; i++) {
            val firstChunkIndex = i / policy.getMaxLength();
            systemJournalAfter2.commitRecord(SystemJournal.TruncationRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(i)
                    .firstChunkName("chunk" + firstChunkIndex)
                    .startOffset(policy.getMaxLength() * firstChunkIndex)
                    .build()).join();
        }

        // Step 4: third failover validate.
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash3 = getMetadataStore();
        @Cleanup
        val garbageCollector4 = new GarbageCollector(containerId,
                chunkStorage,
                metadataStoreAfterCrash2,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector4.initialize(new InMemoryTaskQueueManager()).join();
        SystemJournal systemJournalAfter3 = new SystemJournal(containerId, chunkStorage, metadataStoreAfterCrash3, garbageCollector4, config, executorService());

        systemJournalAfter3.bootstrap(4, snapshotInfoStore).join();

        TestUtils.checkSegmentBounds(metadataStoreAfterCrash3, systemSegmentName, 20, 20);
    }

    /**
     * Test concurrent writes to storage system segments by simulating concurrent writes.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSystemSegmentConcurrency() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        @Cleanup
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();

        int containerId = 42;
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = getDefaultConfigBuilder(policy).build();

        val snapshotData = new InMemorySnapshotInfoStore();
        val snapshotInfoStore = new SnapshotInfoStore(containerId,
                snapshotId -> snapshotData.setSnapshotId(containerId, snapshotId),
                () -> snapshotData.getSnapshotId(containerId));

        // Start container with epoch 1
        @Cleanup
        ChunkedSegmentStorage segmentStorage1 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreBeforeCrash, executorService(), config);

        segmentStorage1.initialize(epoch);

        // Bootstrap
        segmentStorage1.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        segmentStorage1.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage1);

        checkSystemSegmentsLayout(segmentStorage1);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        val writeSize = 10;
        val numWrites = 10;
        val numOfStorageSystemSegments = SystemJournal.getChunkStorageSystemSegments(containerId).length;
        val data = new byte[numOfStorageSystemSegments][writeSize * numWrites];

        var futures = new ArrayList<CompletableFuture<Void>>();
        val rnd = new Random(0);
        for (int i = 0; i < numOfStorageSystemSegments; i++) {
            final int k = i;
            futures.add(CompletableFuture.runAsync(() -> {
                rnd.nextBytes(data[k]);
                String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[k];
                val h = segmentStorage1.openWrite(systemSegmentName).join();
                // Init
                long offset = 0;
                for (int j = 0; j < numWrites; j++) {
                    segmentStorage1.write(h, offset, new ByteArrayInputStream(data[k], writeSize * j, writeSize), writeSize, null).join();
                    offset += writeSize;
                }
                val info = segmentStorage1.getStreamSegmentInfo(systemSegmentName, null).join();
                Assert.assertEquals(writeSize * numWrites, info.getLength());
                byte[] out = new byte[writeSize * numWrites];
                val hr = segmentStorage1.openRead(systemSegmentName).join();
                segmentStorage1.read(hr, 0, out, 0, writeSize * numWrites, null).join();
                Assert.assertArrayEquals(data[k], out);
            }, executorService()));
        }

        Futures.allOf(futures).join();
        // Step 2
        // Start container with epoch 2
        epoch++;

        @Cleanup
        ChunkedSegmentStorage segmentStorage2 = new ChunkedSegmentStorage(containerId, chunkStorage, metadataStoreAfterCrash, executorService(), config);
        segmentStorage2.initialize(epoch);

        // Bootstrap
        segmentStorage2.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        segmentStorage2.bootstrap(snapshotInfoStore).join();
        deleteGarbage(segmentStorage2);
        checkSystemSegmentsLayout(segmentStorage2);

        // Validate
        for (int i = 0; i < numOfStorageSystemSegments; i++) {
            String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(containerId)[i];
            val info = segmentStorage2.getStreamSegmentInfo(systemSegmentName, null).join();
            Assert.assertEquals(writeSize * numWrites, info.getLength());
            byte[] out = new byte[writeSize * numWrites];
            val hr = segmentStorage2.openRead(systemSegmentName).join();
            segmentStorage2.read(hr, 0, out, 0, writeSize * numWrites, null).join();
            Assert.assertArrayEquals(data[i], out);
        }
    }

    /**
     * Check system segment layout.
     */
    private void checkSystemSegmentsLayout(ChunkedSegmentStorage segmentStorage) throws Exception {
        for (String systemSegment : segmentStorage.getSystemJournal().getSystemSegments()) {
            TestUtils.checkChunksExistInStorage(segmentStorage.getChunkStorage(), segmentStorage.getMetadataStore(), systemSegment);
        }
    }

    /**
     * Tests {@link SystemJournal}  with non Appendable {@link ChunkStorage} using {@link SystemJournalTests}.
     */
    public static class NonAppendableChunkStorageSystemJournalTests extends SystemJournalTests {
        @Override
        @Before
        public void before() throws Exception {
            super.before();
        }

        @Override
        @After
        public void after() throws Exception {
            super.after();
        }

        @Override
        protected ChunkStorage getChunkStorage() throws Exception {
            val chunkStorage = new InMemoryChunkStorage(super.executorService());
            chunkStorage.setShouldSupportAppend(false);
            return chunkStorage;
        }
    }
}
