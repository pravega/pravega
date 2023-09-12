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
package io.pravega.cli.admin.segmentstore;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.segmentstore.storage.StorageUpdateSnapshotCommand;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.io.FileHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.SystemJournal;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.utils.LocalServiceStarter;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class StorageCommandsTest extends ThreadPooledTestSuite {

    private static final String SCOPE = "testScope";

    private static final Duration TIMEOUT = Duration.ofMillis(30 * 1000);
    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    // Setup utility.
    private final AtomicReference<AdminCommandState> state = new AtomicReference<>();

    /**
     * A directory for FILESYSTEM storage as LTS.
     */
    private File baseDir = null;
    private StorageFactory storageFactory = null;

    /**
     * A directory for storing logs and CSV files generated during the test..
     */
    private File logsDir = null;
    private BookKeeperLogFactory factory = null;

    @Override
    protected int getThreadPoolSize() {
        return 10;
    }

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("TestStorage").toFile().getAbsoluteFile();
        this.logsDir = Files.createTempDirectory("Storage").toFile().getAbsoluteFile();
        FileSystemStorageConfig adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
                .build();
        ChunkedSegmentStorageConfig config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        this.storageFactory = new FileSystemSimpleStorageFactory(config, adapterConfig, executorService());
    }

    @After
    public void tearDown() {
        state.get().close();
        if (this.factory != null) {
            this.factory.close();
        }
        FileHelpers.deleteFileOrDirectory(this.baseDir);
        FileHelpers.deleteFileOrDirectory(this.logsDir);
    }

    @Test
    public void testListChunksCommand() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId++);
        this.factory = new BookKeeperLogFactory(pravegaRunner.getBookKeeperRunner().getBkConfig().get(), pravegaRunner.getBookKeeperRunner().getZkClient().get(),
                executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.factory, true);
        String streamName = "testListChunksCommand";

        TestUtils.createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName, config);
        try (val clientRunner = new TestUtils.ClientRunner(pravegaRunner.getControllerRunner(), SCOPE)) {
            // Write events to the streams.
            TestUtils.writeEvents(streamName, clientRunner.getClientFactory());
        }

        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());
        for (int containerId = 0; containerId < containerCount; containerId++) {
            componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(TIMEOUT).join();
        }

        state.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.admin.gateway.port", String.valueOf(pravegaRunner.getSegmentStoreRunner().getAdminPort()));
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "CHUNKED_STORAGE");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        state.get().getConfigBuilder().include(pravegaProperties);

        String commandResult = TestUtils.executeCommand("storage list-chunks _system/containers/metadata_0 localhost", state.get());
        Assert.assertTrue(commandResult.contains("List of chunks for _system/containers/metadata_0"));
    }

    @Test
    public void testSnapshotUpdateCommand() throws Exception {

        int segmentLength = 3000;
        SystemJournal.SystemJournalRecordBatch batch = generateSystemJournalRecordBatch();
        ByteArraySegment batchBytes = new SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer()
                .serialize(batch);
        //generate segment chunk file
        Path chunkDirPath = Files.createTempDirectory("chunksDir").toAbsolutePath();
        File storageMetadataChunk = Files.createTempFile(chunkDirPath, "storage_metadata_3$attributes.index.E-1-O-0.c4971981-4492-4c3d-9828-036fd3fd56c1",
                "").toFile();
        File storageMetadataChunk2 = Files.createTempFile(chunkDirPath, "storage_metadata_3$attributes.index.E-1-O-1500.c4971981-4492-4c3d-9828-036fd3fd56c1",
                "").toFile();
        Files.write(storageMetadataChunk.toPath().toAbsolutePath(), new byte[1500]);
        Files.write(storageMetadataChunk2.toPath().toAbsolutePath(), new byte[1500]);

        //generate sysjournal file
        Path journalFileDir = Files.createTempDirectory("journalFileDir").toAbsolutePath();
        File sysJournal = Files.createTempFile(journalFileDir, "_sysjournal.epoch1.container3.file118", "").toFile();
        Files.write(sysJournal.toPath().toAbsolutePath(), batchBytes.array());

        //generate snapshot file
        Path snapshotFileDir = Files.createTempDirectory("snapshotFile").toAbsolutePath();
        File snapshotFile = Files.createTempFile(snapshotFileDir, "_sysjournal.epoch1.container3.snapshot7", "").toFile();

        //outputdir
        Path outputDir = Files.createTempDirectory("outputDir").toAbsolutePath();

        //run command
        state.set(new AdminCommandState());
        Assert.assertNotNull(StorageUpdateSnapshotCommand.descriptor());
        TestUtils.executeCommand("storage update-latest-journal-snapshot " + chunkDirPath.toAbsolutePath().toString() +
                " " + sysJournal.getAbsolutePath() +
                " " + snapshotFile.getAbsolutePath() +
                " " + outputDir.toAbsolutePath().toString() + File.separator, state.get());

        val outputJournal = Files.list(outputDir).collect(Collectors.toList());
        Assert.assertTrue("No journal created as part of", outputJournal.size() == 1);
        byte[] journalBytesRead = Files.readAllBytes(outputJournal.get(0));
        final SystemJournal.SystemSnapshotRecord.Serializer systemSnapshotSerializer = new SystemJournal.SystemSnapshotRecord.Serializer();
        SystemJournal.SystemSnapshotRecord deserializedRecord = systemSnapshotSerializer.deserialize(journalBytesRead);
        val snapshotRecords = deserializedRecord.getSegmentSnapshotRecords();
        SystemJournal.SegmentSnapshotRecord segmentRecord = snapshotRecords.iterator().next();
        Assert.assertTrue("Failure updating journal snapshot", segmentRecord.getSegmentMetadata().getLength() == segmentLength);
    }

    private SystemJournal.SystemJournalRecordBatch generateSystemJournalRecordBatch() {

        SegmentMetadata segmentMetadata = SegmentMetadata.builder()
                .chunkCount(1)
                .length(2106)
                .name("storage_metadata_3$attributes.index")
                .lastModified(System.currentTimeMillis())
                .firstChunk("_system/containers/storage_metadata_3$attributes.index.E-1-O-0.c4971981-4492-4c3d-9828-036fd3fd56c1")
                .lastChunk("_sytsem/containers/storage_metadata_3$attributes.index.E-1-O-0.c4971981-4492-4c3d-9828-036fd3fd56c1")
                .firstChunkStartOffset(0)
                .lastChunkStartOffset(2106)
                .status(25)
                .startOffset(0)
                .ownerEpoch(0)
                .maxRollinglength(1345682)
                .build();

        ChunkMetadata chunkMetadata = ChunkMetadata.builder()
                .length(2106)
                .name("_system/containers/storage_metadata_3$attributes.index.E-1-O-0.c4971981-4492-4c3d-9828-036fd3fd56c1")
                .status(25)
                .nextChunk(null)
                .build();

        SystemJournal.SegmentSnapshotRecord segmentSnapshotRecord = SystemJournal.SegmentSnapshotRecord.builder()
                .segmentMetadata(segmentMetadata)
                .chunkMetadataCollection(Collections.singletonList(chunkMetadata))
                .build();

        SystemJournal.SystemSnapshotRecord systemSnapshotRecord = SystemJournal.SystemSnapshotRecord.builder()
                .epoch(1)
                .segmentSnapshotRecords(Collections.singletonList(segmentSnapshotRecord))
                .fileIndex(1)
                .build();

        SystemJournal.SystemJournalRecordBatch batch = SystemJournal.SystemJournalRecordBatch.builder()
                .systemJournalRecords(Collections.singletonList(systemSnapshotRecord))
                .build();

        return batch;
    }

}
