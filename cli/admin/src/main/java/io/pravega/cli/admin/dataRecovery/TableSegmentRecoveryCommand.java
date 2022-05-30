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
package io.pravega.cli.admin.dataRecovery;

import com.google.common.base.Preconditions;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.InProcessServiceStarter;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.BufferViewBuilder;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.segmentstore.server.tables.EntrySerializer;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.test.common.TestUtils;
import io.pravega.test.integration.utils.SetupUtils;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Attempts to repair an already corrupted Attribute Index Segment in Storage (see SegmentAttributeBTreeIndex, BTreeIndex).
 * 
 *
 */
public class TableSegmentRecoveryCommand extends DataRecoveryCommand {

    private final static int DEFAULT_ROLLOVER_SIZE = 32 * 1024 * 1024; // Default rollover size for Table Segment attribute chunks.
    private static final String ATTRIBUTE_SUFFIX = "$attributes.index";
    private final static Duration TIMEOUT = Duration.ofSeconds(10);

    /**
     * A directory for FILESYSTEM storage as LTS.
     */
    private File baseDir = null;
    private StorageFactory storageFactory = null;
    private ScheduledExecutorService executor;
    private File logsDir = null;
    private BookKeeperLogFactory factory = null;

    /**
     * Creates a new instance of the DataRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    public TableSegmentRecoveryCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2);
        String tableSegmentDataChunksPath = getArg(0);
        String newTableSegmentName = getArg(1);

        // STEP 1: Get all the segment chunks related to the main Segment in order.
        File[] potentialFiles = new File(tableSegmentDataChunksPath).listFiles();
        Preconditions.checkNotNull(potentialFiles, "No files found in provided path.");
        List<File> listOfFiles = Arrays.stream(potentialFiles)
                .filter(File::isFile)
                .filter(f -> !f.getName().contains(ATTRIBUTE_SUFFIX)) // We are interested in the data, not the attribute segments.
                .sorted()
                .collect(Collectors.toList());

        // STEP 2: Start a Pravega instance that is able to write data to tier 2.
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "table-segment-repair-command");
        setupStorageEnabledInProcPravegaCluster();

        byte[] partialEntryFromLastChunk = null;
        int unprocessedBytesFromLastChunk = 0;
        for (File f : listOfFiles) {
            // STEP 3: Identify the PUT operations for this Segment, which are the ones that will be needed to reconstruct
            // the BTreeIndex.
            ByteArraySegment chunkData = new ByteArraySegment(getBytesToProcess(partialEntryFromLastChunk, unprocessedBytesFromLastChunk, f));
            List<TableSegmentOperation> tableSegmentOperations = new ArrayList<>();
            unprocessedBytesFromLastChunk = scanAllEntriesInTableSegmentChunks(chunkData, tableSegmentOperations);
            partialEntryFromLastChunk = unprocessedBytesFromLastChunk > 0 ?
                    chunkData.getReader(chunkData.getLength() - unprocessedBytesFromLastChunk, unprocessedBytesFromLastChunk).readAllBytes() : null;

            // STEP 4: Write all the data to a new (hash-based) Table Segment
            writeEntriesToNewTableSegment(newTableSegmentName, tableSegmentOperations);
        }

        // Close resources.
        ExecutorServiceHelpers.shutdown(this.executor);
    }

    private void setupStorageEnabledInProcPravegaCluster() throws Exception {
        this.baseDir = Files.createTempDirectory("TestDataRecovery").toFile().getAbsoluteFile();
        this.logsDir = Files.createTempDirectory("DataRecovery").toFile().getAbsoluteFile();
        FileSystemStorageConfig adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
                .build();
        this.storageFactory = new FileSystemStorageFactory(adapterConfig, this.executor);
        InProcessServiceStarter.PravegaRunner pravegaRunner = new InProcessServiceStarter.PravegaRunner(1, 1);
        pravegaRunner.startBookKeeperRunner(0);
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, null);
    }

    private void writeEntriesToNewTableSegment(String tableSegment, List<TableSegmentOperation> tableSegmentOperations) throws Exception {
        @Cleanup
        ConnectionPool pool = createConnectionPool();
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateSegmentHelper(zkClient, pool);
        segmentHelper.createTableSegment(tableSegment, "", 0, false, 0, DEFAULT_ROLLOVER_SIZE).join();
        for (TableSegmentOperation operation : tableSegmentOperations) {
            if (operation instanceof PutOperation) {
                segmentHelper.updateTableEntries(tableSegment, Collections.singletonList(operation.getContents()), "", 0)
                        .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            } else {
                segmentHelper.removeTableKeys(tableSegment, Collections.singletonList(operation.getContents().getKey()), "", 0)
                        .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            }
        }
    }

    private int scanAllEntriesInTableSegmentChunks(ByteArraySegment byteArraySegment, List<TableSegmentOperation> entries) throws IOException {
        EntrySerializer serializer = new EntrySerializer();
        int processedBytes = 0;
        int unprocessedBytesFromLastChunk = 0;
        while (processedBytes < byteArraySegment.getLength()) {
            try {
                ByteArraySegment slice = byteArraySegment.slice(processedBytes, byteArraySegment.getLength() - processedBytes);
                EntrySerializer.Header header = serializer.readHeader(slice.getBufferViewReader());
                // If the header has been parsed correctly, then we can proceed.
                processedBytes += EntrySerializer.HEADER_LENGTH + header.getKeyLength() + header.getValueLength();
                byte[] keyBytes = slice.slice(EntrySerializer.HEADER_LENGTH, EntrySerializer.HEADER_LENGTH + header.getKeyLength())
                        .getReader().readNBytes(header.getKeyLength());
                byte[] valueBytes = slice.slice(EntrySerializer.HEADER_LENGTH + header.getKeyLength(),
                                EntrySerializer.HEADER_LENGTH + header.getKeyLength() + header.getValueLength())
                        .getReader().readNBytes(header.getValueLength());
                System.err.println(new String(keyBytes, StandardCharsets.UTF_8) + " -> " + new String(valueBytes, StandardCharsets.UTF_8) + " version " + header.getEntryVersion());
                // Add operation to the list of operations to replay later on (PUT or DELETE).
                entries.add(valueBytes.length == 0 ? new DeleteOperation(TableSegmentEntry.versioned(keyBytes, valueBytes, header.getEntryVersion())) :
                        new PutOperation(TableSegmentEntry.versioned(keyBytes, valueBytes, header.getEntryVersion())));
                // Full entry read, so reset unprocessed bytes.
                if (unprocessedBytesFromLastChunk > EntrySerializer.HEADER_LENGTH + header.getKeyLength() + header.getValueLength()) {
                    System.err.println("WARNING: SOME BYTES ARE MISSING " + unprocessedBytesFromLastChunk);
                }
                unprocessedBytesFromLastChunk = 0;
            } catch (Exception e) {
                processedBytes++;
                unprocessedBytesFromLastChunk++;
            }
        }

        return unprocessedBytesFromLastChunk;
    }

    private byte[] getBytesToProcess(byte[] partialEntryFromLastChunk, int unprocessedBytesFromLastChunk, File f) throws IOException {
        byte[] bytesToProcess;
        if (unprocessedBytesFromLastChunk > 0) {
            Preconditions.checkState(partialEntryFromLastChunk != null);
            bytesToProcess = Arrays.copyOf(partialEntryFromLastChunk, (int) (partialEntryFromLastChunk.length + f.length()));
            byte[] currentChunkBytes = Files.readAllBytes(f.toPath());
            System.arraycopy(currentChunkBytes, 0, bytesToProcess, partialEntryFromLastChunk.length, currentChunkBytes.length);
        } else {
            bytesToProcess = Files.readAllBytes(f.toPath());
        }
        return bytesToProcess;
    }

    @Getter
    private static abstract class TableSegmentOperation {
        protected TableSegmentEntry contents;
    }

    private static class PutOperation extends TableSegmentOperation {

        protected PutOperation(TableSegmentEntry contents) {
            this.contents = contents;
        }
    }

    private static class DeleteOperation extends TableSegmentOperation {

        protected DeleteOperation(TableSegmentEntry contents) {
            this.contents = contents;
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "tableSegment-recovery", "Allows to repair a Table Segment " +
                "being damaged/corrupted.",
                new ArgDescriptor("segment-chunks-location", "Location of main Segment chunks."));
    }
}
