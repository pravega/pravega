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
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.integration.utils.InProcessServiceStarter;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.segmentstore.server.tables.EntrySerializer;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import lombok.Cleanup;
import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Attempts to repair an already corrupted Attribute Index in Storage of a Hash-Based Table Segment (see
 * SegmentAttributeBTreeIndex, BTreeIndex). The command works in three steps:
 *    1. It scans the main Segment data chunks (i.e., the ones that contain PUT or DELETE operations). This yields that
 *       the user of the command should provide this data in a folder so the CLI command can process it.
 *    2. Then, the CLI command starts a new in-process Pravega instance that writes data to local storage.
 *    3. All the processed PUT/DELETE operations from the original main Segment of the Table Segment are written to the
 *       local Pravega instance, so it reconstructs the Attribute Index in Storage for the original data.
 *
 * The generated Attribute Index chunks in Storage should be stored in the original cluster replacing the corrupted index.
 * This operation requires additional steps in terms of modifying the Container and Storage Metadata of the original
 * Table Segment, so it can work with the new generated data.
 *
 */
public class TableSegmentRecoveryCommand extends DataRecoveryCommand {

    private final static int DEFAULT_ROLLOVER_SIZE = 32 * 1024 * 1024; // Default rollover size for Table Segment attribute chunks.
    private final static int NUM_CONTAINERS = 1;
    private static final String ATTRIBUTE_SUFFIX = "$attributes.index";
    private final static Duration TIMEOUT = Duration.ofSeconds(10);

    private ScheduledExecutorService executor;
    private InProcessServiceStarter.PravegaRunner pravegaRunner = null;

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

        // STEP 5: Make sure that we flush all the data to Storage before closing the Pravega instance.
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        CompletableFuture<WireCommands.StorageFlushed> reply = adminSegmentHelper.flushToStorage(0,
                new PravegaNodeUri("localhost", getServiceConfig().getAdminGatewayPort()), "");
        reply.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        output("Flushed the Segment Container with containerId %d to Storage.", 0);

        // Close resources.
        ExecutorServiceHelpers.shutdown(this.executor);
        this.pravegaRunner.close();
    }

    private void setupStorageEnabledInProcPravegaCluster() throws Exception {
        /**
         * A directory for FILESYSTEM storage as LTS.
         */
        File baseDir = Files.createTempDirectory("table-segment-recovery").toFile().getAbsoluteFile();
        System.err.println("Storage directory for this cluster is: " + baseDir.getAbsolutePath());
        FileSystemStorageConfig adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
                .build();
        StorageFactory storageFactory = new FileSystemStorageFactory(adapterConfig, this.executor);
        this.pravegaRunner = new InProcessServiceStarter.PravegaRunner(1, NUM_CONTAINERS);
        this.pravegaRunner.startBookKeeperRunner(0);
        this.pravegaRunner.startControllerAndSegmentStore(storageFactory, null);
    }

    private void writeEntriesToNewTableSegment(String tableSegment, List<TableSegmentOperation> tableSegmentOperations) throws Exception {
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        SegmentHelper segmentHelper = instantiateAdminSegmentHelper(zkClient);
        segmentHelper.createTableSegment(tableSegment, "", 0, false, 0, DEFAULT_ROLLOVER_SIZE).join();
        for (TableSegmentOperation operation : tableSegmentOperations) {
            if (operation instanceof PutOperation) {
                System.err.println("Writing table entry");
                segmentHelper.updateTableEntries(tableSegment, Collections.singletonList(operation.getContents()), "", 0)
                        .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            } else {
                segmentHelper.removeTableKeys(tableSegment, Collections.singletonList(operation.getContents().getKey()), "", 0)
                        .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            }
        }
    }

    private int scanAllEntriesInTableSegmentChunks(ByteArraySegment byteArraySegment, List<TableSegmentOperation> entries) {
        EntrySerializer serializer = new EntrySerializer();
        int processedBytes = 0;
        int unprocessedBytesFromLastChunk = 0;
        while (processedBytes < byteArraySegment.getLength()) {
            try {
                ByteArraySegment slice = byteArraySegment.slice(processedBytes, byteArraySegment.getLength() - processedBytes);
                EntrySerializer.Header header = serializer.readHeader(slice.getBufferViewReader());
                // If the header has been parsed correctly, then we can proceed.
                int totalEntryLength = EntrySerializer.HEADER_LENGTH + header.getKeyLength() + header.getValueLength();
                processedBytes += totalEntryLength;
                byte[] keyBytes = slice.slice(EntrySerializer.HEADER_LENGTH, EntrySerializer.HEADER_LENGTH + header.getKeyLength())
                        .getReader().readNBytes(header.getKeyLength());
                byte[] valueBytes = slice.slice(EntrySerializer.HEADER_LENGTH + header.getKeyLength(), totalEntryLength)
                        .getReader().readNBytes(header.getValueLength());
                // Add operation to the list of operations to replay later on (PUT or DELETE).
                entries.add(valueBytes.length == 0 ? new DeleteOperation(TableSegmentEntry.versioned(keyBytes, valueBytes, header.getEntryVersion())) :
                        new PutOperation(TableSegmentEntry.versioned(keyBytes, valueBytes, header.getEntryVersion())));
                // Full entry read, so reset unprocessed bytes.
                Preconditions.checkState(unprocessedBytesFromLastChunk < totalEntryLength, "Some bytes are missing to process.");
                unprocessedBytesFromLastChunk = 0;
                if (entries.size() % 100 == 0) {
                    output("Progress of scanning data chunk: " + ((processedBytes * 100.0) / byteArraySegment.getLength()));
                }
            } catch (Exception e) {
                processedBytes++;
                unprocessedBytesFromLastChunk++;
                outputError("Exception while processing data. Unprocessed bytes: " + unprocessedBytesFromLastChunk);
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
