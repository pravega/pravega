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
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.segmentstore.server.tables.EntrySerializer;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.test.integration.utils.LocalServiceStarter;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Attempts to repair an already corrupted Attribute Index in Storage related to a Hash-Based Table Segment (see
 * SegmentAttributeBTreeIndex, BTreeIndex). The command works in three steps:
 *    1. It scans the main Segment data chunks (i.e., the ones that contain PUT or DELETE operations). This yields that
 *       the user of the command should provide this data in a folder so the CLI command can process it.
 *    2. Then, the CLI command starts a new in-process Pravega instance that writes data to local storage.
 *    3. All the processed PUT/DELETE operations from the original main Segment of the Table Segment are written to the
 *       local Pravega instance, so it reconstructs the Attribute Index in Storage for the original data.
 *
 * The generated Attribute Index chunks in Storage should be stored back in the original cluster replacing the corrupted
 * index. This operation requires additional steps in terms of modifying the Container and Storage Metadata of the
 * original Table Segment, so it can work with the new generated data. The procedure to do so is available in the admin
 * section of https://cncf.pravega.io/docs/nightly/.
 */
public class TableSegmentRecoveryCommand extends DataRecoveryCommand {

    private final static int NUM_CONTAINERS = 1; // We need just one Container in the local Pravega instance.
    private static final String ATTRIBUTE_SUFFIX = "$attributes.index"; // We need main Segment chunks, not attribute chunks.
    private final static Duration TIMEOUT = Duration.ofSeconds(10);

    private ScheduledExecutorService executor;
    private LocalServiceStarter.PravegaRunner pravegaRunner = null;
    @Getter(AccessLevel.PACKAGE)
    private Path pravegaStorageDir;
    private long indexChunkRollingSize;

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
        ensureArgCount(3);
        String tableSegmentDataChunksPath = getArg(0);
        String newTableSegmentName = getArg(1);
        String pravegaStorageDir = getArg(2);

        // Set the rolling size of index chunks.
        this.indexChunkRollingSize = this.getAttributeIndexConfig().getAttributeSegmentRollingPolicy().getMaxLength();
        Preconditions.checkState(this.indexChunkRollingSize > 0);

        // STEP 1: Get all the segment chunks related to the main Segment in order.
        File[] potentialFiles = new File(tableSegmentDataChunksPath).listFiles();
        assert potentialFiles != null;
        List<File> listOfFiles = Arrays.stream(potentialFiles)
                .filter(File::isFile)
                .filter(f -> !f.getName().contains(ATTRIBUTE_SUFFIX)) // We are interested in the data, not the attribute segments.
                .sorted() // We need to process PUT/REMOVE operations in the right order.
                .collect(Collectors.toList());

        // STEP 2: Start a Pravega instance that is able to write data to tier 2.
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "table-segment-repair-command");
        this.pravegaStorageDir = Path.of(pravegaStorageDir);
        setupStorageEnabledLocalPravegaCluster();

        byte[] partialEntryFromLastChunk = null;
        int unprocessedBytesFromLastChunk = 0;
        for (File f : listOfFiles) {
            // STEP 3: Identify the PUT/REMOVE operations for this Segment, which are the ones that will be needed to
            // reconstruct the BTreeIndex.
            Preconditions.checkState((unprocessedBytesFromLastChunk > 0 && partialEntryFromLastChunk != null)
                    || (unprocessedBytesFromLastChunk == 0 && partialEntryFromLastChunk == null));
            ByteArraySegment chunkData = new ByteArraySegment(getBytesToProcess(partialEntryFromLastChunk, f));
            List<TableSegmentOperation> tableSegmentOperations = new ArrayList<>();
            output("Start scanning file: " + f.getName());
            unprocessedBytesFromLastChunk = scanAllEntriesInTableSegmentChunks(chunkData, tableSegmentOperations);
            partialEntryFromLastChunk = unprocessedBytesFromLastChunk > 0 ?
                    chunkData.getReader(chunkData.getLength() - unprocessedBytesFromLastChunk, unprocessedBytesFromLastChunk).readAllBytes() : null;

            // STEP 4: Write all the data to a new (hash-based) Table Segment
            writeEntriesToNewTableSegment(newTableSegmentName, tableSegmentOperations);
        }

        // STEP 5: Make sure that we flush all the data to Storage before closing the Pravega instance.
        flushDataToStorage();
        output("Command correctly executed.");

        // Close resources.
        ExecutorServiceHelpers.shutdown(this.executor);
        this.pravegaRunner.close();
    }

    /**
     * Start a local Pravega cluster with SLTS enabled, so we can use the resulting Table Segment index chunks.
     */
    private void setupStorageEnabledLocalPravegaCluster() throws Exception {
        output("Storage directory for this cluster is: " + this.pravegaStorageDir);
        FileSystemStorageConfig adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, this.pravegaStorageDir.toString())
                .build();
        StorageFactory storageFactory = new FileSystemSimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, adapterConfig, this.executor);
        this.pravegaRunner = new LocalServiceStarter.PravegaRunner(1, NUM_CONTAINERS);
        this.pravegaRunner.startBookKeeperRunner(0);
        this.pravegaRunner.startControllerAndSegmentStore(storageFactory, null, true);
    }

    /**
     * Writes a list of {@link TableSegmentOperation} to the specified Table Segment. The Table Segment will be created
     * if it does not exist.
     *
     * @param tableSegment Name of the Table Segment to write data to.
     * @param tableSegmentOperations List of operations to perform against the Table Segment.
     */
    private void writeEntriesToNewTableSegment(String tableSegment, List<TableSegmentOperation> tableSegmentOperations) throws Exception {
        @Cleanup
        SegmentHelper segmentHelper = instantiateAdminSegmentHelper(this.pravegaRunner.getBookKeeperRunner().getZkClient().get());
        Controller.NodeUri nodeUri = Controller.NodeUri.newBuilder()
                .setEndpoint("localhost")
                .setPort(this.pravegaRunner.getSegmentStoreRunner().getServicePort())
                .build();
        segmentHelper.createTableSegment(tableSegment, "", 0, false, 0, this.indexChunkRollingSize, nodeUri).join();
        PravegaNodeUri pravegaNodeUri = new PravegaNodeUri("localhost", this.pravegaRunner.getSegmentStoreRunner().getServicePort());
        for (TableSegmentOperation operation : tableSegmentOperations) {
            if (operation instanceof PutOperation) {
                segmentHelper.updateTableEntries(tableSegment, pravegaNodeUri, Collections.singletonList(operation.getContents()), "", 0)
                        .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            } else {
                segmentHelper.removeTableKeys(tableSegment, nodeUri, Collections.singletonList(operation.getContents().getKey()), "", 0)
                        .get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Given an input {@link ByteArraySegment}, this method scans it for valid Table Segment operations (e.g., PUT, DELETE).
     * It returns the amount of unprocessed bytes from the input byteArraySegment (e.g., TableEntry split within a chunk
     * rollover), so it can be used when processing the next chunk.
     *
     * @param byteArraySegment Contents of a main Segment data chunk with Table Entries.
     * @param tableSegmentOperations List to be filled with {@link TableSegmentOperation} objects according to the Table
     *                               Entries found in the original data.
     * @return Number of unprocessed bytes, normally due to a partial entry split across two different chunks.
     */
    private int scanAllEntriesInTableSegmentChunks(ByteArraySegment byteArraySegment, List<TableSegmentOperation> tableSegmentOperations) {
        EntrySerializer serializer = new EntrySerializer();
        int processedBytes = 0;
        int unprocessedBytesFromLastChunk = 0;
        while (processedBytes < byteArraySegment.getLength()) {
            try {
                ByteArraySegment slice = byteArraySegment.slice(processedBytes, byteArraySegment.getLength() - processedBytes);
                EntrySerializer.Header header = serializer.readHeader(slice.getBufferViewReader());
                // If the header has been parsed correctly, then we can proceed.
                int valueLength = header.getValueLength() < 0 ? 0 : header.getValueLength(); // In case of a removal, use 0 instead of -1.
                int totalEntryLength = EntrySerializer.HEADER_LENGTH + header.getKeyLength() + valueLength;
                byte[] keyBytes = slice.slice(EntrySerializer.HEADER_LENGTH, header.getKeyLength())
                        .getReader().readNBytes(header.getKeyLength());
                byte[] valueBytes = valueLength == 0 ? new byte[0] : slice.slice(EntrySerializer.HEADER_LENGTH + header.getKeyLength(), header.getValueLength())
                        .getReader().readNBytes(header.getValueLength());
                // Add the operation to the list of operations to replay later on (PUT or DELETE).
                tableSegmentOperations.add(valueBytes.length == 0 ?
                        new DeleteOperation(TableSegmentEntry.unversioned(keyBytes, valueBytes)) :
                        new PutOperation(TableSegmentEntry.unversioned(keyBytes, valueBytes)));
                // Full entry read, so reset unprocessed bytes.
                Preconditions.checkState(unprocessedBytesFromLastChunk < totalEntryLength, "Some bytes are missing to process.");
                unprocessedBytesFromLastChunk = 0;
                processedBytes += totalEntryLength;
                // Show the progress of scanning Table Segment operations.
                if (tableSegmentOperations.size() % 100 == 0) {
                    output("Progress of scanning data chunk: " + ((processedBytes * 100.0) / byteArraySegment.getLength()));
                }
            } catch (IOException | RuntimeException e) {
                processedBytes++;
                unprocessedBytesFromLastChunk++;
                outputError("Exception while processing data. Unprocessed bytes: " + unprocessedBytesFromLastChunk, e);
            }
        }

        return unprocessedBytesFromLastChunk;
    }

    /**
     * Loads in memory the bytes to process from input file. In the case of having bytes unprocessed from previous chunk,
     * it prepends them to the input file chunk contents.
     *
     * @param partialEntryFromLastChunk Bytes left unprocessed from previous Table Segment chunk.
     * @param f Input file handle.
     * @return Bytes from the input file, optionally prepended by the bytes from partialEntryFromLastChunk.
     */
    private byte[] getBytesToProcess(byte[] partialEntryFromLastChunk, File f) throws IOException {
        byte[] bytesToProcess;
        if (partialEntryFromLastChunk != null && partialEntryFromLastChunk.length > 0) {
            bytesToProcess = Arrays.copyOf(partialEntryFromLastChunk, (int) (partialEntryFromLastChunk.length + f.length()));
            byte[] currentChunkBytes = Files.readAllBytes(f.toPath());
            System.arraycopy(currentChunkBytes, 0, bytesToProcess, partialEntryFromLastChunk.length, currentChunkBytes.length);
        } else {
            bytesToProcess = Files.readAllBytes(f.toPath());
        }
        return bytesToProcess;
    }

    /**
     * Calls to flush data to Storage in the Segment Containers of the local Pravega instance.
     */
    private void flushDataToStorage() throws ExecutionException, InterruptedException, TimeoutException {
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(this.pravegaRunner.getBookKeeperRunner().getZkClient().get());
        CompletableFuture<WireCommands.StorageFlushed> reply = adminSegmentHelper.flushToStorage(0,
                new PravegaNodeUri("localhost", this.pravegaRunner.getSegmentStoreRunner().getAdminPort()), "");
        reply.get(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        output("Flushed the Segment Container with containerId %d to Storage.", 0);
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
        return new CommandDescriptor(COMPONENT, "tableSegment-recovery", "Allows to repair a Table Segment being damaged/corrupted.",
                new ArgDescriptor("segment-chunks-location", "Location of main Segment chunks."),
                new ArgDescriptor("new-table-segment-name", "Name of the new (hash-based) Table Segment were operations " +
                        "from the damaged one will be stored at."),
                new ArgDescriptor("pravega-local-storage-dir", "Location in the local machine to be used as Storage dir for Pravega."));
    }
}
