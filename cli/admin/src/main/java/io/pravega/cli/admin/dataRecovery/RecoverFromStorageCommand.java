/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin.dataRecovery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.TableSegmentUtils;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.ContainerRecoveryUtils;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.containers.MetadataStore;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.tables.TableExtensionConfig;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Loads the storage instance, recovers all segments from there.
 */
public class RecoverFromStorageCommand extends DataRecoveryCommand {
    private static final int CONTAINER_EPOCH = 1;
    private static final Duration TIMEOUT = Duration.ofMillis(1000 * 1000);

    private static final String ATTRIBUTE_SUFFIX = "$attributes.index";

    private static final String EVENT_PROCESSEOR_SEGMENT = "event_processor_GC"; // _system/containers/event_processor_GC.queue.3_3

    private static final String EPOCH_SPLITTER = ".E-";
    private static final String OFFSET_SPLITTER = "O-";

    private static final DurableLogConfig NO_TRUNCATIONS_DURABLE_LOG_CONFIG = DurableLogConfig.builder().with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10000).with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 50000).with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 1024 * 1024 * 1024L).build();
    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().build();

    long NO_STREAM_SEGMENT_ID = Long.MIN_VALUE;

    private static final MetadataStore.SegmentInfo.SegmentInfoSerializer SERIALIZER = new MetadataStore.SegmentInfo.SegmentInfoSerializer();

    private static final BaseMetadataStore.TransactionData.TransactionDataSerializer SLTS_SERIALIZER = new BaseMetadataStore.TransactionData.TransactionDataSerializer();

    private static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig.builder().build();

    private static final ContainerConfig CONTAINER_CONFIG = ContainerConfig.builder().with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60).build();

    private static final WriterConfig WRITER_CONFIG = WriterConfig.builder().build();

    private final ScheduledExecutorService executorService = getCommandArgs().getState().getExecutor();
    //    private final String TIER2_PATH = getFileSystemStorageConfig().getRoot();
    private final int containerCount;
    private final int RETRY_ATTEMPT = 3;
    private final StorageFactory storageFactory;


    /**
     * Creates an instance of RecoverFromStorageCommand class.
     *
     * @param args The arguments for the command.
     */
    public RecoverFromStorageCommand(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
        this.storageFactory = createStorageFactory(this.executorService);
    }

    private DebugStreamSegmentContainer createDebugSegmentContainer(Context context, int containerId, DurableDataLogFactory dataLogFactory) throws Exception {
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(NO_TRUNCATIONS_DURABLE_LOG_CONFIG, dataLogFactory, executorService);
        DebugStreamSegmentContainer debugStreamSegmentContainer = new DebugStreamSegmentContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory, context.getReadIndexFactory(), context.getAttributeIndexFactory(), context.getWriterFactory(), this.storageFactory, context.getDefaultExtensions(), executorService);
        Services.startAsync(debugStreamSegmentContainer, executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return debugStreamSegmentContainer;
    }

    private static class FileComparator implements Comparator<File> {
        @Override
        public int compare(File f1, File f2) {
            String[] file1 = f1.getName().split(EPOCH_SPLITTER);
            String[] file2 = f2.getName().split(EPOCH_SPLITTER);
            String file1Epoch = file1[file1.length - 1].split("-")[0];
            String file2Epoch = file2[file2.length - 1].split("-")[0];
            if (Long.parseLong(file1Epoch) != Long.parseLong(file2Epoch)) {
                return Long.valueOf(file1Epoch).compareTo(Long.valueOf(file2Epoch));
            }
            file1 = f1.getName().split(OFFSET_SPLITTER);
            file2 = f2.getName().split(OFFSET_SPLITTER);
            return Long.valueOf(file1[file1.length - 1].split("\\.")[0]).compareTo(Long.valueOf(file2[file2.length - 1].split("\\.")[0]));
        }
    }

    public void execute() throws Exception {
        ensureArgCount(2);
        String tableSegmentDataChunksPath = getArg(0);
        int containerId = Integer.parseInt(getArg(1));

        Context context = createContext(executorService);
        @Cleanup
        val zkClient = createZKClient();

        val bkConfig = getCommandArgs().getState().getConfigBuilder().include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL())).build().getConfig(BookKeeperConfig::builder);
        @Cleanup
        val dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, executorService);
        output("Container Count = %d", this.containerCount);

        dataLogFactory.initialize();
        output("Started ZK Client at %s.", getServiceConfig().getZkURL());

        output("Starting recovery...");

        DebugStreamSegmentContainer debugStreamSegmentContainer = createDebugSegmentContainer(context, containerId, dataLogFactory);

        output("Debugsegment container initiazed...");

        // STEP 1: Get all the segment chunks related to the main Segment in order.
        File[] potentialFiles = new File(tableSegmentDataChunksPath).listFiles();
        assert potentialFiles != null;

        Map<String, List<File>> metadataSegments = segregateMetadataSegments(potentialFiles);
        Preconditions.checkState(metadataSegments.size() == 2, "Only MetadataSegment and Storage MetadataSegment chunks should be present");
        List<File> storageChunks = null;
        boolean firstRun = true;
        int attempts = 0;

        ChunkValidator chunkValidator = new ChunkValidator(debugStreamSegmentContainer);
        while( firstRun || (!chunkValidator.validate() /*&& attempts < RETRY_ATTEMPT*/ ) ) {
            if( firstRun ) firstRun = !firstRun;
            for (String segment : metadataSegments.keySet()) {
                List<File> chunks = metadataSegments.get(segment);
                chunks.sort(new FileComparator());
                if (chunks.get(0).getName().contains("storage")) {
                    storageChunks = chunks;
                    continue;
                }
                List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations = TableSegmentUtils.getOperationsFromChunks(chunks);
                writeEntriesToNewTableSegment(debugStreamSegmentContainer, NameUtils.getMetadataSegmentName(containerId), tableSegmentOperations);
            }
            //write storage entries in the end after container entries (to avoid length mismatch).
            if (storageChunks != null) {
                writeStorageEntriesToNewTableSegment(debugStreamSegmentContainer, NameUtils.getStorageMetadataSegmentName(containerId), TableSegmentUtils.getOperationsFromChunks(storageChunks));
            }

            //flush to storage
            output("Flushing to stoarge");
            flushToStorage(debugStreamSegmentContainer);
            attempts++;
        }

        Thread.sleep(5000);
        listKeysinStorage(debugStreamSegmentContainer);

//        Thread.sleep(5000);
        output("Stopping DebugSegmentContainer");
        debugStreamSegmentContainer.close();
    }

    private void listKeysinStorage(DebugStreamSegmentContainer container) {
        try {
            Map<Integer, Set<String>> segmentsByContainer = ContainerRecoveryUtils.getExistingSegments(Map.of(container.getId(), container), executorService, TIMEOUT);
            System.out.println("----------------------------");
            output("segments retrieved from storage");
            for(Set<String> segs : segmentsByContainer.values()){
                segs.forEach((seg) -> System.out.println(seg));
            }
            System.out.println("----------------------------");
        }catch(Exception e){
            output("exception while fetching all segments in storage "+e);
        }
    }

    private Map<String, List<File>> segregateMetadataSegments(File[] chunkFiles) {
        Map<String, List<File>> metadataSegments = new HashMap<>();
        Arrays.stream(chunkFiles)
                .filter(File::isFile)
                .filter(f -> !f.getName().contains(ATTRIBUTE_SUFFIX))
                .forEach(file -> {
                    String segment = file.getName().split(EPOCH_SPLITTER)[0];
                    metadataSegments.putIfAbsent(segment, new ArrayList<File>());
                    metadataSegments.get(segment).add(file);
                });
        return metadataSegments;
    }

    private void clearSegmentEntries(DebugStreamSegmentContainer debugStreamSegmentContainer) throws Exception {
        Map<Integer, Set<String>> segmentsMap = ContainerRecoveryUtils.getExistingSegments(ImmutableMap.of(debugStreamSegmentContainer.getMetadata().getContainerId(), debugStreamSegmentContainer), this.executorService, TIMEOUT);
        assert segmentsMap.entrySet().size() == 1;
        Set<String> segments = segmentsMap.entrySet().iterator().next().getValue();
        segments.forEach(segment -> {
            try {
                System.out.println("Deleting entry for " + segment);
                debugStreamSegmentContainer.deleteStreamSegment(segment, TIMEOUT);
            } catch (Exception e) {
                if (!(e instanceof StreamSegmentNotExistsException)) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void writeEntriesToNewTableSegment(DebugStreamSegmentContainer container, String tableSegment, List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations) throws Exception {
        output("Writing entries to container_meta");
        ContainerTableExtension tableExtension = container.getExtension(ContainerTableExtension.class);
        HashSet<String> deletedKeys = new HashSet<>();
        for (TableSegmentUtils.TableSegmentOperation operation : tableSegmentOperations) {
            TableSegmentEntry entry = operation.getContents();
            TableEntry unversionedEntry = TableEntry.unversioned(new ByteBufWrapper(entry.getKey().getKey()), new ByteBufWrapper(entry.getValue()));
            String seg = new String(unversionedEntry.getKey().getKey().getCopy());

//            if(! (seg.contains("RGscale") || seg.contains("abhin") ))  {
//                continue;
//            }
//            if (seg.contains("transaction")) {
//                continue;
//            }
            // if we dont delete the keys, the keys from debugcontainer startup exist, and subsequent addition of keys result in version mismatch.

            if (seg.contains(EVENT_PROCESSEOR_SEGMENT)) continue;

            if(!deletedKeys.contains(seg)) { // delete the keys only once
                System.out.println("deleting segment "+seg);
                tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
                deletedKeys.add(seg);
            }

            if (operation instanceof TableSegmentUtils.PutOperation) {
                MetadataStore.SegmentInfo segmentInfo = SERIALIZER.deserialize(new ByteArraySegment(unversionedEntry.getValue().getCopy()).getReader());
                System.out.println("Printing container metadata for segment " + segmentInfo.getProperties().toString());
                output("ContainerMeta: Writing segment " + segmentInfo.getProperties().getName());
                tableExtension.put(tableSegment, Collections.singletonList(unversionedEntry), TIMEOUT);
                if (!container.isSegmentExists(segmentInfo.getProperties().getName()) && segmentInfo.getSegmentId() != NO_STREAM_SEGMENT_ID) {
                    output("ContainerMeta: Segemnt does not  Exists " + segmentInfo.getProperties().getName());
                    container.queueMapOperation(segmentInfo.getProperties(), segmentInfo.getSegmentId());
                }
            } else {
                tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
            }
        }
    }

    private void writeStorageEntriesToNewTableSegment(DebugStreamSegmentContainer container, String tableSegment, List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations) throws Exception {
        ContainerTableExtension tableExtension = container.getExtension(ContainerTableExtension.class);
        HashSet<String> deletedKeys = new HashSet<>();
        for (TableSegmentUtils.TableSegmentOperation operation : tableSegmentOperations) {
            TableSegmentEntry entry = operation.getContents();

            TableEntry unversionedEntry = TableEntry.unversioned(new ByteBufWrapper(entry.getKey().getKey()), new ByteBufWrapper(entry.getValue()));
//            unversionedEntry = reconcileEntry(unversionedEntry);
            String segment = new String(unversionedEntry.getKey().getKey().getCopy());
//            if(! (segment.contains("RGscale") || segment.contains("abhin"))) {
//                continue;
//            }

//            if(null!=segment && segment.contains("RGcommitStreamReader")) continue;
//            if(null!=segment && segment.contains("RGabortStreamReader")) continue;
            if (segment.contains(EVENT_PROCESSEOR_SEGMENT)) continue;  //_system/containers/event_processor_GC.queue.3_3
            if (operation instanceof TableSegmentUtils.PutOperation) {
                try {
                    StorageMetadata storageMetadata = SLTS_SERIALIZER.deserialize(entry.getValue().array()).getValue();
                    if (storageMetadata != null) {
                        System.out.println("Printing storage metadata segment: " + SLTS_SERIALIZER.deserialize(entry.getValue().array()).getValue().toString());
                        System.out.println();
                    }
                }catch(Exception npe){
                    System.out.println("nullpointer "+npe);
                }
                if(!deletedKeys.contains(segment)) {
                    System.out.println("deleting segment "+segment);
                    tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
                    deletedKeys.add(segment);
                }
                output("Storage Metadata: writing segment " + segment);
                tableExtension.put(tableSegment, Collections.singletonList(unversionedEntry), TIMEOUT);
            } else {
                tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
            }
        }
    }

    private TableEntry reconcileEntry(TableEntry entry) throws IOException {
        BufferView value = entry.getValue();
        BaseMetadataStore.TransactionData transactionData = new BaseMetadataStore.TransactionData.TransactionDataSerializer().deserialize(value);
        StorageMetadata storageMetadata = transactionData.getValue();
        if (storageMetadata instanceof SegmentMetadata) {
            SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
            String lastChunk = segmentMetadata.getLastChunk();
            File chunkFile = new File(lastChunk);
            if (chunkFile.exists()) {
                long expectedChunkLength = ((SegmentMetadata) storageMetadata).getLength() - ((SegmentMetadata) storageMetadata).getLastChunkStartOffset();
                if (expectedChunkLength != chunkFile.length()) {
                    ((SegmentMetadata) storageMetadata).setLength(chunkFile.length());
                    ByteArraySegment transactionBytes = new BaseMetadataStore.TransactionData.TransactionDataSerializer().serialize(transactionData);
                    return TableEntry.unversioned(entry.getKey().getKey(), transactionBytes);
                }
            } else {
                output("Did not find the last chunk for segment " + segmentMetadata.getName());
            }
        } else if (storageMetadata instanceof ChunkMetadata) {
            ChunkMetadata chunkMetadata = (ChunkMetadata) storageMetadata;
            File chunkFile = new File(chunkMetadata.getName());
            if (null == chunkMetadata.getNextChunk() && chunkFile.exists()) {
                if (chunkMetadata.getLength() != chunkFile.length()) {
                    chunkMetadata.setLength(chunkFile.length());
                    ByteArraySegment transactionBytes = new BaseMetadataStore.TransactionData.TransactionDataSerializer().serialize(transactionData);
                    return TableEntry.unversioned(entry.getKey().getKey(), transactionBytes);
                }
            }
        }
        return null;
    }

    private void flushToStorage(DebugStreamSegmentContainer debugSegmentContainer) {
        debugSegmentContainer.flushToStorage(TIMEOUT).join();
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "recover-from-storage", "Recover the state of a container from what is present on tier-2.");
    }

    // Creates the environment for debug segment container
    private static Context createContext(ScheduledExecutorService scheduledExecutorService) {
        return new Context(scheduledExecutorService);
    }

    private interface Validator {
       boolean validate() throws Exception;
    }

    private class ChunkValidator implements Validator {

        DebugStreamSegmentContainer container;

        ChunkValidator(DebugStreamSegmentContainer container) {
            this.container = container;
        }

        @Override
        public boolean validate() throws Exception {
            Map<Integer, Set<String>> segmentsByContainer = ContainerRecoveryUtils.getExistingSegments(Map.of(container.getId(), container), RecoverFromStorageCommand.this.executorService, TIMEOUT);
            for (Set<String> segs : segmentsByContainer.values()) {
                for(String seg: segs) {
                    try {
                        if (!validateSegment(seg)) {
                            return false;
                        }
                    } catch (Exception e) {
                        output("Error validating segment {}", seg);
                        return false;
                    }
                }
            }
            return true;
        }

        private boolean validateSegment(String seg) throws Exception {

            ContainerTableExtension extension = this.container.getExtension(ContainerTableExtension.class);

            try {
                output("running validation for " + seg);
                List<TableEntry> entries = extension.get(NameUtils.getStorageMetadataSegmentName(this.container.getId()), Collections.singletonList(BufferView.wrap(seg.getBytes(StandardCharsets.UTF_8))), TIMEOUT).get();
                TableEntry entry = entries.get(0);
                StorageMetadata storageMetadata = SLTS_SERIALIZER.deserialize(entry.getValue().getCopy()).getValue();

                if (storageMetadata instanceof ChunkMetadata) {
                    ChunkMetadata chunkMetdata = (ChunkMetadata) storageMetadata;
                    if (chunkMetdata.getNextChunk() != null && !chunkMetdata.getNextChunk().equalsIgnoreCase("null")) {
                        output("next chunk for "+seg+ " is "+chunkMetdata.getNextChunk());
                        return validateSegment(chunkMetdata.getNextChunk());
                    } else {
                        System.out.println("----------");
                        System.out.println("----------");
                        return true;
                    }
                }
                if (storageMetadata instanceof SegmentMetadata) {
                    SegmentMetadata segmentMetadata = (SegmentMetadata) storageMetadata;
                    output("is segmentmetadata. first chunk is  "+segmentMetadata.getFirstChunk() + " segment metadata is "+segmentMetadata.toString());
                    if (segmentMetadata.getFirstChunk() != null && !segmentMetadata.getFirstChunk().equalsIgnoreCase("null")) {
                        output("first chunk of "+seg + " is "+segmentMetadata.getFirstChunk());
                        return validateSegment(segmentMetadata.getFirstChunk());
                    }
                }
            } catch (Exception e) {
                output("There was exception fetching entry from " + NameUtils.getStorageMetadataSegmentName(this.container.getId()) + " exception " + e);
                return false;
            }
            return true;
        }
    }

    private static class Context implements AutoCloseable {
        @Getter
        public final ReadIndexFactory readIndexFactory;
        @Getter
        public final AttributeIndexFactory attributeIndexFactory;
        @Getter
        public final WriterFactory writerFactory;
        public final CacheStorage cacheStorage;
        public final CacheManager cacheManager;

        Context(ScheduledExecutorService scheduledExecutorService) {
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE / 5);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, scheduledExecutorService);
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.writerFactory = new StorageWriterFactory(WRITER_CONFIG, scheduledExecutorService);
        }

        public SegmentContainerFactory.CreateExtensions getDefaultExtensions() {
            return (c, e) -> Collections.singletonMap(ContainerTableExtension.class, createTableExtension(c, e));
        }

        private ContainerTableExtension createTableExtension(SegmentContainer c, ScheduledExecutorService e) {
            return new ContainerTableExtensionImpl(TableExtensionConfig.builder().build(), c, this.cacheManager, e);
        }

        @Override
        public void close() {
            this.readIndexFactory.close();
            this.cacheManager.close();
            this.cacheStorage.close();
        }
    }
}
