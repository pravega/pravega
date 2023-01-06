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

import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.TableSegmentUtils;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
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
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.SnapshotInfo;
import io.pravega.segmentstore.storage.chunklayer.SystemJournal;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads the storage instance, recovers all segments from there.
 */
public class RecoverFromStorage3Command extends DataRecoveryCommand {
    private static final int CONTAINER_EPOCH = 1;
    private static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);

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

    private final int BUFFER_SIZE =  8048576;
    private final StorageFactory storageFactory;


    /**
     * Creates an instance of RecoverFromStorageCommand class.
     *
     * @param args The arguments for the command.
     */
    public RecoverFromStorage3Command(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
        this.storageFactory = createStorageFactory(this.executorService);
    }

    private DebugStreamSegmentContainer createDebugSegmentContainer(Context context, int containerId, DurableDataLogFactory dataLogFactory, String path) throws Exception {
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(NO_TRUNCATIONS_DURABLE_LOG_CONFIG, dataLogFactory, executorService);
        DebugStreamSegmentContainer debugStreamSegmentContainer = new DebugContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory, context.getReadIndexFactory(), context.getAttributeIndexFactory(), context.getWriterFactory(), this.storageFactory, context.getDefaultExtensions(), executorService, path);
        Services.startAsync(debugStreamSegmentContainer, executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return debugStreamSegmentContainer;
    }

    @Override
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
        for (int i = 0; i < 1; i++) {
            output("Recovering container %s", containerId);
            DebugStreamSegmentContainer debugStreamSegmentContainer = createDebugSegmentContainer(context, containerId, dataLogFactory, tableSegmentDataChunksPath);

            // table segment raw bytes
            List<ByteArraySegment> segmentRawBytesList = readSegmentBytes(NameUtils.getMetadataSegmentName(debugStreamSegmentContainer.getId()), debugStreamSegmentContainer).get();
            List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations = TableSegmentUtils.getOperationsFromBytes(segmentRawBytesList);
            writeEntriesToContainerMetadata(debugStreamSegmentContainer, NameUtils.getMetadataSegmentName(containerId), tableSegmentOperations);

            // storage table segment raw bytes
            List<ByteArraySegment> storageSegmentBytes = readSegmentBytes(NameUtils.getStorageMetadataSegmentName(debugStreamSegmentContainer.getId()), debugStreamSegmentContainer).get();
            List<TableSegmentUtils.TableSegmentOperation> storageTableSegmentOperations = TableSegmentUtils.getOperationsFromBytes(storageSegmentBytes);
            writeEntriesToStorageMetadata(debugStreamSegmentContainer, NameUtils.getMetadataSegmentName(containerId), storageTableSegmentOperations);


            flushToStorage(debugStreamSegmentContainer);
            reconcileStorageSegment(debugStreamSegmentContainer);

            SystemJournal journal = ((ChunkedSegmentStorage) debugStreamSegmentContainer.getStorage()).getSystemJournal();
            output("Generating snapshot info for container %s", containerId);
            journal.generateSnapshotIfRequired().join();
            journal.writeSnapshotInfoIfRequired().join();
            debugStreamSegmentContainer.flushToStorage(TIMEOUT);
            Thread.sleep(5000);
            listKeysinStorage(debugStreamSegmentContainer);
            output("Stopping container %s",containerId);
            debugStreamSegmentContainer.close();

        }
    }


    private void reconcileStorageSegment(DebugStreamSegmentContainer container) throws Exception {

        output("Reconciling metadata and storage");

        Map<Integer, Set<String>> segmentsByContainer = ContainerRecoveryUtils.getExistingSegments(Map.of(container.getId(), container), executorService, TIMEOUT, NameUtils.getStorageMetadataSegmentName(container.getId()));
        Set<String> segments = segmentsByContainer.get(container.getId());

        ContainerTableExtension extension = container.getExtension(ContainerTableExtension.class);

        for(String segment: segments) {
            output("Retrieving segment %s from storage ",segment);
            List<TableEntry> entries = extension.get(NameUtils.getStorageMetadataSegmentName(container.getId()), Collections.singletonList(BufferView.wrap(segment.getBytes(StandardCharsets.UTF_8))), TIMEOUT).get();
            TableEntry entry = entries.get(0);
            StorageMetadata storageMetadata = SLTS_SERIALIZER.deserialize(entry.getValue().getCopy()).getValue();
            if (storageMetadata instanceof SegmentMetadata) {
                SegmentMetadata storageSegment = (SegmentMetadata)storageMetadata;
                List<TableEntry> segmentEntry = null;
                try {
                    segmentEntry = extension.get(NameUtils.getMetadataSegmentName(container.getId()), Collections.singletonList(BufferView.wrap(segment.getBytes(StandardCharsets.UTF_8))), TIMEOUT).get();
                    if (segmentEntry.get(0) == null) {
                        output("No data retrieved for %s",((SegmentMetadata) storageMetadata).getName());
                        continue;
                    }
                }catch(Exception e) {
                    output("There was an error finding Segment %s in container metadata. Exception %s",storageSegment.getName(), e);
                    continue;
                }
                MetadataStore.SegmentInfo segmentInfo = SERIALIZER.deserialize(segmentEntry.get(0).getValue().getCopy());
                // use the data from storage for this segment and "put" it in container Metadata
                StreamSegmentInformation segmentProperties = StreamSegmentInformation.builder()
                        .name(segmentInfo.getProperties().getName())
                        .startOffset(storageSegment.getStartOffset())
                        .length(storageSegment.getLength())
                        .sealed(storageSegment.isSealed())
                        .deleted(storageSegment.isActive())
                        .lastModified(new ImmutableDate(storageSegment.getLastModified()))
                        .attributes(segmentInfo.getProperties().getAttributes())
                        .build();
                MetadataStore.SegmentInfo sereializedContainerSegment = MetadataStore.SegmentInfo.builder()
                        .segmentId(segmentInfo.getSegmentId())
                        .properties(segmentProperties)
                        .build();
                TableEntry unversionedEntry = TableEntry.unversioned(segmentEntry.get(0).getKey().getKey(), SERIALIZER.serialize(sereializedContainerSegment));
                output("Storing segment %s in container metadata post syncing with storage segment",segmentInfo.getProperties().getName());
                extension.put(NameUtils.getMetadataSegmentName(container.getId()), Collections.singletonList(unversionedEntry), TIMEOUT).join();
            }
        }
    }

    private void writeEntriesToContainerMetadata(DebugStreamSegmentContainer container, String tableSegment, List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations) throws Exception {
        output("Writing entries to container metadata");
        ContainerTableExtension tableExtension = container.getExtension(ContainerTableExtension.class);
        for (TableSegmentUtils.TableSegmentOperation operation : tableSegmentOperations) {
            TableSegmentEntry entry = operation.getContents();
            TableEntry unversionedEntry = TableEntry.unversioned(new ByteBufWrapper(entry.getKey().getKey()), new ByteBufWrapper(entry.getValue()));
            String seg = new String(unversionedEntry.getKey().getKey().getCopy());

            if(!allowSegment(seg)) continue;

            if (operation instanceof TableSegmentUtils.PutOperation) {
                MetadataStore.SegmentInfo segmentInfo = SERIALIZER.deserialize(new ByteArraySegment(unversionedEntry.getValue().getCopy()).getReader());
                //output("ContainerMeta: Writing segment " + segmentInfo.getProperties().getName());
                tableExtension.put(tableSegment, Collections.singletonList(unversionedEntry), TIMEOUT).join();
                if (!container.isSegmentExists(segmentInfo.getProperties().getName()) && segmentInfo.getSegmentId() != NO_STREAM_SEGMENT_ID) {
                    //output("ContainerMeta: Segemnt does not  Exists " + segmentInfo.getProperties().getName());
                    container.queueMapOperation(segmentInfo.getProperties(), segmentInfo.getSegmentId());
                }
            } else {
                tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
            }
        }
    }

    private void writeEntriesToStorageMetadata(DebugStreamSegmentContainer container, String tableSegment, List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations) throws Exception {
        output("Writing entries to storage metadata");
        ContainerTableExtension tableExtension = container.getExtension(ContainerTableExtension.class);
        HashSet<String> deletedKeys = new HashSet<>();
        for (TableSegmentUtils.TableSegmentOperation operation : tableSegmentOperations) {
            TableSegmentEntry entry = operation.getContents();

            TableEntry unversionedEntry = TableEntry.unversioned(new ByteBufWrapper(entry.getKey().getKey()), new ByteBufWrapper(entry.getValue()));
            String segment = new String(unversionedEntry.getKey().getKey().getCopy());

            if(!allowSegment(segment)) continue;
            if (operation instanceof TableSegmentUtils.PutOperation) {
                try {
                    StorageMetadata storageMetadata = SLTS_SERIALIZER.deserialize(entry.getValue().array()).getValue();
                }catch(Exception npe){
                    System.out.println("nullpointer "+npe);
                }
                tableExtension.put(tableSegment, Collections.singletonList(unversionedEntry), TIMEOUT).join();
            } else {
                tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
            }
        }
    }

    private boolean allowSegment(String segmentName) {
        if(segmentName.contains("scaleGroup") || segmentName.contains(EVENT_PROCESSEOR_SEGMENT)) {
            return false;
        }
        return true;
    }

//    private void writeEntriesToNewTableSegment(DebugStreamSegmentContainer container, String tableSegment, List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations) throws Exception {
//        output("Writing entries to container_meta");
//        ContainerTableExtension tableExtension = container.getExtension(ContainerTableExtension.class);
//        HashSet<String> deletedKeys = new HashSet<>();
//        for (TableSegmentUtils.TableSegmentOperation operation : tableSegmentOperations) {
//            TableSegmentEntry entry = operation.getContents();
//            TableEntry unversionedEntry = TableEntry.unversioned(new ByteBufWrapper(entry.getKey().getKey()), new ByteBufWrapper(entry.getValue()));
//            String seg = new String(unversionedEntry.getKey().getKey().getCopy());
//
//            if (seg.contains(EVENT_PROCESSEOR_SEGMENT)) continue;
//            if(!deletedKeys.contains(seg)) { // delete the keys only once
//                System.out.println("deleting segment "+seg);
//                tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
//                deletedKeys.add(seg);
//            }
//            if (operation instanceof TableSegmentUtils.PutOperation) {
//                MetadataStore.SegmentInfo segmentInfo = SERIALIZER.deserialize(new ByteArraySegment(unversionedEntry.getValue().getCopy()).getReader());
//                System.out.println("Printing container metadata for segment " + segmentInfo.getProperties().toString());
//                output("ContainerMeta: Writing segment " + segmentInfo.getProperties().getName());
//                tableExtension.put(tableSegment, Collections.singletonList(unversionedEntry), TIMEOUT).join();
//                if (!container.isSegmentExists(segmentInfo.getProperties().getName()) && segmentInfo.getSegmentId() != NO_STREAM_SEGMENT_ID) {
//                    output("ContainerMeta: Segemnt does not  Exists " + segmentInfo.getProperties().getName());
//                    container.queueMapOperation(segmentInfo.getProperties(), segmentInfo.getSegmentId());
//                }
//            } else {
//                tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
//            }
//        }
//    }
//
//    private void writeStorageEntriesToNewTableSegment(DebugStreamSegmentContainer container, String tableSegment, List<TableSegmentUtils.TableSegmentOperation> tableSegmentOperations) throws Exception {
//        ContainerTableExtension tableExtension = container.getExtension(ContainerTableExtension.class);
//        HashSet<String> deletedKeys = new HashSet<>();
//        for (TableSegmentUtils.TableSegmentOperation operation : tableSegmentOperations) {
//            TableSegmentEntry entry = operation.getContents();
//
//            TableEntry unversionedEntry = TableEntry.unversioned(new ByteBufWrapper(entry.getKey().getKey()), new ByteBufWrapper(entry.getValue()));
//            String segment = new String(unversionedEntry.getKey().getKey().getCopy());
//
//            if (segment.contains(EVENT_PROCESSEOR_SEGMENT)) continue;  //_system/containers/event_processor_GC.queue.3_3
//            if (operation instanceof TableSegmentUtils.PutOperation) {
//                try {
//                    StorageMetadata storageMetadata = SLTS_SERIALIZER.deserialize(entry.getValue().array()).getValue();
//                    if (storageMetadata != null) {
//                        System.out.println("Printing storage metadata segment: " + SLTS_SERIALIZER.deserialize(entry.getValue().array()).getValue().toString());
//                        System.out.println();
//                    }
//                }catch(Exception npe){
//                    System.out.println("nullpointer "+npe);
//                }
//                if(!deletedKeys.contains(segment)) {
//                    System.out.println("deleting segment "+segment);
//                    tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
//                    deletedKeys.add(segment);
//                }
//                output("Storage Metadata: writing segment " + segment);
//                tableExtension.put(tableSegment, Collections.singletonList(unversionedEntry), TIMEOUT).join();
//            } else {
//                tableExtension.remove(tableSegment, Collections.singletonList(unversionedEntry.getKey()), TIMEOUT);
//            }
//        }
//    }


    private CompletableFuture<List<ByteArraySegment>> readSegmentBytes(String segment, DebugStreamSegmentContainer container) throws ExecutionException, InterruptedException {
        Storage storage = container.getStorage();
        byte[] buffer = new byte[BUFFER_SIZE];
        SegmentProperties info = storage.getStreamSegmentInfo(segment,TIMEOUT).get();
        AtomicInteger bytesToRead = new AtomicInteger((int) info.getLength());
        final List<ByteArraySegment> byteArraySegments = new ArrayList<>();
        AtomicLong totalBytesRead = new AtomicLong();
        CompletableFuture<Void> sg = storage.openRead(segment).thenCompose(segmentHandle -> {
            return Futures.loop(
                    () -> bytesToRead.get() > 0,
                    () -> {
                       return storage.read(segmentHandle, totalBytesRead.get(), buffer, 0, Math.min(BUFFER_SIZE, bytesToRead.get()), TIMEOUT)
                                .thenAcceptAsync((bytesRead) -> {
                                    bytesToRead.addAndGet(-bytesRead);
                                    byteArraySegments.add(new ByteArraySegment(buffer,0,bytesRead));
                                    totalBytesRead.set(totalBytesRead.get()+bytesRead);
                                }, executorService);
                    },
                    executorService);
        });
        return sg.handle((v, ex) -> {
            output("total bytes read for segment %s is %d",segment, totalBytesRead.get());
            return byteArraySegments;
        });
    }

    private void listKeysinStorage(DebugStreamSegmentContainer container) {
        try {
            Map<Integer, Set<String>> segmentsByContainer = ContainerRecoveryUtils.getExistingSegments(Map.of(container.getId(), container), executorService, TIMEOUT, NameUtils.getStorageMetadataSegmentName(container.getId()));
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

    public class DebugContainer extends DebugStreamSegmentContainer {

        String journalPath;

        public DebugContainer(int debugSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory,
                              ReadIndexFactory readIndexFactory, AttributeIndexFactory attributeIndexFactory,
                              WriterFactory writerFactory, StorageFactory storageFactory,
                              SegmentContainerFactory.CreateExtensions createExtensions, ScheduledExecutorService executor, String chunkPath) {
            super(debugSegmentContainerId, config, durableLogFactory, readIndexFactory, attributeIndexFactory, writerFactory,
                    storageFactory, createExtensions, executor);
            this.journalPath = chunkPath;
        }

        @Override
        @SneakyThrows
        protected CompletableFuture<SnapshotInfo> readStorageSnapshot(Duration timeout) {

            File[] journalFiles = new File(this.journalPath).listFiles();
            AtomicLong latestEpoch = new AtomicLong();
            AtomicLong latestSnapshot = new AtomicLong();
            Arrays.stream(journalFiles)
                    .filter(file -> file.getName().contains(String.valueOf("container"+this.metadata.getContainerId())))
                    .filter(file -> file.getName().contains("snapshot"))
                    .forEach(file -> {
                        String[] journalParts = file.getName().split("\\.");
                        Matcher matcherepoch = Pattern.compile("\\d+").matcher(journalParts[1]);
                        matcherepoch.find();
                        long epoch = Long.parseLong(matcherepoch.group(0)); // only one number
                        Matcher snapshotMatcher = Pattern.compile("\\d+").matcher(journalParts[3]);
                        snapshotMatcher.find();
                        long snapshot = Long.parseLong(snapshotMatcher.group(0));
                        latestEpoch.set(Math.max(latestEpoch.get(),epoch));
                        latestSnapshot.set(Math.max(latestSnapshot.get(),snapshot));
                    });
            val retValue = SnapshotInfo.builder()
                    .snapshotId(latestSnapshot.get())
                    .epoch(latestEpoch.get())
                    .build();
            output("latest epoch is %s and latest snapshot is %s for container %s",latestEpoch.get(),latestSnapshot.get(), this.metadata.getContainerId());
            return CompletableFuture.completedFuture(retValue);
        }

//        @Override
//        public CompletableFuture<Void> startSecondaryServicesAsync() {
//            CompletableFuture<Void> cf = super.startSecondaryServicesAsync();
//            return CompletableFuture.allOf(cf, super.startSecondaryServicesAsync());
//        }
//
//        private CompletableFuture<Void> syncContainerAndStorageMetadata() {
//
//
//
//            return CompletableFuture.completedFuture(null);
//        }

    }
}
