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

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.concurrent.Services;
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
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.tables.TableExtensionConfig;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Loads the storage instance, recovers all segments from there.
 */
public class DurableLogRecoveryCommand extends DataRecoveryCommand {
    private static final int CONTAINER_EPOCH = 1;
    private static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);

    private static final DurableLogConfig NO_TRUNCATIONS_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10000)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 50000)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 1024 * 1024 * 1024L)
            .build();
    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().build();

    private static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig.builder().build();

    private static final ContainerConfig CONTAINER_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .build();

    private static final WriterConfig WRITER_CONFIG = WriterConfig.builder().build();

    private final ScheduledExecutorService executorService = getCommandArgs().getState().getExecutor();
    private final int containerCount;
    private final StorageFactory storageFactory;

    /**
     * Creates an instance of DurableLogRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    public DurableLogRecoveryCommand(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
        this.storageFactory = createStorageFactory(this.executorService);
    }

    @Override
    public void execute() throws Exception {
        @Cleanup
        Storage storage = this.storageFactory.createStorageAdapter();
        @Cleanup
        val zkClient = createZKClient();

        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);
        @Cleanup
        val dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, executorService);

        output("Container Count = %d", this.containerCount);

        dataLogFactory.initialize();
        output("Started ZK Client at %s.", getServiceConfig().getZkURL());

        storage.initialize(CONTAINER_EPOCH);
        output("Loaded %s Storage.", getServiceConfig().getStorageImplementation());

        output("Starting recovery...");
        // create back up of metadata segments
        Map<Integer, String> backUpMetadataSegments = ContainerRecoveryUtils.createBackUpMetadataSegments(storage,
                this.containerCount, executorService, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        @Cleanup
        Context context = createContext(executorService);

        // create debug segment container instances using new new dataLog and old storage.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = startDebugSegmentContainers(context, dataLogFactory);

        output("Containers started. Recovering all segments...");
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService, TIMEOUT);
        output("All segments recovered.");

        // Update core attributes from the backUp Metadata segments
        output("Updating core attributes for segments registered.");
        ContainerRecoveryUtils.updateCoreAttributes(backUpMetadataSegments, debugStreamSegmentContainerMap, executorService,
                TIMEOUT);

        // Flush new metadata segment to the storage
        flushToStorage(debugStreamSegmentContainerMap);

        // Waits for metadata segments to be flushed to LTS and then stops the debug segment containers
        stopDebugSegmentContainers(debugStreamSegmentContainerMap);

        output("Segments have been recovered.");
        output("Recovery Done!");
    }

    // Flushes data from Durable log to the storage
    private void flushToStorage(Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap) {
        for (val debugSegmentContainer : debugStreamSegmentContainerMap.values()) {
            output("Waiting for metadata segment of container %d to be flushed to the Long-Term storage.",
                    debugSegmentContainer.getId());
            debugSegmentContainer.flushToStorage(TIMEOUT).join();
        }
    }

    // Creates debug segment container instances, puts them in a map and returns it.
    private Map<Integer, DebugStreamSegmentContainer> startDebugSegmentContainers(Context context, BookKeeperLogFactory dataLogFactory)
            throws Exception {
        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(NO_TRUNCATIONS_DURABLE_LOG_CONFIG, dataLogFactory, executorService);

        // Create a debug segment container instances using a
        for (int containerId = 0; containerId < this.containerCount; containerId++) {
            DebugStreamSegmentContainer debugStreamSegmentContainer = new
                    DebugStreamSegmentContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                    context.getReadIndexFactory(), context.getAttributeIndexFactory(), context.getWriterFactory(), this.storageFactory,
                    context.getDefaultExtensions(), executorService);

            output("Starting debug segment container %d.", containerId);
            Services.startAsync(debugStreamSegmentContainer, executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);
        }
        return debugStreamSegmentContainerMap;
    }

    // Closes the debug segment container instances in the given map after waiting for the metadata segment to be flushed to
    // the given storage.
    private void stopDebugSegmentContainers(Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap)
            throws Exception {
        for (val debugSegmentContainerEntry : debugStreamSegmentContainerMap.entrySet()) {
            output("Stopping debug segment container %d.", debugSegmentContainerEntry.getKey());
            debugSegmentContainerEntry.getValue().close();
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-recovery", "Recovers the state of the DurableLog from the storage.");
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
}
