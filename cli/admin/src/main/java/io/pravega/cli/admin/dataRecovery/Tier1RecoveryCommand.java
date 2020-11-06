/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.dataRecovery;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.ContainerRecoveryUtils;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerFactory;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.shared.NameUtils;
import lombok.Cleanup;
import lombok.val;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Loads the storage instance, recovers all segments from there.
 */
public class Tier1RecoveryCommand extends DataRecoveryCommand {
    private static final int CONTAINER_EPOCH = 1;
    private static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);

    private final ScheduledExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(100, "recoveryProcessor");
    private final int containerCount;
    private final StorageFactory storageFactory;

    /**
     * Creates an instance of Tier1RecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    public Tier1RecoveryCommand(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
        this.storageFactory = createStorageFactory(executorService);
    }

    @Override
    public void execute() throws Exception {
        // set up logging
        setLogging(descriptor().getName());
        output(Level.INFO, "Container Count = %d", this.containerCount);

        @Cleanup
        Storage storage = this.storageFactory.createStorageAdapter();
        storage.initialize(CONTAINER_EPOCH);
        output(Level.INFO, "Loaded %s Storage.", getServiceConfig().getStorageImplementation().toString());

        val config = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);

        // Start a zk client and create a bookKeeperLogFactory
        val serviceConfig = getServiceConfig();
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, serviceConfig.getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);
        @Cleanup
        val zkClient = createZKClient();
        @Cleanup
        val factory = new BookKeeperLogFactory(bkConfig, zkClient, executorService);
        try {
            factory.initialize();
        } catch (DurableDataLogException ex) {
            zkClient.close();
            throw ex;
        }

        output(Level.INFO, "Starting recovery...");
        // create back up of metadata segments
        Map<Integer, String> backUpMetadataSegments = ContainerRecoveryUtils.createBackUpMetadataSegments(storage,
                this.containerCount, executorService, TIMEOUT);

        // Start debug segment containers
        @Cleanup
        ContainerContext context = createContainerContext(executorService, this.storageFactory, factory, config);
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = createContainers(context, this.containerCount, storage);
        output(Level.INFO, "Debug segment containers started.");

        output(Level.INFO, "Recovering all segments...");
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService);
        output(Level.INFO, "All segments recovered.");

        // Update core attributes from the backUp Metadata segments
        output(Level.INFO, "Updating core attributes for segments registered.");
        ContainerRecoveryUtils.updateCoreAttributes(backUpMetadataSegments, debugStreamSegmentContainerMap, executorService,
                TIMEOUT);

        // Waits for metadata segments to be flushed to LTS and then stops the debug segment containers
        stopDebugSegmentContainersPostFlush(debugStreamSegmentContainerMap, storage);
        output(Level.INFO, "Segments have been recovered.");
        output(Level.INFO, "Recovery Done!");
    }

    // Closes the debug segment container instances in the given map after waiting for the metadata segment to be flushed to
    // the given storage.
    private void stopDebugSegmentContainersPostFlush(Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap,
                                                     Storage storage)
            throws Exception {
        for (val debugSegmentContainer : debugStreamSegmentContainerMap.values()) {
            output(Level.FINE, "Waiting for metadata segment of container %d to be flushed to the Long-Term storage.", debugSegmentContainer.getId());
            String metadataSegmentName = NameUtils.getMetadataSegmentName(debugSegmentContainer.getId());
            waitForSegmentsInStorage(Collections.singleton(metadataSegmentName), debugSegmentContainer, storage)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Services.stopAsync(debugSegmentContainer, executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            output(Level.FINE, "Stopping debug segment container %d.", debugSegmentContainer.getId());
            debugSegmentContainer.close();
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "Tier1-recovery", "Recover Tier1 state from the storage.");
    }

    // Creates debug segment container instances, puts them in a map and returns it.
    private Map<Integer, DebugStreamSegmentContainer> createContainers(ContainerContext context, int containerCount, Storage storage) {
        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();

        // Create a debug segment container instances using a
        for (int containerId = 0; containerId < containerCount; containerId++) {
            DebugStreamSegmentContainer debugStreamSegmentContainer = (DebugStreamSegmentContainer)
                    context.containerFactory.createDebugStreamSegmentContainer(containerId);
            Services.startAsync(debugStreamSegmentContainer, executorService).join();
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);
            output(Level.FINE, "Container %d started.", containerId);
        }
        return debugStreamSegmentContainerMap;
    }

    public static ContainerContext createContainerContext(ScheduledExecutorService scheduledExecutorService, StorageFactory storageFactory,
                                                          BookKeeperLogFactory bookKeeperLogFactory, ContainerConfig containerConfig) {
        return new ContainerContext(scheduledExecutorService, storageFactory, bookKeeperLogFactory, containerConfig);
    }


    public static class ContainerContext implements AutoCloseable {
        private final StreamSegmentContainerFactory containerFactory;
        private final OperationLogFactory operationLogFactory;
        private final ReadIndexFactory readIndexFactory;
        private final AttributeIndexFactory attributeIndexFactory;
        private final WriterFactory writerFactory;
        private final CacheStorage cacheStorage;
        private final CacheManager cacheManager;
        private static final DurableLogConfig DEFAULT_DURABLE_LOG_CONFIG = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
                .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, 20)
                .build();
        private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();

        private static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 2 * 1024)
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 1000)
                .build();

        private static final WriterConfig DEFAULT_WRITER_CONFIG = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
                .build();

        ContainerContext(ScheduledExecutorService scheduledExecutorService, StorageFactory storageFactory,
                         BookKeeperLogFactory bookKeeperLogFactory, ContainerConfig containerConfig) {
            this.operationLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, bookKeeperLogFactory, scheduledExecutorService);
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, scheduledExecutorService);
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, scheduledExecutorService);
            this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, scheduledExecutorService);
            this.containerFactory = new StreamSegmentContainerFactory(containerConfig, this.operationLogFactory,
                    this.readIndexFactory, this.attributeIndexFactory, this.writerFactory, storageFactory,
                    this::createContainerExtensions, scheduledExecutorService);
        }

        private Map<Class<? extends SegmentContainerExtension>, SegmentContainerExtension> createContainerExtensions(
                SegmentContainer container, ScheduledExecutorService executor) {
            return Collections.singletonMap(ContainerTableExtension.class, new ContainerTableExtensionImpl(container, this.cacheManager, executor));
        }

        @Override
        public void close() {
            this.readIndexFactory.close();
            this.cacheManager.close();
            this.cacheStorage.close();
        }
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, DebugStreamSegmentContainer container,
                                                             Storage storage) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, storage));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties sp, Storage storage) {
        if (sp.getLength() == 0) {
            // Empty segments may or may not exist in Storage, so don't bother complicating ourselves with this.
            return CompletableFuture.completedFuture(null);
        }

        // We want to make sure that both the main segment and its attribute segment have been sync-ed to Storage. In case
        // of the attribute segment, the only thing we can easily do is verify that it has been sealed when the main segment
        // it is associated with has also been sealed.
        String attributeSegmentName = NameUtils.getAttributeSegmentName(sp.getName());
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        AtomicBoolean tryAgain = new AtomicBoolean(true);
        return Futures.loop(
                tryAgain::get,
                () -> {
                    val segInfo = getStorageSegmentInfo(sp.getName(), timer, storage);
                    val attrInfo = getStorageSegmentInfo(attributeSegmentName, timer, storage);
                    return CompletableFuture.allOf(segInfo, attrInfo)
                            .thenCompose(v -> {
                                SegmentProperties storageProps = segInfo.join();
                                SegmentProperties attrProps = attrInfo.join();
                                if (sp.isSealed()) {
                                    tryAgain.set(!storageProps.isSealed() || !(attrProps.isSealed() || attrProps.isDeleted()));
                                } else {
                                    tryAgain.set(sp.getLength() != storageProps.getLength());
                                }

                                if (tryAgain.get() && !timer.hasRemaining()) {
                                    return Futures.<Void>failedFuture(new TimeoutException(
                                            String.format("Segment %s did not complete in Storage in the allotted time.", sp.getName())));
                                } else {
                                    return Futures.delayedFuture(Duration.ofMillis(100), executorService);
                                }
                            });
                },
                executorService);
    }

    private CompletableFuture<SegmentProperties> getStorageSegmentInfo(String segmentName, TimeoutTimer timer, Storage storage) {
        return Futures
                .exceptionallyExpecting(storage.getStreamSegmentInfo(segmentName, timer.getRemaining()),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        StreamSegmentInformation.builder().name(segmentName).deleted(true).build());
    }
}
