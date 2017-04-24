/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.store;

import io.pravega.common.segment.SegmentToContainerMapper;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.OperationLogFactory;
import io.pravega.service.server.ReadIndexFactory;
import io.pravega.service.server.SegmentContainerFactory;
import io.pravega.service.server.SegmentContainerManager;
import io.pravega.service.server.SegmentContainerRegistry;
import io.pravega.service.server.WriterFactory;
import io.pravega.service.server.containers.ContainerConfig;
import io.pravega.service.server.containers.StreamSegmentContainerFactory;
import io.pravega.service.server.logs.DurableLogConfig;
import io.pravega.service.server.logs.DurableLogFactory;
import io.pravega.service.server.mocks.LocalSegmentContainerManager;
import io.pravega.service.server.reading.ContainerReadIndexFactory;
import io.pravega.service.server.reading.ReadIndexConfig;
import io.pravega.service.server.writer.StorageWriterFactory;
import io.pravega.service.server.writer.WriterConfig;
import io.pravega.service.storage.CacheFactory;
import io.pravega.service.storage.DurableDataLogFactory;
import io.pravega.service.storage.StorageFactory;
import io.pravega.service.storage.mocks.InMemoryCacheFactory;
import io.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.service.storage.mocks.InMemoryStorageFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Helps create StreamSegmentStore Instances.
 */
@Slf4j
public final class ServiceBuilder implements AutoCloseable {
    //region Members

    private final SegmentToContainerMapper segmentToContainerMapper;
    private final ServiceBuilderConfig serviceBuilderConfig;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<OperationLogFactory> operationLogFactory;
    private final AtomicReference<ReadIndexFactory> readIndexFactory;
    private final AtomicReference<DurableDataLogFactory> dataLogFactory;
    private final AtomicReference<StorageFactory> storageFactory;
    private final AtomicReference<SegmentContainerFactory> containerFactory;
    private final AtomicReference<SegmentContainerRegistry> containerRegistry;
    private final AtomicReference<SegmentContainerManager> containerManager;
    private final AtomicReference<CacheFactory> cacheFactory;
    private final AtomicReference<WriterFactory> writerFactory;
    private final AtomicReference<StreamSegmentStore> streamSegmentService;
    private Function<ComponentSetup, DurableDataLogFactory> dataLogFactoryCreator;
    private Function<ComponentSetup, StorageFactory> storageFactoryCreator;
    private Function<ComponentSetup, SegmentContainerManager> segmentContainerManagerCreator;
    private Function<ComponentSetup, CacheFactory> cacheFactoryCreator;
    private Function<ComponentSetup, StreamSegmentStore> streamSegmentStoreCreator;

    //endregion

    //region Constructor

    public ServiceBuilder(ServiceBuilderConfig serviceBuilderConfig) {
        this(serviceBuilderConfig, createExecutorService(serviceBuilderConfig.getConfig(ServiceConfig::builder)));
    }

    /**
     * Creates a new instance of the ServiceBuilder class.
     *
     * @param serviceBuilderConfig The ServiceBuilderConfig to use.
     * @param executorService      The executor to use for background tasks.
     */
    public ServiceBuilder(ServiceBuilderConfig serviceBuilderConfig, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(serviceBuilderConfig, "config");
        this.serviceBuilderConfig = serviceBuilderConfig;
        ServiceConfig serviceConfig = this.serviceBuilderConfig.getConfig(ServiceConfig::builder);
        this.segmentToContainerMapper = new SegmentToContainerMapper(serviceConfig.getContainerCount());
        this.executorService = executorService;
        this.operationLogFactory = new AtomicReference<>();
        this.readIndexFactory = new AtomicReference<>();
        this.dataLogFactory = new AtomicReference<>();
        this.storageFactory = new AtomicReference<>();
        this.containerFactory = new AtomicReference<>();
        this.containerRegistry = new AtomicReference<>();
        this.containerManager = new AtomicReference<>();
        this.cacheFactory = new AtomicReference<>();
        this.writerFactory = new AtomicReference<>();
        this.streamSegmentService = new AtomicReference<>();

        // Setup default creators - we cannot use the ServiceBuilder unless all of these are setup.
        this.dataLogFactoryCreator = notConfiguredCreator(DurableDataLogFactory.class);
        this.storageFactoryCreator = notConfiguredCreator(StorageFactory.class);
        this.segmentContainerManagerCreator = notConfiguredCreator(SegmentContainerManager.class);
        this.cacheFactoryCreator = notConfiguredCreator(CacheFactory.class);
        this.streamSegmentStoreCreator = notConfiguredCreator(StreamSegmentStore.class);
    }

    private static ScheduledExecutorService createExecutorService(ServiceConfig serviceConfig) {
        val tf = new ThreadFactoryBuilder()
                .setNameFormat("segment-store-%d")
                .build();
        val executor = new ScheduledThreadPoolExecutor(serviceConfig.getThreadPoolSize(), tf);

        // Do not execute any periodic tasks after shutdown.
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        // Do not execute any delayed tasks after shutdown.
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        // Remove tasks from the executor once they are done executing. By default, even when canceled, these tasks are
        // not removed; if this setting is not enabled we could end up with leaked (and obsolete) tasks.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        closeComponent(this.containerManager);
        closeComponent(this.containerRegistry);
        closeComponent(this.dataLogFactory);
        closeComponent(this.readIndexFactory);
        closeComponent(this.cacheFactory);

        this.executorService.shutdown();
    }

    //endregion

    //region Configuration

    /**
     * Attaches the given DurableDataLogFactory creator to this ServiceBuilder. The given Function will only not be invoked
     * right away; it will be called when needed.
     *
     * @param dataLogFactoryCreator The Function to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withDataLogFactory(Function<ComponentSetup, DurableDataLogFactory> dataLogFactoryCreator) {
        Preconditions.checkNotNull(dataLogFactoryCreator, "dataLogFactoryCreator");
        this.dataLogFactoryCreator = dataLogFactoryCreator;
        return this;
    }

    /**
     * Attaches the given StorageFactory creator to this ServiceBuilder. The given Function will only not be invoked
     * right away; it will be called when needed.
     *
     * @param storageFactoryCreator The Function to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withStorageFactory(Function<ComponentSetup, StorageFactory> storageFactoryCreator) {
        Preconditions.checkNotNull(storageFactoryCreator, "storageFactoryCreator");
        this.storageFactoryCreator = storageFactoryCreator;
        return this;
    }

    /**
     * Attaches the given SegmentContainerManager creator to this ServiceBuilder. The given Function will only not be invoked
     * right away; it will be called when needed.
     *
     * @param segmentContainerManagerCreator The Function to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withContainerManager(Function<ComponentSetup, SegmentContainerManager> segmentContainerManagerCreator) {
        Preconditions.checkNotNull(segmentContainerManagerCreator, "segmentContainerManagerCreator");
        this.segmentContainerManagerCreator = segmentContainerManagerCreator;
        return this;
    }

    /**
     * Attaches the given CacheFactory creator to this ServiceBuilder. The given Function will only not be invoked
     * right away; it will be called when needed.
     *
     * @param cacheFactoryCreator The Function to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withCacheFactory(Function<ComponentSetup, CacheFactory> cacheFactoryCreator) {
        Preconditions.checkNotNull(cacheFactoryCreator, "cacheFactoryCreator");
        this.cacheFactoryCreator = cacheFactoryCreator;
        return this;
    }

    /**
     * Attaches the given StreamSegmentStore creator to this ServiceBuilder. The given Function will not be invoked
     * right away; it will be called when needed.
     *
     * @param streamSegmentStoreCreator The Function to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withStreamSegmentStore(Function<ComponentSetup, StreamSegmentStore> streamSegmentStoreCreator) {
        Preconditions.checkNotNull(streamSegmentStoreCreator, "streamSegmentStoreCreator");
        this.streamSegmentStoreCreator = streamSegmentStoreCreator;
        return this;
    }

    //endregion

    //region Service Builder

    /**
     * Creates a new instance of StreamSegmentStore using the components generated by this class.
     */
    public StreamSegmentStore createStreamSegmentService() {
        return getSingleton(this.streamSegmentService, this.streamSegmentStoreCreator);
    }

    /**
     * Initializes the ServiceBuilder.
     */
    public CompletableFuture<Void> initialize() {
        return getSingleton(this.containerManager, this.segmentContainerManagerCreator)
                .initialize();
    }

    /**
     * Creates or gets the instance of the SegmentContainerRegistry used throughout this ServiceBuilder.
     */
    private SegmentContainerRegistry getSegmentContainerRegistry() {
        return getSingleton(this.containerRegistry, this::createSegmentContainerRegistry);
    }

    //endregion

    //region Component Builders

    private WriterFactory createWriterFactory() {
        WriterConfig writerConfig = this.serviceBuilderConfig.getConfig(WriterConfig::builder);
        return new StorageWriterFactory(writerConfig, this.executorService);
    }

    private ReadIndexFactory createReadIndexFactory() {
        CacheFactory cacheFactory = getSingleton(this.cacheFactory, this.cacheFactoryCreator);
        ReadIndexConfig readIndexConfig = this.serviceBuilderConfig.getConfig(ReadIndexConfig::builder);
        return new ContainerReadIndexFactory(readIndexConfig, cacheFactory, this.executorService);
    }

    private SegmentContainerFactory createSegmentContainerFactory() {
        ReadIndexFactory readIndexFactory = getSingleton(this.readIndexFactory, this::createReadIndexFactory);
        StorageFactory storageFactory = getSingleton(this.storageFactory, this.storageFactoryCreator);
        OperationLogFactory operationLogFactory = getSingleton(this.operationLogFactory, this::createOperationLogFactory);
        WriterFactory writerFactory = getSingleton(this.writerFactory, this::createWriterFactory);
        ContainerConfig containerConfig = this.serviceBuilderConfig.getConfig(ContainerConfig::builder);
        return new StreamSegmentContainerFactory(containerConfig, operationLogFactory, readIndexFactory, writerFactory, storageFactory, this.executorService);
    }

    private SegmentContainerRegistry createSegmentContainerRegistry() {
        SegmentContainerFactory containerFactory = getSingleton(this.containerFactory, this::createSegmentContainerFactory);
        return new StreamSegmentContainerRegistry(containerFactory, this.executorService);
    }

    private OperationLogFactory createOperationLogFactory() {
        DurableDataLogFactory dataLogFactory = getSingleton(this.dataLogFactory, this.dataLogFactoryCreator);
        DurableLogConfig durableLogConfig = this.serviceBuilderConfig.getConfig(DurableLogConfig::builder);
        return new DurableLogFactory(durableLogConfig, dataLogFactory, this.executorService);
    }

    private <T> T getSingleton(AtomicReference<T> instance, Function<ComponentSetup, T> creator) {
        if (instance.get() == null) {
            instance.set(creator.apply(new ComponentSetup(this)));
        }

        return instance.get();
    }

    private <T> T getSingleton(AtomicReference<T> instance, Supplier<T> creator) {
        if (instance.get() == null) {
            instance.set(creator.get());
        }

        return instance.get();
    }

    //endregion

    //region Helpers

    private static <T> Function<ComponentSetup, T> notConfiguredCreator(Class<?> c) {
        return ignored -> {
            throw new IllegalStateException("ServiceBuilder not properly configured. Missing supplier for: " + c.getName());
        };
    }

    private static <T extends AutoCloseable> void closeComponent(AtomicReference<T> target) {
        T t = target.get();
        if (t != null) {
            try {
                t.close();
            } catch (Exception ex) {
                log.error("Error while closing ServiceBuilder: {}.", ex);
            }

            target.set(null);
        }
    }

    //endregion

    //region ServiceBuilder Factory

    /**
     * Creates a new instance of the ServiceBuilder class which is contained in memory. Any data added to this service will
     * be loss when the object is garbage collected or the process terminates.
     *
     * @param config The ServiceBuilderConfig to use.
     */
    public static ServiceBuilder newInMemoryBuilder(ServiceBuilderConfig config) {
        return attachDefaultComponents(new ServiceBuilder(config));
    }

    /**
     * Creates a new instance of the ServiceBuilder class which is contained in memory. Any data added to this service will
     * be loss when the object is garbage collected or the process terminates.
     *
     * @param config          The ServiceBuilderConfig to use.
     * @param executorService An ExecutorService to use for async operations.
     */
    public static ServiceBuilder newInMemoryBuilder(ServiceBuilderConfig config, ScheduledExecutorService executorService) {
        return attachDefaultComponents(new ServiceBuilder(config, executorService));
    }

    private static ServiceBuilder attachDefaultComponents(ServiceBuilder serviceBuilder) {
        return serviceBuilder.withCacheFactory(setup -> new InMemoryCacheFactory())
                             .withContainerManager(setup -> new LocalSegmentContainerManager(
                                     setup.getContainerRegistry(), setup.getSegmentToContainerMapper()))
                             .withStorageFactory(setup -> new InMemoryStorageFactory(setup.getExecutor()))
                             .withDataLogFactory(setup -> new InMemoryDurableDataLogFactory(setup.getExecutor()))
                             .withStreamSegmentStore(setup -> new StreamSegmentService(setup.getContainerRegistry(),
                                     setup.getSegmentToContainerMapper()));
    }

    //endregion

    /**
     * Setup helper for a ServiceBuilder component.
     */
    public static class ComponentSetup {
        private final ServiceBuilder builder;

        private ComponentSetup(ServiceBuilder builder) {
            this.builder = builder;
        }

        /**
         * Gets the Configuration with specified constructor from the ServiceBuilder's config.
         *
         * @param builderConstructor A Supplier that creates a ConfigBuilder for the desired configuration type.
         * @param <T>                The type of the Configuration to instantiate.
         */
        public <T> T getConfig(Supplier<? extends ConfigBuilder<T>> builderConstructor) {
            return this.builder.serviceBuilderConfig.getConfig(builderConstructor);
        }

        /**
         * Gets a pointer to the SegmentContainerRegistry for this ServiceBuilder.
         */
        public SegmentContainerRegistry getContainerRegistry() {
            return this.builder.getSegmentContainerRegistry();
        }

        /**
         * Gets a pointer to the SegmentToContainerMapper for this ServiceBuilder.
         */
        public SegmentToContainerMapper getSegmentToContainerMapper() {
            return this.builder.segmentToContainerMapper;
        }

        /**
         * Gets a pointer to the Executor Service for this ServiceBuilder.
         */
        public ScheduledExecutorService getExecutor() {
            return this.builder.executorService;
        }
    }
}
