/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.store;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.SegmentContainerManager;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.ReadOnlySegmentContainerFactory;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerFactory;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.mocks.LocalSegmentContainerManager;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps create StreamSegmentStore Instances.
 */
@Slf4j
public class ServiceBuilder implements AutoCloseable {
    //region Members

    private final SegmentStoreMetrics.ThreadPool threadPoolMetrics;
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

    /**
     * Creates a new instance of the ServiceBuilder class.
     *
     * @param serviceBuilderConfig The ServiceBuilderConfig to use.
     * @param executorService      The executor to use for background tasks.
     */
    private ServiceBuilder(ServiceBuilderConfig serviceBuilderConfig, ServiceConfig serviceConfig, ScheduledExecutorService executorService) {
        this.serviceBuilderConfig = Preconditions.checkNotNull(serviceBuilderConfig, "serviceBuilderConfig");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.segmentToContainerMapper = createSegmentToContainerMapper(serviceConfig);
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
        this.threadPoolMetrics = new SegmentStoreMetrics.ThreadPool(this.executorService);
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
        this.threadPoolMetrics.close();
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
     *
     * @throws DurableDataLogException If unable to initialize DurableDataLogFactory.
     */
    public void initialize() throws DurableDataLogException {
        getSingleton(this.dataLogFactory, this.dataLogFactoryCreator).initialize();
        getSingleton(this.containerManager, this.segmentContainerManagerCreator).initialize();
    }

    /**
     * Creates or gets the instance of the SegmentContainerRegistry used throughout this ServiceBuilder.
     */
    private SegmentContainerRegistry getSegmentContainerRegistry() {
        return getSingleton(this.containerRegistry, this::createSegmentContainerRegistry);
    }

    protected ScheduledExecutorService getExecutorService() {
        return this.executorService;
    }

    //endregion

    //region Component Builders

    protected SegmentToContainerMapper createSegmentToContainerMapper(ServiceConfig serviceConfig) {
        return new SegmentToContainerMapper(serviceConfig.getContainerCount());
    }

    protected WriterFactory createWriterFactory() {
        WriterConfig writerConfig = this.serviceBuilderConfig.getConfig(WriterConfig::builder);
        return new StorageWriterFactory(writerConfig, this.executorService);
    }

    protected ReadIndexFactory createReadIndexFactory() {
        CacheFactory cacheFactory = getSingleton(this.cacheFactory, this.cacheFactoryCreator);
        ReadIndexConfig readIndexConfig = this.serviceBuilderConfig.getConfig(ReadIndexConfig::builder);
        return new ContainerReadIndexFactory(readIndexConfig, cacheFactory, this.executorService);
    }

    protected StorageFactory createStorageFactory() {
        return getSingleton(this.storageFactory, this.storageFactoryCreator);
    }

    protected SegmentContainerFactory createSegmentContainerFactory() {
        ReadIndexFactory readIndexFactory = getSingleton(this.readIndexFactory, this::createReadIndexFactory);
        StorageFactory storageFactory = createStorageFactory();
        OperationLogFactory operationLogFactory = getSingleton(this.operationLogFactory, this::createOperationLogFactory);
        WriterFactory writerFactory = getSingleton(this.writerFactory, this::createWriterFactory);
        ContainerConfig containerConfig = this.serviceBuilderConfig.getConfig(ContainerConfig::builder);
        return new StreamSegmentContainerFactory(containerConfig, operationLogFactory, readIndexFactory, writerFactory, storageFactory, this.executorService);
    }

    private SegmentContainerRegistry createSegmentContainerRegistry() {
        SegmentContainerFactory containerFactory = getSingleton(this.containerFactory, this::createSegmentContainerFactory);
        return new StreamSegmentContainerRegistry(containerFactory, this.executorService);
    }

    protected OperationLogFactory createOperationLogFactory() {
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
     * be lost when the object is garbage collected or the process terminates.
     *
     * @param builderConfig The ServiceBuilderConfig to use.
     */
    public static ServiceBuilder newInMemoryBuilder(ServiceBuilderConfig builderConfig) {
        int threadPoolSize = builderConfig.getConfig(ServiceConfig::builder).getThreadPoolSize();
        return newInMemoryBuilder(builderConfig, ExecutorServiceHelpers.newScheduledThreadPool(threadPoolSize, "segment-store"));
    }

    /**
     * Creates a new instance of the ServiceBuilder class which is contained in memory. Any data added to this service will
     * be lost when the object is garbage collected or the process terminates.
     *
     * @param builderConfig          The ServiceBuilderConfig to use.
     * @param executorService An ExecutorService to use for async operations.
     */
    public static ServiceBuilder newInMemoryBuilder(ServiceBuilderConfig builderConfig, ScheduledExecutorService executorService) {
        ServiceConfig serviceConfig = builderConfig.getConfig(ServiceConfig::builder);
        ServiceBuilder builder;
        if (serviceConfig.isReadOnlySegmentStore()) {
            // Only components required for ReadOnly SegmentStore.
            builder = new ReadOnlyServiceBuilder(builderConfig, serviceConfig, executorService);
        } else {
            // Components that are required for general SegmentStore.
            builder = new ServiceBuilder(builderConfig, serviceConfig, executorService)
                    .withCacheFactory(setup -> new InMemoryCacheFactory());
        }

        // Components that are required for all types of SegmentStore.
        return builder
                .withDataLogFactory(setup -> new InMemoryDurableDataLogFactory(setup.getExecutor()))
                .withContainerManager(setup -> new LocalSegmentContainerManager(
                        setup.getContainerRegistry(), setup.getSegmentToContainerMapper()))
                .withStorageFactory(setup -> new InMemoryStorageFactory(setup.getExecutor()))
                .withStreamSegmentStore(setup -> new StreamSegmentService(setup.getContainerRegistry(),
                        setup.getSegmentToContainerMapper()));

    }

    //endregion

    //region ReadOnlyServiceBuilder

    private static class ReadOnlyServiceBuilder extends ServiceBuilder {
        private static final int READONLY_CONTAINER_COUNT = 1; // Everything maps to a single container.

        private ReadOnlyServiceBuilder(ServiceBuilderConfig serviceBuilderConfig, ServiceConfig serviceConfig, ScheduledExecutorService executorService) {
            super(serviceBuilderConfig, serviceConfig, executorService);

            // We attach a LocalSegmentContainerManager, since we only have one Container Running.
            // Note that withContainerManager() is disabled in ReadOnlyServiceBuilder, hence we must invoke the one on
            // the parent class.
            super.withContainerManager(setup -> new LocalSegmentContainerManager(setup.getContainerRegistry(), setup.getSegmentToContainerMapper()));
        }

        @Override
        protected SegmentToContainerMapper createSegmentToContainerMapper(ServiceConfig serviceConfig) {
            return new SegmentToContainerMapper(READONLY_CONTAINER_COUNT);
        }

        @Override
        protected SegmentContainerFactory createSegmentContainerFactory() {
            StorageFactory storageFactory = createStorageFactory();
            return new ReadOnlySegmentContainerFactory(storageFactory, getExecutorService());
        }

        @Override
        public ServiceBuilder withContainerManager(Function<ComponentSetup, SegmentContainerManager> segmentContainerManagerCreator) {
            // Do nothing. We use a special SegmentContainerManager.
            log.info("Not attaching a SegmentContainerManager to ReadOnlyServiceBuilder.");
            return this;
        }

        @Override
        protected OperationLogFactory createOperationLogFactory() {
            throw new UnsupportedOperationException("Cannot create OperationLogFactory for ReadOnly SegmentStore.");
        }

        @Override
        protected ReadIndexFactory createReadIndexFactory() {
            throw new UnsupportedOperationException("Cannot create ReadIndexFactory for ReadOnly SegmentStore.");
        }

        @Override
        protected WriterFactory createWriterFactory() {
            throw new UnsupportedOperationException("Cannot create WriterFactory for ReadOnly SegmentStore.");
        }
    }

    //endregion

    //region ComponentSetup

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

    //endregion
}
