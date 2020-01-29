/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.store;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.SegmentContainerManager;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.ReadOnlySegmentContainerFactory;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerFactory;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.mocks.LocalSegmentContainerManager;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.tables.TableService;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps create StreamSegmentStore Instances.
 */
@Slf4j
public class ServiceBuilder implements AutoCloseable {
    //region Members

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
    private final SegmentStoreMetrics.ThreadPool threadPoolMetrics;
    private final SegmentToContainerMapper segmentToContainerMapper;
    private final ServiceBuilderConfig serviceBuilderConfig;
    @Getter(AccessLevel.PROTECTED)
    private final ScheduledExecutorService coreExecutor;
    private final ScheduledExecutorService storageExecutor;
    private final CacheManager cacheManager;
    private final AtomicReference<OperationLogFactory> operationLogFactory;
    private final AtomicReference<ReadIndexFactory> readIndexFactory;
    private final AtomicReference<AttributeIndexFactory> attributeIndexFactory;
    private final AtomicReference<DurableDataLogFactory> dataLogFactory;
    private final AtomicReference<StorageFactory> storageFactory;
    private final AtomicReference<SegmentContainerFactory> containerFactory;
    private final AtomicReference<SegmentContainerRegistry> containerRegistry;
    private final AtomicReference<SegmentContainerManager> containerManager;
    private final AtomicReference<WriterFactory> writerFactory;
    private final AtomicReference<StreamSegmentStore> streamSegmentService;
    private final AtomicReference<TableStore> tableStoreService;
    private Function<ComponentSetup, DurableDataLogFactory> dataLogFactoryCreator;
    private Function<ComponentSetup, StorageFactory> storageFactoryCreator;
    private Function<ComponentSetup, SegmentContainerManager> segmentContainerManagerCreator;
    private Function<ComponentSetup, StreamSegmentStore> streamSegmentStoreCreator;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ServiceBuilder class.
     *
     * @param serviceBuilderConfig The ServiceBuilderConfig to use.
     */
    private ServiceBuilder(ServiceBuilderConfig serviceBuilderConfig, ServiceConfig serviceConfig, ExecutorBuilder executorBuilder) {
        this.serviceBuilderConfig = Preconditions.checkNotNull(serviceBuilderConfig, "serviceBuilderConfig");
        this.segmentToContainerMapper = createSegmentToContainerMapper(serviceConfig);
        this.operationLogFactory = new AtomicReference<>();
        this.readIndexFactory = new AtomicReference<>();
        this.attributeIndexFactory = new AtomicReference<>();
        this.dataLogFactory = new AtomicReference<>();
        this.storageFactory = new AtomicReference<>();
        this.containerFactory = new AtomicReference<>();
        this.containerRegistry = new AtomicReference<>();
        this.containerManager = new AtomicReference<>();
        this.writerFactory = new AtomicReference<>();
        this.streamSegmentService = new AtomicReference<>();
        this.tableStoreService = new AtomicReference<>();

        // Setup default creators - we cannot use the ServiceBuilder unless all of these are setup.
        this.dataLogFactoryCreator = notConfiguredCreator(DurableDataLogFactory.class);
        this.storageFactoryCreator = notConfiguredCreator(StorageFactory.class);
        this.segmentContainerManagerCreator = notConfiguredCreator(SegmentContainerManager.class);
        this.streamSegmentStoreCreator = notConfiguredCreator(StreamSegmentStore.class);

        // Setup Thread Pools.
        String instancePrefix = getInstanceIdPrefix(serviceConfig);
        this.coreExecutor = executorBuilder.apply(serviceConfig.getCoreThreadPoolSize(), instancePrefix + "core");
        this.storageExecutor = executorBuilder.apply(serviceConfig.getStorageThreadPoolSize(), instancePrefix + "storage-io");
        this.threadPoolMetrics = new SegmentStoreMetrics.ThreadPool(this.coreExecutor);

        this.cacheManager = new CacheManager(serviceConfig.getCachePolicy(), this.coreExecutor);
    }

    private String getInstanceIdPrefix(ServiceConfig serviceConfig) {
        String id = serviceConfig.getInstanceId();
        return id == null || id.isEmpty() ? "" : id + "-";
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        closeComponent(this.containerManager);
        closeComponent(this.containerRegistry);
        closeComponent(this.dataLogFactory);
        closeComponent(this.readIndexFactory);
        this.cacheManager.close();
        this.threadPoolMetrics.close();
        ExecutorServiceHelpers.shutdown(SHUTDOWN_TIMEOUT, this.storageExecutor, this.coreExecutor);
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
     * @return The new instance of StreamSegmentStore using the components generated by this class.
     */
    public StreamSegmentStore createStreamSegmentService() {
        return getSingleton(this.streamSegmentService, this.streamSegmentStoreCreator);
    }

    /**
     * Creates a new instance of TableStore using the components generated by this class.
     * @return The new instance of TableStore using the components generated by this class.
     */
    public TableStore createTableStoreService() {
        return getSingleton(this.tableStoreService, setup -> new TableService(setup.getContainerRegistry(), setup.getSegmentToContainerMapper()));
    }

    /**
     * Initializes the ServiceBuilder.
     *
     * @throws DurableDataLogException If unable to initialize DurableDataLogFactory.
     */
    public void initialize() throws DurableDataLogException {
        this.cacheManager.startAsync().awaitRunning();
        getSingleton(this.dataLogFactory, this.dataLogFactoryCreator).initialize();
        getSingleton(this.containerManager, this.segmentContainerManagerCreator).initialize();
    }

    /**
     * Creates or gets the instance of the SegmentContainerRegistry used throughout this ServiceBuilder.
     */
    private SegmentContainerRegistry getSegmentContainerRegistry() {
        return getSingleton(this.containerRegistry, this::createSegmentContainerRegistry);
    }

    //endregion

    //region Component Builders

    protected SegmentToContainerMapper createSegmentToContainerMapper(ServiceConfig serviceConfig) {
        return new SegmentToContainerMapper(serviceConfig.getContainerCount());
    }

    protected WriterFactory createWriterFactory() {
        WriterConfig writerConfig = this.serviceBuilderConfig.getConfig(WriterConfig::builder);
        return new StorageWriterFactory(writerConfig, this.coreExecutor);
    }

    protected ReadIndexFactory createReadIndexFactory() {
        ReadIndexConfig readIndexConfig = this.serviceBuilderConfig.getConfig(ReadIndexConfig::builder);
        return new ContainerReadIndexFactory(readIndexConfig, this.cacheManager, this.coreExecutor);
    }

    protected AttributeIndexFactory createAttributeIndexFactory() {
        AttributeIndexConfig config = this.serviceBuilderConfig.getConfig(AttributeIndexConfig::builder);
        return new ContainerAttributeIndexFactoryImpl(config, this.cacheManager, this.coreExecutor);
    }

    protected StorageFactory createStorageFactory() {
        return getSingleton(this.storageFactory, this.storageFactoryCreator);
    }

    protected SegmentContainerFactory createSegmentContainerFactory() {
        ReadIndexFactory readIndexFactory = getSingleton(this.readIndexFactory, this::createReadIndexFactory);
        AttributeIndexFactory attributeIndexFactory = getSingleton(this.attributeIndexFactory, this::createAttributeIndexFactory);
        StorageFactory storageFactory = createStorageFactory();
        OperationLogFactory operationLogFactory = getSingleton(this.operationLogFactory, this::createOperationLogFactory);
        WriterFactory writerFactory = getSingleton(this.writerFactory, this::createWriterFactory);
        ContainerConfig containerConfig = this.serviceBuilderConfig.getConfig(ContainerConfig::builder);
        return new StreamSegmentContainerFactory(containerConfig, operationLogFactory, readIndexFactory, attributeIndexFactory,
                writerFactory, storageFactory, this::createContainerExtensions, this.coreExecutor);
    }

    private Map<Class<? extends SegmentContainerExtension>, SegmentContainerExtension> createContainerExtensions(
            SegmentContainer container, ScheduledExecutorService executor) {
        return Collections.singletonMap(ContainerTableExtension.class, new ContainerTableExtensionImpl(container, this.cacheManager, executor));
    }

    private SegmentContainerRegistry createSegmentContainerRegistry() {
        SegmentContainerFactory containerFactory = getSingleton(this.containerFactory, this::createSegmentContainerFactory);
        return new StreamSegmentContainerRegistry(containerFactory, this.coreExecutor);
    }

    protected OperationLogFactory createOperationLogFactory() {
        DurableDataLogFactory dataLogFactory = getSingleton(this.dataLogFactory, this.dataLogFactoryCreator);
        DurableLogConfig durableLogConfig = this.serviceBuilderConfig.getConfig(DurableLogConfig::builder);
        return new DurableLogFactory(durableLogConfig, dataLogFactory, this.coreExecutor);
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
                log.error("Error while closing ServiceBuilder: ", ex);
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
     * @return The new instance of the ServiceBuilder.
     */
    public static ServiceBuilder newInMemoryBuilder(ServiceBuilderConfig builderConfig) {
        return newInMemoryBuilder(builderConfig, ExecutorServiceHelpers::newScheduledThreadPool);
    }

    /**
     * Creates a new instance of the ServiceBuilder class which is contained in memory. Any data added to this service will
     * be lost when the object is garbage collected or the process terminates.
     *
     * @param builderConfig   The ServiceBuilderConfig to use.
     * @param executorBuilder A Function that, given a thread count and a pool name, creates a ScheduledExecutorService
     *                        with the given number of threads that have the given name as prefix.
     * @return The new instance of the ServiceBuilder.
     */
    @VisibleForTesting
    public static ServiceBuilder newInMemoryBuilder(ServiceBuilderConfig builderConfig, ExecutorBuilder executorBuilder) {
        ServiceConfig serviceConfig = builderConfig.getConfig(ServiceConfig::builder);
        ServiceBuilder builder;
        if (serviceConfig.isReadOnlySegmentStore()) {
            // Only components required for ReadOnly SegmentStore.
            builder = new ReadOnlyServiceBuilder(builderConfig, serviceConfig, executorBuilder);
        } else {
            // Components that are required for general SegmentStore.
            builder = new ServiceBuilder(builderConfig, serviceConfig, executorBuilder);
        }

        // Components that are required for all types of SegmentStore.
        return builder
                .withDataLogFactory(setup -> new InMemoryDurableDataLogFactory(setup.getCoreExecutor()))
                .withContainerManager(setup -> new LocalSegmentContainerManager(
                        setup.getContainerRegistry(), setup.getSegmentToContainerMapper()))
                .withStorageFactory(setup -> new InMemoryStorageFactory(setup.getStorageExecutor()))
                .withStreamSegmentStore(setup -> new StreamSegmentService(setup.getContainerRegistry(),
                        setup.getSegmentToContainerMapper()));

    }

    @FunctionalInterface
    @VisibleForTesting
    public interface ExecutorBuilder {
        ScheduledExecutorService apply(int threadPoolSize, String name);
    }

    //endregion

    //region ReadOnlyServiceBuilder

    private static class ReadOnlyServiceBuilder extends ServiceBuilder {
        private static final int READONLY_CONTAINER_COUNT = 1; // Everything maps to a single container.

        private ReadOnlyServiceBuilder(ServiceBuilderConfig serviceBuilderConfig, ServiceConfig serviceConfig, ExecutorBuilder executorBuilder) {
            super(serviceBuilderConfig, serviceConfig, executorBuilder);

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
            return new ReadOnlySegmentContainerFactory(storageFactory, getCoreExecutor());
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
    public static class ComponentSetup implements ConfigSetup {
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
        @Override
        public <T> T getConfig(Supplier<? extends ConfigBuilder<T>> builderConstructor) {
            return this.builder.serviceBuilderConfig.getConfig(builderConstructor);
        }

        /**
         * Gets a pointer to the SegmentContainerRegistry for this ServiceBuilder.
         * @return The pointer to the SegmentContainerRegistry.
         */
        public SegmentContainerRegistry getContainerRegistry() {
            return this.builder.getSegmentContainerRegistry();
        }

        /**
         * Gets a pointer to the SegmentToContainerMapper for this ServiceBuilder.
         * @return The pointer to the SegmentToContainerMapper.
         */
        public SegmentToContainerMapper getSegmentToContainerMapper() {
            return this.builder.segmentToContainerMapper;
        }

        /**
         * Gets a pointer to the Core Executor Service for this ServiceBuilder.
         * @return The pointer to the Core Executor Service.
         */
        public ScheduledExecutorService getCoreExecutor() {
            return this.builder.coreExecutor;
        }

        /**
         * Gets a pointer to the Executor Service for this ServiceBuilder that is used for Storage access.
         * @return The pointer to the Executor Service.
         */
        public ScheduledExecutorService getStorageExecutor() {
            return this.builder.storageExecutor;
        }
    }

    //endregion
}
