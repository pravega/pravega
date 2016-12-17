/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.store;

import com.emc.pravega.common.concurrent.InlineExecutor;
import com.emc.pravega.common.segment.SegmentToContainerMapper;
import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.MetadataRepository;
import com.emc.pravega.service.server.OperationLogFactory;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.emc.pravega.service.server.SegmentContainerFactory;
import com.emc.pravega.service.server.SegmentContainerManager;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.emc.pravega.service.server.WriterFactory;
import com.emc.pravega.service.server.containers.StreamSegmentContainerFactory;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.logs.DurableLogFactory;
import com.emc.pravega.service.server.mocks.InMemoryCacheFactory;
import com.emc.pravega.service.server.mocks.InMemoryMetadataRepository;
import com.emc.pravega.service.server.mocks.LocalSegmentContainerManager;
import com.emc.pravega.service.server.reading.ContainerReadIndexFactory;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.writer.StorageWriterFactory;
import com.emc.pravega.service.server.writer.WriterConfig;
import com.emc.pravega.service.storage.CacheFactory;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import com.emc.pravega.service.storage.mocks.InMemoryStorageFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

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
    private final AtomicReference<MetadataRepository> metadataRepository;
    private final AtomicReference<CacheFactory> cacheFactory;
    private final AtomicReference<WriterFactory> writerFactory;
    private Function<ComponentSetup, DurableDataLogFactory> dataLogFactoryCreator;
    private Function<ComponentSetup, StorageFactory> storageFactoryCreator;
    private Function<ComponentSetup, MetadataRepository> metadataRepositoryCreator;
    private Function<ComponentSetup, SegmentContainerManager> segmentContainerManagerCreator;
    private Function<ComponentSetup, CacheFactory> cacheFactoryCreator;

    //endregion

    //region Constructor
    
    public ServiceBuilder(ServiceBuilderConfig serviceBuilderConfig) {
        this(serviceBuilderConfig, createExecutorService(serviceBuilderConfig.getConfig(ServiceConfig::new)));
    }
    
    /**
     * Creates a new instance of the ServiceBuilder class.
     *
     * @param serviceBuilderConfig The ServiceBuilderConfig to use.
     */
    public ServiceBuilder(ServiceBuilderConfig serviceBuilderConfig, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(serviceBuilderConfig, "config");
        this.serviceBuilderConfig = serviceBuilderConfig;
        ServiceConfig serviceConfig = this.serviceBuilderConfig.getConfig(ServiceConfig::new);
        this.segmentToContainerMapper = new SegmentToContainerMapper(serviceConfig.getContainerCount());
        this.executorService = executorService;
        this.operationLogFactory = new AtomicReference<>();
        this.readIndexFactory = new AtomicReference<>();
        this.dataLogFactory = new AtomicReference<>();
        this.storageFactory = new AtomicReference<>();
        this.containerFactory = new AtomicReference<>();
        this.containerRegistry = new AtomicReference<>();
        this.containerManager = new AtomicReference<>();
        this.metadataRepository = new AtomicReference<>();
        this.cacheFactory = new AtomicReference<>();
        this.writerFactory = new AtomicReference<>();

        // Setup default creators - we cannot use the ServiceBuilder unless all of these are setup.
        this.dataLogFactoryCreator = notConfiguredCreator(DurableDataLogFactory.class);
        this.storageFactoryCreator = notConfiguredCreator(StorageFactory.class);
        this.metadataRepositoryCreator = notConfiguredCreator(MetadataRepository.class);
        this.segmentContainerManagerCreator = notConfiguredCreator(SegmentContainerManager.class);
        this.cacheFactoryCreator = notConfiguredCreator(CacheFactory.class);
    }

    private static ScheduledExecutorService createExecutorService(ServiceConfig serviceConfig) {
        val tf = new ThreadFactoryBuilder()
                .setNameFormat("segment-store-%d")
                .build();
        return Executors.newScheduledThreadPool(serviceConfig.getThreadPoolSize(), tf);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        closeComponent(this.containerManager);
        closeComponent(this.containerRegistry);
        closeComponent(this.dataLogFactory);
        closeComponent(this.readIndexFactory);
        closeComponent(this.storageFactory);
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
     * Attaches the given MetadataRepository creator to this ServiceBuilder. The given Function will only not be invoked
     * right away; it will be called when needed.
     *
     * @param metadataRepositoryCreator The Function to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withMetadataRepository(Function<ComponentSetup, MetadataRepository> metadataRepositoryCreator) {
        Preconditions.checkNotNull(metadataRepositoryCreator, "metadataRepositoryCreator");
        this.metadataRepositoryCreator = metadataRepositoryCreator;
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

    //endregion

    //region Service Builder

    /**
     * Creates a new instance of StreamSegmentStore using the components generated by this class.
     */
    public StreamSegmentStore createStreamSegmentService() {
        return new StreamSegmentService(getSegmentContainerRegistry(), this.segmentToContainerMapper);
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
    public SegmentContainerRegistry getSegmentContainerRegistry() {
        return getSingleton(this.containerRegistry, this::createSegmentContainerRegistry);
    }

    //endregion

    //region Component Builders

    private WriterFactory createWriterFactory() {
        StorageFactory storageFactory = getSingleton(this.storageFactory, this.storageFactoryCreator);
        WriterConfig writerConfig = this.serviceBuilderConfig.getConfig(WriterConfig::new);
        return new StorageWriterFactory(writerConfig, storageFactory, this.executorService);
    }

    private ReadIndexFactory createReadIndexFactory() {
        StorageFactory storageFactory = getSingleton(this.storageFactory, this.storageFactoryCreator);
        ReadIndexConfig readIndexConfig = this.serviceBuilderConfig.getConfig(ReadIndexConfig::new);
        return new ContainerReadIndexFactory(readIndexConfig, storageFactory, this.executorService);
    }

    private SegmentContainerFactory createSegmentContainerFactory() {
        MetadataRepository metadataRepository = getSingleton(this.metadataRepository, this.metadataRepositoryCreator);
        ReadIndexFactory readIndexFactory = getSingleton(this.readIndexFactory, this::createReadIndexFactory);
        StorageFactory storageFactory = getSingleton(this.storageFactory, this.storageFactoryCreator);
        OperationLogFactory operationLogFactory = getSingleton(this.operationLogFactory, this::createOperationLogFactory);
        CacheFactory cacheFactory = getSingleton(this.cacheFactory, this.cacheFactoryCreator);
        WriterFactory writerFactory = getSingleton(this.writerFactory, this::createWriterFactory);
        return new StreamSegmentContainerFactory(metadataRepository, operationLogFactory, readIndexFactory, writerFactory, storageFactory, cacheFactory, this.executorService);
    }

    private SegmentContainerRegistry createSegmentContainerRegistry() {
        SegmentContainerFactory containerFactory = getSingleton(this.containerFactory, this::createSegmentContainerFactory);
        return new StreamSegmentContainerRegistry(containerFactory, this.executorService);
    }

    private OperationLogFactory createOperationLogFactory() {
        DurableDataLogFactory dataLogFactory = getSingleton(this.dataLogFactory, this.dataLogFactoryCreator);
        DurableLogConfig durableLogConfig = this.serviceBuilderConfig.getConfig(DurableLogConfig::new);
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

    private static <T> Function<ComponentSetup, T> notConfiguredCreator(Class c) {
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
        ServiceBuilder serviceBuilder = new ServiceBuilder(config);
        return serviceBuilder
                .withCacheFactory(setup -> new InMemoryCacheFactory())
                .withContainerManager(setup -> new LocalSegmentContainerManager(setup.getContainerRegistry(), setup.getSegmentToContainerMapper()))
                .withMetadataRepository(setup -> new InMemoryMetadataRepository())
                .withStorageFactory(setup -> new InMemoryStorageFactory(setup.getExecutor()))
                .withDataLogFactory(setup -> new InMemoryDurableDataLogFactory());
    }
    
    /**
     * Same as {@link #newInMemoryBuilder(ServiceBuilderConfig)} but executes non-delayed tasks inline.
     */
    public static ServiceBuilder newInlineExecutionInMemoryBuilder(ServiceBuilderConfig config) {
        ServiceBuilder serviceBuilder = new ServiceBuilder(config, new InlineExecutor());
        return serviceBuilder
                .withCacheFactory(setup -> new InMemoryCacheFactory())
                .withContainerManager(setup -> new LocalSegmentContainerManager(setup.getContainerRegistry(), setup.getSegmentToContainerMapper()))
                .withMetadataRepository(setup -> new InMemoryMetadataRepository())
                .withStorageFactory(setup -> new InMemoryStorageFactory(setup.getExecutor()))
                .withDataLogFactory(setup -> new InMemoryDurableDataLogFactory());
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
         * Gets the ComponentConfig with specified constructor from the ServiceBuilder's config.
         *
         * @param constructor The ComponentConfig constructor.
         * @param <T>         The type of the ComponentConfig to instantiate.
         */
        public <T extends ComponentConfig> T getConfig(Function<Properties, ? extends T> constructor) {
            return this.builder.serviceBuilderConfig.getConfig(constructor);
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
