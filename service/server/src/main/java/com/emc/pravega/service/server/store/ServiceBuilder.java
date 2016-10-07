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

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.MetadataRepository;
import com.emc.pravega.service.server.OperationLogFactory;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.emc.pravega.service.server.SegmentContainerFactory;
import com.emc.pravega.service.server.SegmentContainerManager;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.emc.pravega.service.server.SegmentToContainerMapper;
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
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
    private Supplier<DurableDataLogFactory> dataLogFactoryCreator;
    private Supplier<StorageFactory> storageFactoryCreator;
    private Supplier<MetadataRepository> metadataRepositoryCreator;
    private Supplier<SegmentContainerManager> segmentContainerManagerCreator;
    private Supplier<CacheFactory> cacheFactoryCreator;

    //endregion

    //region Constructor

    public ServiceBuilder(ServiceBuilderConfig serviceBuilderConfig) {
        Preconditions.checkNotNull(serviceBuilderConfig, "config");
        this.serviceBuilderConfig = serviceBuilderConfig;
        ServiceConfig serviceConfig = this.serviceBuilderConfig.getServiceConfig();
        this.segmentToContainerMapper = new SegmentToContainerMapper(serviceConfig.getContainerCount());
        this.executorService = Executors.newScheduledThreadPool(serviceConfig.getThreadPoolSize());
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
        this.dataLogFactoryCreator = notImplementedCreator(DurableDataLogFactory.class);
        this.storageFactoryCreator = notImplementedCreator(StorageFactory.class);
        this.metadataRepositoryCreator = notImplementedCreator(MetadataRepository.class);
        this.segmentContainerManagerCreator = notImplementedCreator(SegmentContainerManager.class);
        this.cacheFactoryCreator = notImplementedCreator(CacheFactory.class);
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
     * Attaches the given DurableDataLogFactory creator to this ServiceBuilder. The given Supplier will only not be invoked
     * right away; it will be called when needed.
     *
     * @param dataLogFactoryCreator The Supplier to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withDataLogFactory(Supplier<DurableDataLogFactory> dataLogFactoryCreator) {
        Preconditions.checkNotNull(dataLogFactoryCreator, "dataLogFactoryCreator");
        this.dataLogFactoryCreator = dataLogFactoryCreator;
        return this;
    }

    /**
     * Attaches the given StorageFactory creator to this ServiceBuilder. The given Supplier will only not be invoked
     * right away; it will be called when needed.
     *
     * @param storageFactoryCreator The Supplier to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withStorageFactory(Supplier<StorageFactory> storageFactoryCreator) {
        Preconditions.checkNotNull(storageFactoryCreator, "storageFactoryCreator");
        this.storageFactoryCreator = storageFactoryCreator;
        return this;
    }

    /**
     * Attaches the given MetadataRepository creator to this ServiceBuilder. The given Supplier will only not be invoked
     * right away; it will be called when needed.
     *
     * @param metadataRepositoryCreator The Supplier to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withMetadataRepository(Supplier<MetadataRepository> metadataRepositoryCreator) {
        Preconditions.checkNotNull(metadataRepositoryCreator, "metadataRepositoryCreator");
        this.metadataRepositoryCreator = metadataRepositoryCreator;
        return this;
    }

    /**
     * Attaches the given SegmentContainerManager creator to this ServiceBuilder. The given Supplier will only not be invoked
     * right away; it will be called when needed.
     *
     * @param segmentContainerManagerCreator The Supplier to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withContainerManager(Supplier<SegmentContainerManager> segmentContainerManagerCreator) {
        Preconditions.checkNotNull(segmentContainerManagerCreator, "segmentContainerManagerCreator");
        this.segmentContainerManagerCreator = segmentContainerManagerCreator;
        return this;
    }

    /**
     * Attaches the given CacheFactory creator to this ServiceBuilder. The given Supplier will only not be invoked
     * right away; it will be called when needed.
     *
     * @param cacheFactoryCreator The Supplier to attach.
     * @return This ServiceBuilder.
     */
    public ServiceBuilder withCacheFactory(Supplier<CacheFactory> cacheFactoryCreator) {
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
     * Creates or gets the instance of SegmentContainerManager used throughout this ServiceBuilder.
     */
    public SegmentContainerManager getContainerManager() {
        return getSingleton(this.containerManager, this.segmentContainerManagerCreator);
    }

    /**
     * Creates or gets the instance of the SegmentContainerRegistry used throughout this ServiceBuilder.
     */
    public SegmentContainerRegistry getSegmentContainerRegistry() {
        return getSingleton(this.containerRegistry, this::createSegmentContainerRegistry);
    }

    public SegmentToContainerMapper getSegmentToContainerMapper() {
        return this.segmentToContainerMapper;
    }

    public ScheduledExecutorService getExecutorService() {
        return this.executorService;
    }

    public ServiceBuilderConfig getConfig() {
        return this.serviceBuilderConfig;
    }

    //endregion

    //region Component Builders

    private WriterFactory createWriterFactory() {
        StorageFactory storageFactory = getSingleton(this.storageFactory, this.storageFactoryCreator);
        WriterConfig writerConfig = this.serviceBuilderConfig.getWriterConfig();
        return new StorageWriterFactory(writerConfig, storageFactory, this.executorService);
    }

    private ReadIndexFactory createReadIndexFactory() {
        StorageFactory storageFactory = getSingleton(this.storageFactory, this.storageFactoryCreator);
        ReadIndexConfig readIndexConfig = this.serviceBuilderConfig.getReadIndexConfig();
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
        DurableLogConfig durableLogConfig = this.serviceBuilderConfig.getDurableLogConfig();
        return new DurableLogFactory(durableLogConfig, dataLogFactory, this.executorService);
    }

    private <T> T getSingleton(AtomicReference<T> instance, Supplier<T> creator) {
        if (instance.get() == null) {
            instance.set(creator.get());
        }

        return instance.get();
    }

    //endregion

    //region Helpers

    private static <T> Supplier<T> notImplementedCreator(Class c) {
        return () -> {
            throw new IllegalStateException("ServiceBuilder not properly configured. Missing supplier for: " + c.getName());
        };
    }

    private static <V extends AutoCloseable> void closeComponent(AtomicReference<V> target) {
        V t = target.get();
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
                .withCacheFactory(InMemoryCacheFactory::new)
                .withContainerManager(() -> new LocalSegmentContainerManager(serviceBuilder.getSegmentContainerRegistry(), serviceBuilder.getSegmentToContainerMapper()))
                .withMetadataRepository(InMemoryMetadataRepository::new)
                .withStorageFactory(() -> new InMemoryStorageFactory(serviceBuilder.getExecutorService()))
                .withDataLogFactory(InMemoryDurableDataLogFactory::new);
    }

    //endregion
}
