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

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.host.StorageLoader;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.tables.TableExtensionConfig;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import lombok.Getter;

import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Base class for data recovery related commands' classes.
 */
public abstract class DataRecoveryCommand extends AdminCommand {
    protected static final String COMPONENT = "storage";

    protected static final int REPAIR_LOG_ID = Integer.MAX_VALUE;
    protected static final int BACKUP_LOG_ID = Integer.MAX_VALUE - 1;

    protected static final DurableLogConfig NO_TRUNCATIONS_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10000)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 50000)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 1024 * 1024 * 1024L)
            .build();

    protected static final ContainerConfig CONTAINER_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .build();

    protected static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().build();
    protected static final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig.builder().build();
    protected static final WriterConfig WRITER_CONFIG = WriterConfig.builder().build();
    protected final ScheduledExecutorService executorService = getCommandArgs().getState().getExecutor();
    protected final StorageFactory storageFactory = createStorageFactory(this.executorService);

    /**
     * Creates a new instance of the DataRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    DataRecoveryCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Creates the {@link StorageFactory} instance by reading the config values.
     *
     * @param executorService   A thread pool for execution.
     * @return                  A newly created {@link StorageFactory} instance.
     */
    StorageFactory createStorageFactory(ScheduledExecutorService executorService) {
        ServiceBuilder.ConfigSetupHelper configSetupHelper = new ServiceBuilder.ConfigSetupHelper(getCommandArgs().getState().getConfigBuilder().build());
        StorageLoader loader = new StorageLoader();
        return loader.load(configSetupHelper, getServiceConfig().getStorageImplementation(),
                getServiceConfig().getStorageLayout(), executorService);
    }

    // Creates the environment for debug segment container
    protected static Context createContext(ScheduledExecutorService scheduledExecutorService) {
        return new Context(scheduledExecutorService);
    }

    protected static class Context implements AutoCloseable {
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
