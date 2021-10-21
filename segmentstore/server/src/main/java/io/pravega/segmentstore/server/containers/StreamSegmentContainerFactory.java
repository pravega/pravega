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
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.DebugSegmentContainer;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Represents a SegmentContainerFactory that builds instances of the StreamSegmentContainer class.
 */
public class StreamSegmentContainerFactory implements SegmentContainerFactory {
    private final ContainerConfig config;
    private final OperationLogFactory operationLogFactory;
    private final ReadIndexFactory readIndexFactory;
    private final AttributeIndexFactory attributeIndexFactory;
    private final WriterFactory writerFactory;
    private final StorageFactory storageFactory;
    private final CreateExtensions createExtensions;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the StreamSegmentContainerFactory.
     *
     * @param config                The ContainerConfig to use for this StreamSegmentContainer.
     * @param operationLogFactory   The OperationLogFactory to use for every container creation.
     * @param readIndexFactory      The ReadIndexFactory to use for every container creation.
     * @param attributeIndexFactory The AttributeIndexFactory to use for every container creation.
     * @param writerFactory         The Writer Factory to use for every container creation.
     * @param storageFactory        The Storage Factory to use for every container creation.
     * @param createExtensions         A Function that, when given an instance of a SegmentContainer, will create the required
     *                              {@link SegmentContainerExtension}s for it.
     * @param executor              The Executor to use for running async tasks.
     * @throws NullPointerException If any of the arguments are null.
     */
    public StreamSegmentContainerFactory(ContainerConfig config, OperationLogFactory operationLogFactory, ReadIndexFactory readIndexFactory,
                                         AttributeIndexFactory attributeIndexFactory, WriterFactory writerFactory,
                                         StorageFactory storageFactory, CreateExtensions createExtensions, ScheduledExecutorService executor) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.operationLogFactory = Preconditions.checkNotNull(operationLogFactory, "operationLogFactory");
        this.readIndexFactory = Preconditions.checkNotNull(readIndexFactory, "readIndexFactory");
        this.attributeIndexFactory = Preconditions.checkNotNull(attributeIndexFactory, "attributeIndexFactory");
        this.writerFactory = Preconditions.checkNotNull(writerFactory, "writerFactory");
        this.storageFactory = Preconditions.checkNotNull(storageFactory, "storageFactory");
        this.createExtensions = Preconditions.checkNotNull(createExtensions, "createExtensions");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    @Override
    public DebugSegmentContainer createDebugStreamSegmentContainer(int containerId) {
        return new DebugStreamSegmentContainer(containerId, config, this.operationLogFactory, this.readIndexFactory,
                this.attributeIndexFactory, this.writerFactory, this.storageFactory, this.createExtensions, this.executor);
    }

    @Override
    public SegmentContainer createStreamSegmentContainer(int containerId) {
        return new StreamSegmentContainer(containerId, config, this.operationLogFactory, this.readIndexFactory,
                this.attributeIndexFactory, this.writerFactory, this.storageFactory, this.createExtensions, this.executor);
    }
}
