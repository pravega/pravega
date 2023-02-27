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
package io.pravega.segmentstore.server.containers;

import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.DebugSegmentContainer;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class DebugStreamSegmentContainer extends StreamSegmentContainer implements DebugSegmentContainer {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private final ContainerConfig config;

    /**
     * Creates a new instance of the DebugStreamSegmentContainer class.
     *
     * @param debugSegmentContainerId  The Id of the debugSegmentContainer.
     * @param config                   The ContainerConfig to use for this DebugStreamSegmentContainer.
     * @param durableLogFactory        The DurableLogFactory to use to create DurableLogs.
     * @param readIndexFactory         The ReadIndexFactory to use to create Read Indices.
     * @param attributeIndexFactory    The AttributeIndexFactory to use to create Attribute Indices.
     * @param writerFactory            The WriterFactory to use to create Writers.
     * @param storageFactory           The StorageFactory to use to create Storage Adapters.
     * @param createExtensions         A Function that, given an instance of this class, will create the set of
     *                                 {@link SegmentContainerExtension}s to be associated with that instance.
     * @param executor                 An Executor that can be used to run async tasks.
     */
    public DebugStreamSegmentContainer(int debugSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory,
                                       ReadIndexFactory readIndexFactory, AttributeIndexFactory attributeIndexFactory,
                                       WriterFactory writerFactory, StorageFactory storageFactory,
                                       SegmentContainerFactory.CreateExtensions createExtensions, ScheduledExecutorService executor) {
        super(debugSegmentContainerId, config, durableLogFactory, readIndexFactory, attributeIndexFactory, writerFactory,
                storageFactory, createExtensions, executor);
        this.config = config;
    }

    @Override
    public CompletableFuture<Void> registerSegment(String streamSegmentName, long length, boolean isSealed) {
        ArrayView segmentInfo = MetadataStore.SegmentInfo.recoveredSegment(streamSegmentName, length, isSealed);
        return metadataStore.createSegment(streamSegmentName, segmentInfo, new TimeoutTimer(TIMEOUT));
    }

    public final UpdateableContainerMetadata getMetadata() {
        return super.metadata;
    }

    public void queueMapOperation(SegmentProperties properties, long segmentId) {
        StreamSegmentMapOperation operation = new StreamSegmentMapOperation(properties);
        operation.setStreamSegmentId(segmentId);
        OperationPriority priority = this.calculatePriority(SegmentType.fromAttributes(properties.getAttributes()), operation);
        this.getDurableLog().add(operation, priority, TIMEOUT).join();
    }

    public boolean isSegmentExists(String segmentName) throws Exception {
        SegmentProperties containerSegment = null;
        try {
            containerSegment = this.getStreamSegmentInfo(segmentName, TIMEOUT).get();
            log.info("[DebugSegmentContainer{}] Abh segment retrieved {}"+this.getId(), containerSegment.getName());
        } catch (Exception e) {
            Throwable unwrapped = Exceptions.unwrap(e);
            if (unwrapped instanceof StreamSegmentNotExistsException) {
                log.info("[DebugSegmentContainer{}] Abh error retrieving segment {}"+this.getId(), segmentName);
                return false;
            }
            throw e;
        }
        if (containerSegment == null)
            return false;
        return (containerSegment.getName() != null && containerSegment.getName().equalsIgnoreCase(segmentName));
    }

    public Storage getStorage() {
        return super.getStorage();
    }

    @Override
    public CompletableFuture<Void> startSecondaryServicesAsync() {
        return super.startSecondaryServicesAsync();
    }


}
