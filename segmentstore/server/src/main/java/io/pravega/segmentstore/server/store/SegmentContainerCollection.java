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
package io.pravega.segmentstore.server.store;

import com.google.common.base.Preconditions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Base class for any wrapper that deals with multiple Segment Containers.
 */
@Slf4j
public abstract class SegmentContainerCollection {
    //region Members

    private final SegmentContainerRegistry segmentContainerRegistry;
    private final SegmentToContainerMapper segmentToContainerMapper;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentContainerCollection class.
     *
     * @param segmentContainerRegistry The SegmentContainerRegistry to route requests to.
     * @param segmentToContainerMapper The SegmentToContainerMapper to use to map StreamSegments to Containers.
     */
    public SegmentContainerCollection(SegmentContainerRegistry segmentContainerRegistry, SegmentToContainerMapper segmentToContainerMapper) {
        this.segmentContainerRegistry = Preconditions.checkNotNull(segmentContainerRegistry, "segmentContainerRegistry");
        this.segmentToContainerMapper = Preconditions.checkNotNull(segmentToContainerMapper, "segmentToContainerMapper");
    }

    //endregion

    /**
     * Executes the given Function on the SegmentContainer that the given Segment maps to.
     *
     * @param streamSegmentName The name of the StreamSegment to fetch the Container for.
     * @param toInvoke          A Function that will be invoked on the Container.
     * @param methodName        The name of the calling method (for logging purposes).
     * @param logArgs           (Optional) A vararg array of items to be logged.
     * @param <T>               Resulting type.
     * @return Either the result of toInvoke or a CompletableFuture completed exceptionally with a ContainerNotFoundException
     * in case the SegmentContainer that the Segment maps to does not exist in this StreamSegmentService.
     */
    protected <T> CompletableFuture<T> invoke(String streamSegmentName, Function<SegmentContainer, CompletableFuture<T>> toInvoke,
                                              String methodName, Object... logArgs) {
        int containerId = this.segmentToContainerMapper.getContainerId(streamSegmentName);
        return invoke(containerId, toInvoke, methodName, logArgs);
    }

    /**
     * Executes the given Function on the StreamSegmentContainer that the given Id maps to.
     *
     * @param containerId The Id to fetch the Container for.
     * @param toInvoke    A Function that will be invoked on the Container.
     * @param methodName  The name of the calling method (for logging purposes).
     * @param logArgs     (Optional) A vararg array of items to be logged.
     * @param <T>         Resulting type.
     * @return Either the result of toInvoke or a CompletableFuture completed exceptionally with a ContainerNotFoundException
     * in case the SegmentContainer that the Id maps to does not exist in this StreamSegmentService.
     */
    protected <T> CompletableFuture<T> invoke(int containerId, Function<SegmentContainer, CompletableFuture<T>> toInvoke,
                                              String methodName, Object... logArgs) {
        long traceId = LoggerHelpers.traceEnter(log, methodName, logArgs);
        SegmentContainer container;
        try {
            container = this.segmentContainerRegistry.getContainer(containerId);
        } catch (ContainerNotFoundException ex) {
            return Futures.failedFuture(ex);
        }

        CompletableFuture<T> resultFuture = toInvoke.apply(container);
        if (log.isTraceEnabled()) {
            resultFuture.thenAccept(r -> LoggerHelpers.traceLeave(log, methodName, traceId, r));
        }

        return resultFuture;
    }
}
