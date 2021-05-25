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
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.util.SimpleCache;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.function.BiConsumer;

/**
 * Class that associates flows with {@link AppendBatchSizeTracker} instances. When connection pooling is disabled, this
 * class is expected to contain only one flow associated to one {@link AppendBatchSizeTracker}. On the other hand, when
 * connection pooling is enabled, this class is expected to contain multiple of such associations. The internal flow
 * to {@link AppendBatchSizeTracker} map is implemented via a {@link SimpleCache} object, which in addition to thread-safe
 * operations, it also provides an eviction mechanism. This will keep low the memory requirements of this class along time.
 * Note that if a flow entry is evicted and then the flow is used again, the only consequence will be that its previous
 * batching information would be lost and the flow will start operating with the batching algorithm defaults.
 */
@Slf4j
class FlowToBatchSizeTracker {

    /**
     * Maximum time that the want to keep an inactive flow entry in the cache.
     */
    private static final Duration TRACKER_CACHE_EXPIRATION_TIME = Duration.ofSeconds(TcpClientConnection.CONNECTION_TIMEOUT);
    /**
     * Maximum number of active flow entries in the cache.
     */
    private static final int TRACKER_CACHE_MAX_SIZE = 100000;

    @VisibleForTesting
    @Getter(value = AccessLevel.PACKAGE)
    private final SimpleCache<Integer, AppendBatchSizeTracker> flowToBatchSizeTrackerMap;

    FlowToBatchSizeTracker() {
        this.flowToBatchSizeTrackerMap = new SimpleCache<>(TRACKER_CACHE_MAX_SIZE, TRACKER_CACHE_EXPIRATION_TIME,
                (flow, tracker) -> log.info("Evicting batch tracker for flow: {}", flow));
    }

    @VisibleForTesting
    FlowToBatchSizeTracker(int flowCacheMaxSize, Duration flowCacheExpiration, BiConsumer<Integer, AppendBatchSizeTracker> callback) {
        this.flowToBatchSizeTrackerMap = new SimpleCache<>(flowCacheMaxSize, flowCacheExpiration, callback);
    }

    /**
     * This method returns the {@link AppendBatchSizeTracker} instance for a given flow id. If the flow id does not exist
     * yet in the cache, the new entry for the flow is created along with a new {@link AppendBatchSizeTracker} object.
     *
     * @param flowId    Identifier for the flow.
     * @return The {@link AppendBatchSizeTracker} instance associated to the flow id.
     */
    AppendBatchSizeTracker getAppendBatchSizeTrackerByFlowId(int flowId) {
        if (flowToBatchSizeTrackerMap.putIfAbsent(flowId, new AppendBatchSizeTrackerImpl()) == null) {
            log.debug("Instantiating new batch sze tracker for flow: {}", flowId);
        }
        return flowToBatchSizeTrackerMap.get(flowId);
    }
}
