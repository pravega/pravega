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
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.WriteSettings;
import lombok.Data;

/**
 * Stores configurable throttling settings to be used in {@link ThrottlerCalculator}.
 */
@Data
class ThrottlerPolicy {
    //region Members

    /**
     * Adjustment to the target utilization ratio at or above which throttling will begin to apply. This value helps
     * separate the goals of the {@link ThrottlerCalculator.CacheThrottler} (slow operations if we exceed the target) and
     * the CacheManager (keep the Cache Utilization at or below target, so that we may not need to slow operations if
     * we don't need to).
     *
     * The goal of this adjustment is to give the CacheManager enough breathing room so that it can do its job. It's an
     * async task so it is possible that the cache utilization may slightly exceed the target until it can be reduced to
     * an acceptable level. To avoid unnecessary wobbling in throttling, there is a need for a buffer (adjustment) between
     * this target and when we begin throttling.
     *
     * Example: If CachePolicy.getTargetUtilization()==0.85 and Adjustment==0.05, then throttling will begin to apply
     * at 0.85+0.05=0.90 (90%) of maximum cache capacity.
     */
    @VisibleForTesting
    static final double CACHE_TARGET_UTILIZATION_THRESHOLD_ADJUSTMENT = 0.05;
    /**
     * A multiplier (fraction) that will be applied to {@link WriteSettings#getMaxWriteTimeout()} to determine the
     * DurableDataLog's append latency threshold that will trigger throttling and {@link WriteSettings#getMaxOutstandingBytes()}
     * to determine the minimum amount of outstanding data for which throttling will be performed.
     */
    @VisibleForTesting
    static final double DURABLE_DATALOG_THROTTLE_THRESHOLD_FRACTION = 0.1;

    /**
     * Maximum delay (millis) we are willing to introduce in order to perform batching.
     */
    private final int maxBatchingDelayMillis;
    /**
     * Maximum delay (millis) we are willing to introduce in order to throttle the incoming operations.
     */
    private final int maxDelayMillis;
    /**
     * Maximum size (in number of operations) of the OperationLog, above which maximum throttling will be applied.
     */
    private final int operationLogMaxSize;
    /**
     * Desired size (in number of operations) of the OperationLog, above which a gradual throttling will begin.
     */
    private final int operationLogTargetSize;

    //endregion

    public ThrottlerPolicy(DurableLogConfig config) {
        Preconditions.checkNotNull(config, "config");
        this.maxBatchingDelayMillis = config.getMaxBatchingDelayMillis();
        this.maxDelayMillis = config.getMaxDelayMillis();
        this.operationLogMaxSize = config.getOperationLogMaxSize();
        this.operationLogTargetSize = config.getOperationLogTargetSize();
    }
}
