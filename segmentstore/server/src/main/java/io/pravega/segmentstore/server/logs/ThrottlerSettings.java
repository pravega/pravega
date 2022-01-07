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

import com.google.common.base.Preconditions;
import lombok.Data;

/**
 * Stores configurable settings for {@link ThrottlerCalculator}.
 */
@Data
class ThrottlerSettings {

    private final int maxBatchingDelayMillis;
    private final int maxDelayMillis;
    private final int operationLogMaxSize;
    private final int operationLogTargetSize;

    public ThrottlerSettings(DurableLogConfig config) {
        Preconditions.checkNotNull(config, "config");
        this.maxBatchingDelayMillis = config.getMaxBatchingDelayMillis();
        this.maxDelayMillis = config.getMaxDelayMillis();
        this.operationLogMaxSize = config.getOperationLogMaxSize();
        this.operationLogTargetSize = config.getOperationLogTargetSize();
    }
}
