/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.timeout;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

/**
 * Timeout service config.
 */
@Getter
public class TimeoutServiceConfig {
    private final long maxLeaseValue;
    private final long maxScaleGracePeriod;

    @Builder
    TimeoutServiceConfig(final long maxLeaseValue, final long maxScaleGracePeriod) {
        Preconditions.checkArgument(maxLeaseValue > 0, "maxLeaseValue should be positive integer");
        Preconditions.checkArgument(maxScaleGracePeriod > 0, "maxScaleGracePeriod should be positive integer");

        this.maxLeaseValue = maxLeaseValue;
        this.maxScaleGracePeriod = maxScaleGracePeriod;
    }

    public static TimeoutServiceConfig defaultConfig() {
        return new TimeoutServiceConfig(30000, 30000);
    }
}
