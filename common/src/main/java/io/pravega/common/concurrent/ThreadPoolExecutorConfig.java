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
package io.pravega.common.concurrent;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.TimeUnit;

/**
 * Stores configurations for @{@link java.util.concurrent.ThreadPoolExecutor}.
 */
@Data
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
public class ThreadPoolExecutorConfig {

    /**
     * The minimum number of thread in the pool.
     */
    private final int corePoolSize;

    /**
     * The maximum number of thread in the pool.
     */
    private final int maxPoolSize;

    /**
     * This is the time excess idle threads will wait before terminating.
     */
    @Builder.Default
    private int keepAliveTime = 100;

    /**
     * The unit of time.
     */
    @Builder.Default
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    /**
     * Whether idle core should live after keepAliveTime.
     */
    @Getter(AccessLevel.NONE)
    private boolean allowCoreThreadTimeOut;

    public boolean allowsCoreThreadTimeOut() {
        return this.allowCoreThreadTimeOut;
    }
}
