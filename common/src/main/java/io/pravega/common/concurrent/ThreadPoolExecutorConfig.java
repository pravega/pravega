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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.concurrent.TimeUnit;

/**
 * Stores configurations for @{@link java.util.concurrent.ThreadPoolExecutor}.
 */
@Getter
@Setter
@RequiredArgsConstructor
@AllArgsConstructor
public class ThreadPoolExecutorConfig {

    private final int corePoolSize;

    private final int maxPoolSize;

    private int keepAliveTime = 100;

    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
}