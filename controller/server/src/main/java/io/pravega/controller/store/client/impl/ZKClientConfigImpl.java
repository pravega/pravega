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

package io.pravega.controller.store.client.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.store.client.ZKClientConfig;
import lombok.Builder;
import lombok.Getter;

/**
 * Zookeeper Curator framework client config.
 */
@Getter
public class ZKClientConfigImpl implements ZKClientConfig {
    private final String connectionString;
    private final String namespace;
    private final int initialSleepInterval;
    private final int maxRetries;

    @Builder
    ZKClientConfigImpl(final String connectionString,
                   final String namespace,
                   final int initialSleepInterval,
                   final int maxRetries) {
        Exceptions.checkNotNullOrEmpty(connectionString, "connectionString");
        Exceptions.checkNotNullOrEmpty(namespace, "namespace");
        Exceptions.checkArgument(initialSleepInterval > 0, "retryInterval", "Should be a positive integer");
        Exceptions.checkArgument(maxRetries > 0, "maxRetries", "Should be a positive integer");

        this.connectionString = connectionString;
        this.namespace = namespace;
        this.initialSleepInterval = initialSleepInterval;
        this.maxRetries = maxRetries;
    }
}
