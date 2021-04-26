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
package io.pravega.controller.store.client.impl;

import com.google.common.base.Strings;
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
    private final int sessionTimeoutMs;
    private final boolean secureConnectionToZooKeeper;
    private final String trustStorePath;
    private final String trustStorePasswordPath;

    @Builder
    ZKClientConfigImpl(final String connectionString,
                       final String namespace,
                       final int initialSleepInterval,
                       final int maxRetries,
                       final int sessionTimeoutMs,
                       final boolean secureConnectionToZooKeeper,
                       final String trustStorePath,
                       final String trustStorePasswordPath) {
        Exceptions.checkNotNullOrEmpty(connectionString, "connectionString");
        Exceptions.checkNotNullOrEmpty(namespace, "namespace");
        Exceptions.checkArgument(initialSleepInterval > 0, "retryInterval", "Should be a positive integer");
        Exceptions.checkArgument(maxRetries > 0, "maxRetries", "Should be a positive integer");
        Exceptions.checkArgument(sessionTimeoutMs > 0, "sessionTimeoutMs", "Should be a positive integer");

        this.connectionString = connectionString;
        this.namespace = namespace;
        this.initialSleepInterval = initialSleepInterval;
        this.maxRetries = maxRetries;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.secureConnectionToZooKeeper = secureConnectionToZooKeeper;
        this.trustStorePath = trustStorePath;
        this.trustStorePasswordPath = trustStorePasswordPath;
    }

    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return new StringBuilder(String.format("%s(", getClass().getSimpleName()))
                .append(String.format("connectionString: %s, ", connectionString))
                .append(String.format("namespace: %s, ", namespace))
                .append(String.format("initialSleepInterval: %d, ", initialSleepInterval))
                .append(String.format("maxRetries: %d, ", maxRetries))
                .append(String.format("sessionTimeoutMs: %d, ", sessionTimeoutMs))
                .append(String.format("secureConnectionToZooKeeper: %b, ", secureConnectionToZooKeeper))
                .append(String.format("trustStorePath is %s, ",
                        Strings.isNullOrEmpty(trustStorePath) ? "unspecified" : "specified"))
                .append(String.format("trustStorePasswordPath is %s",
                        Strings.isNullOrEmpty(trustStorePasswordPath) ? "unspecified" : "specified"))
                .append(")")
                .toString();
    }
}
