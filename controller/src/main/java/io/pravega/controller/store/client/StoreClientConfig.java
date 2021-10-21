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
package io.pravega.controller.store.client;

import java.util.Optional;

/**
 * Configuration of the metadata store client.
 */
public interface StoreClientConfig {
    /**
     * Fetches the type of the store the client connects to.
     * @return The type of the store the client connects to.
     */
    StoreType getStoreType();

    /**
     * Fetches whether the ZK base store is enabled, and its configuration if it is enabled.
     * @return Whether the ZK base store is enabled, and its configuration if it is enabled.
     */
    Optional<ZKClientConfig> getZkClientConfig();
}
