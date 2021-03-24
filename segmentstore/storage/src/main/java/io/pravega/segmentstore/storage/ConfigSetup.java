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
package io.pravega.segmentstore.storage;

import io.pravega.common.util.ConfigBuilder;
import java.util.function.Supplier;

/**
 * Interface used to pass the configuration to storage plugin implementation.
 */
public interface ConfigSetup {
    /**
     * Gets the Configuration with specified constructor from the ServiceBuilder's config.
     *
     * @param builderConstructor A Supplier that creates a ConfigBuilder for the desired configuration type.
     * @param <T>                The type of the Configuration to instantiate.
     */
    <T> T getConfig(Supplier<? extends ConfigBuilder<T>> builderConstructor);
}
