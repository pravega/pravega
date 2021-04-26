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

import java.util.concurrent.ScheduledExecutorService;

/**
 * Storage binding implementations are supposed to inherit this interface.
 * The bindings are loaded using `ServiceLoader` (https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html)
 */
public interface StorageFactoryCreator {
    /**
     * API to create a storage factory with given configuration.
     *
     * @param storageFactoryInfo Properties of storage factory to create.
     * @param setup              Configuration for the factory.
     * @param executor           The storage factory is expected to use this ExecutorService for execution of its tasks.
     */
    StorageFactory createFactory(StorageFactoryInfo storageFactoryInfo, ConfigSetup setup, ScheduledExecutorService executor);

    /**
     * The properties of the available storage factories.
     *
     * @return The array of StorageFactoryInfo.
     */
    StorageFactoryInfo[] getStorageFactories();
}
