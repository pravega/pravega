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

package io.pravega.service.storage.impl.hdfs;

import io.pravega.service.storage.Storage;
import io.pravega.service.storage.StorageFactory;
import com.google.common.base.Preconditions;
import java.util.concurrent.Executor;

/**
 * Factory for HDFS Storage adapters.
 */
public class HDFSStorageFactory implements StorageFactory {
    private final HDFSStorageConfig config;
    private final Executor executor;

    /**
     * Creates a new instance of the HDFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public HDFSStorageFactory(HDFSStorageConfig config, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Storage createStorageAdapter() {
        return new HDFSStorage(this.config, this.executor);
    }
}
