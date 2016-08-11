/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.StorageFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.Executor;

/**
 * Default implementation for ReadIndexFactory.
 */
public class ContainerReadIndexFactory implements ReadIndexFactory {
    private final Executor executor;
    private final StorageFactory storageFactory;
    private final ReadIndexConfig config;

    /**
     * Creates a new instance of the ContainerReadIndexFactory class.
     *
     * @param config         Configuration for the ReadIndex.
     * @param storageFactory The StorageFactory to use to get a reference to the Storage adapter.
     * @param executor       The Executor to use to invoke async callbacks.
     */
    public ContainerReadIndexFactory(ReadIndexConfig config, StorageFactory storageFactory, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.config = config;
        this.storageFactory = storageFactory;
        this.executor = executor;
    }

    @Override
    public ReadIndex createReadIndex(ContainerMetadata containerMetadata, Cache cache) {
        return new ContainerReadIndex(this.config, containerMetadata, cache, this.storageFactory.getStorageAdapter(), this.executor);
    }
}
