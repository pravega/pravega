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

package com.emc.pravega.service.server.writer;

import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.WriterFactory;
import com.emc.pravega.service.server.logs.OperationLog;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.Executor;

/**
 * Factory for StorageWriters.
 */
public class StorageWriterFactory implements WriterFactory {
    private final WriterConfig config;
    private final StorageFactory storageFactory;
    private final Executor executor;

    /**
     * Creates a new instance of the StorageWriterFactory class.
     *
     * @param config         The Configuration to use for every Writer that is created.
     * @param storageFactory The StorageFactory to use for every Writer creation.
     * @param executor       The Executor to use.
     */
    public StorageWriterFactory(WriterConfig config, StorageFactory storageFactory, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.storageFactory = storageFactory;
        this.executor = executor;
    }

    @Override
    public Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, Cache cache) {
        Preconditions.checkArgument(containerMetadata.getContainerId() == operationLog.getId(), "Given containerMetadata and operationLog have different Container Ids.");
        Storage storage = this.storageFactory.getStorageAdapter();
        return new StorageWriter(this.config, containerMetadata, operationLog, storage, cache, this.executor);
    }
}
