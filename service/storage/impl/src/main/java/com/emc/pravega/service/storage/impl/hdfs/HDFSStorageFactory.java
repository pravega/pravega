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

package com.emc.pravega.service.storage.impl.hdfs;


import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Factory for HDFS Storage components.
 */
public class HDFSStorageFactory implements StorageFactory {
    private final HDFSStorage storage;
    private final AtomicBoolean closed;

    /**
     * Creates a new instance of the HDFSStorageFactory class.
     *
     * @param serviceBuilderConfig The configuration to use.
     * @param executor             The executor to use for async operations.
     */
    public HDFSStorageFactory(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        this.storage = new HDFSStorage(serviceBuilderConfig, executor);
        this.closed = new AtomicBoolean();
    }

    /**
     * Creates a new instance of the HDFSStorageFactory class, using the default
     * ForkJoinPool executor for async operations.
     *
     * @param serviceBuilderConfig The configuration to use.
     */
    public HDFSStorageFactory(HDFSStorageConfig serviceBuilderConfig) {
        this(serviceBuilderConfig, ForkJoinPool.commonPool());

    }

    @Override
    public Storage getStorageAdapter() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return storage;
    }

    @Override
    public void close() {
        this.closed.set(true);
    }

    /**
     * Initializes the HDFSStorageFactory by attempting to establish a connection to the remote HDFS server.
     *
     * @throws IOException If the initialization failed.
     */
    public void initialize() throws IOException {
        this.storage.initialize();

    }
}
