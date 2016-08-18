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

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorageFactory implements StorageFactory {
    private final InMemoryStorage storage;
    private boolean closed;

    public InMemoryStorageFactory() {
        this(ForkJoinPool.commonPool());
    }

    public InMemoryStorageFactory(Executor executor) {
        this.storage = new InMemoryStorage(executor);
    }

    @Override
    public Storage getStorageAdapter() {
        Exceptions.checkNotClosed(this.closed, this);
        return storage;
    }

    @Override
    public void close() {
        this.closed = true;
    }
}
