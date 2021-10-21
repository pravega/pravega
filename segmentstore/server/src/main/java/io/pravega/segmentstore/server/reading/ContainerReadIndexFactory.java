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
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation for ReadIndexFactory.
 */
public class ContainerReadIndexFactory implements ReadIndexFactory {
    private final ScheduledExecutorService executorService;
    private final ReadIndexConfig config;
    private final CacheManager cacheManager;
    private final AtomicBoolean closed;

    /**
     * Creates a new instance of the ContainerReadIndexFactory class.
     *
     * @param config          Configuration for the ReadIndex.
     * @param cacheManager    The CacheManager to use to manage Cache entries.
     * @param executorService The Executor to use to invoke async callbacks.
     */
    public ContainerReadIndexFactory(ReadIndexConfig config, CacheManager cacheManager, ScheduledExecutorService executorService) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
        this.cacheManager = Preconditions.checkNotNull(cacheManager, "cacheManager");
        this.closed = new AtomicBoolean();
    }

    @Override
    public ReadIndex createReadIndex(ContainerMetadata containerMetadata, ReadOnlyStorage storage) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return new ContainerReadIndex(this.config, containerMetadata, storage, this.cacheManager, this.executorService);
    }

    @Override
    public void close() {
        this.closed.set(true);
    }
}
