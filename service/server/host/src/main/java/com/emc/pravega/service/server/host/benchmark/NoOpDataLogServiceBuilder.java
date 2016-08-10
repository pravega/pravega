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

package com.emc.pravega.service.server.host.benchmark;

import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.server.mocks.InMemoryServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.emc.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ServiceBuilder that has a DurableDataLog that does absolutely nothing.
 * Good for testing Append overhead inside StreamSegmentStore without any sort of outside interference.
 */
public class NoOpDataLogServiceBuilder extends InMemoryServiceBuilder {
    //region Constructor

    public NoOpDataLogServiceBuilder(ServiceBuilderConfig config) {
        super(config);
    }

    //endregion

    //region ServiceBuilder Implementation

    @Override
    protected DurableDataLogFactory createDataLogFactory() {
        return new InMemoryDurableDataLogFactory();
    }

    //endregion

    //region NoOpDurableDataLogFactory

    private static class NoOpDurableDataLogFactory implements DurableDataLogFactory {

        @Override
        public DurableDataLog createDurableDataLog(int containerId) {
            return new NoOpDataLogServiceBuilder.NoOpDurableDataLog();
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }

    //endregion

    //region NoOpDurableDataLog

    private static class NoOpDurableDataLog implements DurableDataLog {
        private final AtomicLong currentOffset = new AtomicLong();

        @Override
        public void initialize(Duration timeout) throws DurableDataLogException {
            // Nothing to do.
        }

        @Override
        public CompletableFuture<Long> append(InputStream data, Duration timeout) {
            return CompletableFuture.completedFuture(this.currentOffset.incrementAndGet());
        }

        @Override
        public CompletableFuture<Boolean> truncate(long upToSequence, Duration timeout) {
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
            return new NoOpIterator();
        }

        @Override
        public int getMaxAppendLength() {
            return 0;
        }

        @Override
        public long getLastAppendSequence() {
            return this.currentOffset.get();
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }

    //endregion

    //region NoOpIterator

    private static class NoOpIterator implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {

        @Override
        public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
            return null;
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }

    //endregion
}