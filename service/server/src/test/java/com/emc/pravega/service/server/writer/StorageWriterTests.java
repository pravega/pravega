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

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.server.CloseableExecutorService;
import com.emc.pravega.service.server.ConfigHelpers;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.logs.OperationLog;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.mocks.InMemoryCache;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import lombok.Cleanup;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;

/**
 * Unit tests for the StorageWriter class.
 */
public class StorageWriterTests {
    private static final int CONTAINER_ID = 1;
    private static final int THREAD_POOL_SIZE = 50;
    private static final WriterConfig DEFAULT_CONFIG = ConfigHelpers.createWriterConfig(1000, 1000);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Test
    public void testMisc() throws Exception {
        final int operationCount = 100;
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Start the writer.
        context.writer.startAsync();
        Thread.sleep(500);
        for (int i = 0; i < operationCount; i++) {
            Operation o = new StreamSegmentAppendOperation(1, Integer.toString(i).getBytes(), new AppendContext(UUID.randomUUID(), i));
            context.operationLog.add(o, TIMEOUT).join();
        }

        Thread.sleep(1000);
    }

    private static class TestContext implements AutoCloseable {
        final CloseableExecutorService executor;
        final UpdateableContainerMetadata metadata;
        final OperationLog operationLog;
        final Storage storage;
        final Cache cache;
        final StorageWriter writer;

        TestContext(WriterConfig config) {
            this.executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
            this.metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            this.storage = new InMemoryStorage(this.executor.get());
            this.cache = new InMemoryCache(Integer.toString(CONTAINER_ID));
            this.operationLog = new LightWeightDurableLog(this.metadata, this.executor.get());
            this.writer = new StorageWriter(config, this.metadata, this.operationLog, this.storage, this.cache, this.executor.get());
        }

        @Override
        public void close() {
            this.writer.close();
            this.operationLog.close();
            this.cache.close();
            this.storage.close();
            this.executor.close();
        }
    }
}
