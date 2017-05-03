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
package io.pravega.service.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.service.storage.DurableDataLog;
import io.pravega.service.storage.DurableDataLogTestBase;
import io.pravega.service.storage.LogAddress;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for InMemoryDurableDataLog.
 */
public class InMemoryDurableDataLogTests extends DurableDataLogTestBase {
    private static final int WRITE_COUNT = 250;
    private final Supplier<Integer> nextContainerId = new AtomicInteger()::incrementAndGet;
    private InMemoryDurableDataLogFactory factory;

    @Before
    public void setUp() {
        this.factory = new InMemoryDurableDataLogFactory(executorService());
    }

    @After
    public void tearDown() {
        if (this.factory != null) {
            this.factory.close();
            this.factory = null;
        }
    }

    @Override
    protected DurableDataLog createDurableDataLog() {
        return this.factory.createDurableDataLog(this.nextContainerId.get());
    }

    @Override
    protected DurableDataLog createDurableDataLog(Object sharedContext) {
        Preconditions.checkArgument(sharedContext instanceof InMemoryDurableDataLog.EntryCollection);
        return new InMemoryDurableDataLog((InMemoryDurableDataLog.EntryCollection) sharedContext, executorService());
    }

    @Override
    protected Object createSharedContext() {
        return new InMemoryDurableDataLog.EntryCollection();
    }

    @Override
    protected LogAddress createLogAddress(long seqNo) {
        return new InMemoryDurableDataLog.InMemoryLogAddress(seqNo);
    }

    @Override
    protected int getWriteCount() {
        return WRITE_COUNT;
    }

    /**
     * Tests the constructor of InMemoryDurableDataLog. The constructor takes in an EntryCollection and this verifies
     * that information from a previous instance of an InMemoryDurableDataLog is still accessible.
     */
    @Test(timeout = 5000)
    public void testConstructor() throws Exception {
        InMemoryDurableDataLog.EntryCollection entries = new InMemoryDurableDataLog.EntryCollection();
        TreeMap<LogAddress, byte[]> writeData;

        // Create first log and write some data to it.
        try (DurableDataLog log = new InMemoryDurableDataLog(entries, executorService())) {
            log.initialize(TIMEOUT);
            writeData = populate(log, WRITE_COUNT);
        }

        // Close the first log, and open a second one, with the same EntryCollection in the constructor.
        try (DurableDataLog log = new InMemoryDurableDataLog(entries, executorService())) {
            log.initialize(TIMEOUT);

            // Verify it contains the same entries.
            verifyReads(log, writeData);
        }
    }
}
