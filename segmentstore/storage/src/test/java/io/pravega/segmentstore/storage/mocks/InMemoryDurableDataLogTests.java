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
package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogTestBase;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for InMemoryDurableDataLog.
 */
public class InMemoryDurableDataLogTests extends DurableDataLogTestBase {
    private static final int WRITE_COUNT = 1000;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
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

    @Test
    public void testWriteSettings() {
        @Cleanup
        val log = createDurableDataLog();
        val ws = log.getWriteSettings();
        Assert.assertEquals(1024 * 1024 - 8 * 1024, ws.getMaxWriteLength());
        Assert.assertEquals(Integer.MAX_VALUE, ws.getMaxOutstandingBytes());
    }

    @Override
    @Test
    public void testRegisterQueueStateListener() {
        @Cleanup
        val log = createDurableDataLog();

        // Following should have no effect.
        log.registerQueueStateChangeListener(new ThrottleSourceListener() {
            @Override
            public void notifyThrottleSourceChanged() {
            }

            @Override
            public boolean isClosed() {
                return false;
            }
        });
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

    @Test(timeout = 5000)
    public void testDebugRelatedFactoryMethods() {
        Assert.assertEquals(this.factory.getRepairLogId(), Integer.MAX_VALUE);
        Assert.assertEquals(this.factory.getBackupLogId(), Integer.MAX_VALUE - 1);
        AssertExtensions.assertThrows(UnsupportedOperationException.class, () -> this.factory.createDebugLogWrapper(0));
    }
}
