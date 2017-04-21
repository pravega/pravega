/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.service.storage.DataLogWriterNotPrimaryException;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogTestBase;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.testcommon.AssertExtensions;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
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
    protected int getWriteCountForWrites() {
        return WRITE_COUNT;
    }

    @Override
    protected int getWriteCountForReads() {
        return getWriteCountForWrites(); // In-Memory is fast enough; we can do this many.
    }

    /**
     * Tests the constructor of InMemoryDurableDataLog. The constructor takes in an EntryCollection and this verifies
     * that information from a previous instance of an InMemoryDurableDataLog is still accessible.
     */
    @Test
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

    @Test
    @Override
    public void testExclusiveWriteLock() throws Exception {
        InMemoryDurableDataLog.EntryCollection entries = new InMemoryDurableDataLog.EntryCollection();

        final long initialEpoch;
        final long secondEpoch;
        try (DurableDataLog log = new InMemoryDurableDataLog(entries, executorService())) {
            log.initialize(TIMEOUT);
            initialEpoch = log.getEpoch();
            AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on empty log initialization.", 0, initialEpoch);

            // 1. No two logs can use the same EntryCollection.
            try (DurableDataLog log2 = new InMemoryDurableDataLog(entries, executorService())) {
                AssertExtensions.assertThrows(
                        "A second log was able to acquire the exclusive write lock, even if another log held it.",
                        () -> log2.initialize(TIMEOUT),
                        ex -> ex instanceof DataLogWriterNotPrimaryException);

                AssertExtensions.assertThrows(
                        "getEpoch() did not throw after failed initialization.",
                        log2::getEpoch,
                        ex -> ex instanceof IllegalStateException);
            }

            // Verify we can still append to the first log.
            TreeMap<LogAddress, byte[]> writeData = populate(log, getWriteCountForWrites());

            // 2. If during the normal operation of a log, it loses its lock, it should no longer be able to append...
            secondEpoch = entries.forceAcquireLock("ForceLock");
            AssertExtensions.assertGreaterThan("forceAcquireLock did not increase epoch.", initialEpoch, secondEpoch);
            AssertExtensions.assertThrows(
                    "A second log acquired the exclusive write lock, but the first log could still append to it.",
                    () -> log.append(new ByteArrayInputStream("h".getBytes()), TIMEOUT).join(),
                    ex -> ex instanceof DataLogWriterNotPrimaryException);

            // ... or to truncate ...
            AssertExtensions.assertThrows(
                    "A second log acquired the exclusive write lock, but the first log could still truncate it.",
                    () -> log.truncate(writeData.lastKey(), TIMEOUT).join(),
                    ex -> ex instanceof DataLogWriterNotPrimaryException);

            // ... but it should still be able to read.
            verifyReads(log, writeData);
        }

        // Verify epoch is incremented with every call to initialize().
        entries.releaseLock("ForceLock");
        try (DurableDataLog log = new InMemoryDurableDataLog(entries, executorService())) {
            log.initialize(TIMEOUT);
            long epoch = log.getEpoch();
            AssertExtensions.assertGreaterThan("Unexpected value from getEpoch() on non-empty log initialization.", secondEpoch, epoch);
        }
    }
}
