/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.TestStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import java.io.ByteArrayInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.Getter;
import lombok.val;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link StorageMetadataStore} class.
 */
public class StorageMetadataStoreTests extends MetadataStoreTestBase {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    /**
     * Tests the ability of the MetadataStore to create a new Segment if there are Storage and/or OperationLog Failures.
     * This tests failures other than StreamSegmentExistsException, which is handled in a different test.
     */
    @Test
    public void testCreateSegmentWithFailures() {
        final String segmentName = "NewSegment";

        @Cleanup
        StorageTestContext context = (StorageTestContext) createTestContext();
        context.storage.setCreateErrorInjector(new ErrorInjector<>(i -> true, IntentionalException::new));
        AssertExtensions.assertSuppliedFutureThrows(
                "createSegment did not fail when random exception was thrown.",
                () -> context.getMetadataStore().createSegment(segmentName, null, TIMEOUT),
                ex -> ex instanceof IntentionalException);
    }

    @Test
    public void testCreateSegmentPartiallyExists() {
        final String segmentName = "NewSegment";

        @Cleanup
        StorageTestContext context = (StorageTestContext) createTestContext();
        String stateFile = StreamSegmentNameUtils.getStateSegmentName(segmentName);

        // Create the State file but leave it empty. It should be treated as inexistent.
        context.storage.create(stateFile, TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "getSegmentInfo did not fail when state file exists partially.",
                () -> context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Create it properly. This should work.
        context.getMetadataStore().createSegment(segmentName, null, TIMEOUT).join();
        val si = context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT).join();
        val expectedSi = StreamSegmentInformation.builder().name(segmentName).build();
        assertEquals("Unexpected result from getSegmentInfo() partial creation.", expectedSi, si);

        // Now, corrupt the file.
        context.getMetadataStore().clearSegmentInfo(segmentName, TIMEOUT)
               .thenCompose(v -> context.storage.create(stateFile, TIMEOUT))
               .thenCompose(v -> context.storage.openWrite(stateFile))
               .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[]{1}), 1, TIMEOUT))
               .join();

        // Create should still throw StreamSegmentExistException because the state file has a non-zero length.
        AssertExtensions.assertSuppliedFutureThrows(
                "createSegment() did not throw for existing (and corrupted) state file.",
                () -> context.getMetadataStore().createSegment(segmentName, null, TIMEOUT),
                ex -> ex instanceof StreamSegmentExistsException);

        AssertExtensions.assertSuppliedFutureThrows(
                "getSegmentInfo() did not throw DataCorruptionException for corrupted state file.",
                () -> context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT),
                ex -> ex instanceof DataCorruptionException);

        // Finally, delete the corrupted file and recreate it - everything should be fine now.
        context.getMetadataStore().deleteSegment(segmentName, TIMEOUT)
               .thenCompose(v -> context.getMetadataStore().createSegment(segmentName, null, TIMEOUT))
               .join();
        val si2 = context.getMetadataStore().getSegmentInfo(segmentName, TIMEOUT).join();
        assertEquals("Unexpected result from getSegmentInfo() after recreation.", expectedSi, si2);
    }

    @Override
    protected TestContext createTestContext(TestConnector connector) {
        return new StorageTestContext(connector);
    }

    private class StorageTestContext extends TestContext {
        final TestStorage storage;
        @Getter
        final StorageMetadataStore metadataStore;
        final AtomicInteger storageReadCount;

        StorageTestContext(TestConnector connector) {
            super(connector);
            this.storage = new TestStorage(new InMemoryStorage(), executorService());
            this.storage.initialize(1L);
            this.metadataStore = new StorageMetadataStore(this.connector, this.storage, executorService());
            this.storageReadCount = new AtomicInteger(0);
            this.storage.setReadInterceptor((segmentName, w) -> storageReadCount.incrementAndGet());
        }

        @Override
        int getStoreReadCount() {
            return this.storageReadCount.get();
        }

        @Override
        void setGetInfoErrorInjector(ErrorInjector<Exception> ei) {
            this.storage.setGetErrorInjector(ei);
        }

        @Override
        public void close() {
            this.storage.close();
        }
    }
}
