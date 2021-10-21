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
package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import lombok.Cleanup;
import lombok.val;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.SequenceInputStream;

import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static org.junit.Assert.assertEquals;

/**
 * This is to test Storage in No-Op mode for user segments only.
 *
 * In this test the underlying userStorage is NOT provided so all user segments are lost.
 * Then all the reading operations (to verify segment content) are removed from this test.
 *
 * The focus of this test is to ensure, in case user segments are no-oped, all the operations
 * going to storage can still succeed.
 *
 */
public class NoOpStorageUserDataWriteOnlyTests extends StorageTestBase {

    private SyncStorage systemStorage;
    private StorageExtraConfig config;

    @Before
    public void setUp() {
        systemStorage = new InMemoryStorageFactory(executorService()).createSyncStorage();
        config = StorageExtraConfig.builder().with(StorageExtraConfig.STORAGE_NO_OP_MODE, true).build();
    }

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new NoOpStorage(config, systemStorage, null), executorService());
    }

    @Override
    @Test
    public void testCreate() {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            // In No-Op mode, segment can be created multiple times. Data integrity is out of concern in No-Op mode.
            createSegment(segmentName, s);
            // Delete and make sure it can be recreated.
            s.openWrite(segmentName).thenCompose(handle -> s.delete(handle, null)).join();
            createSegment(segmentName, s);
        }
    }

    @Override
    @Test
    public void testDelete() {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            //Ensure delete operation can be completed.
            s.openWrite(segmentName).thenCompose(handle -> s.delete(handle, null)).join();
            s.getStreamSegmentInfo(segmentName, TIMEOUT);
        }
    }

    @Override
    @Test
    public void testOpen() {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            // Segment does not exist but openWrite should still succeed in No-Op mode
            s.openWrite(segmentName).join();
            s.openRead(segmentName);
        }
    }

    @Test
    public void testExist() {
        String segmentName = "foo_exist";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            assertFutureThrows("exists() did not throw UnsupportedOperationException.",
                    s.exists(segmentName, TIMEOUT),
                    ex -> ex instanceof UnsupportedOperationException);
        }
    }

    @Test
    public void testUnseal() throws Exception {
        StorageExtraConfig config = StorageExtraConfig.builder().build();
        NoOpStorage.NoOpSegmentHandle handle = new NoOpStorage.NoOpSegmentHandle("foo_unseal");
        @Cleanup
        NoOpStorage storage = new NoOpStorage(config, systemStorage, null);
        storage.unseal(handle);
    }

    @Test
    public void testTruncate() throws Exception {
        StorageExtraConfig config = StorageExtraConfig.builder().build();
        NoOpStorage.NoOpSegmentHandle handle = new NoOpStorage.NoOpSegmentHandle("foo_truncate");
        @Cleanup
        NoOpStorage storage = new NoOpStorage(config, systemStorage, null);
        storage.truncate(handle, 0);
    }

    @Test
    public void testSupportTruncation() throws Exception {
        StorageExtraConfig config = StorageExtraConfig.builder().build();
        NoOpStorage.NoOpSegmentHandle handle = new NoOpStorage.NoOpSegmentHandle("foo_supportTruncation");
        @Cleanup
        NoOpStorage storage = new NoOpStorage(config, systemStorage, null);
        assertEquals(systemStorage.supportsTruncation(), storage.supportsTruncation());
    }

    @Override
    @Test
    public void testWrite() throws Exception {
        String segmentName = "foo_write";
        int appendCount = 10;

        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);

            val writeHandle = s.openWrite(segmentName).join();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format(APPEND_FORMAT, segmentName, j).getBytes();

                val dataStream = new SequenceInputStream(new ByteArrayInputStream(writeData), new ByteArrayInputStream(new byte[100]));
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                offset += writeData.length;
            }

        }
    }

    @Override
    @Test
    public void testRead() throws Exception {
        final String segmentName = "TestRead";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            val segmentHandle = s.openWrite(segmentName).join();

            assertFutureThrows("read() did not throw UnsupportedOperationException.",
                    s.read(segmentHandle, 0, null, 0, 0, TIMEOUT),
                    ex -> ex instanceof UnsupportedOperationException);
        }
    }

    @Override
    @Test
    public void testSeal() throws Exception {
        final String segmentName = "sealSegment";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            SegmentHandle handle = s.openWrite(segmentName).join();
            assertEquals(segmentName, handle.getSegmentName());
            assertEquals(false, handle.isReadOnly());
            s.seal(handle, TIMEOUT).join();
        }
    }

    @Override
    @Test
    public void testConcat() throws Exception {
        final String firstSegmentName = "firstSegment";
        final String secondSegmentName = "secondSegment";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            val firstSegmentHandle = s.openWrite(firstSegmentName).join();
            val secondSegmentHandle = s.openWrite(secondSegmentName).join();
            s.concat(firstSegmentHandle, 0, secondSegmentHandle.getSegmentName(), TIMEOUT).join();
        }
    }

    @Override
    public void testFencing() throws Exception {
    }

    @Override
    public void testListSegmentsWithOneSegment() {
    }

    @Override
    public void testListSegmentsNextNoSuchElementException() {
    }
}
