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
package io.pravega.segmentstore.storage;

import com.google.common.collect.Iterators;
import io.pravega.common.MathHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static io.pravega.test.common.AssertExtensions.assertSuppliedFutureThrows;
import static io.pravega.test.common.AssertExtensions.assertThrows;

import static io.pravega.shared.NameUtils.INTERNAL_NAME_PREFIX;

/**
 * Base class for testing any implementation of the Storage interface.
 */
public abstract class StorageTestBase extends ThreadPooledTestSuite {
    //region General Test arguments
    protected static final Duration TIMEOUT = Duration.ofSeconds(60);
    protected static final long DEFAULT_EPOCH = 1;
    protected static final int APPENDS_PER_SEGMENT = 10;
    protected static final String APPEND_FORMAT = "Segment_%s_Append_%d";
    private static final int SEGMENT_COUNT = 4;
    protected final Random rnd = new Random(0);

    @Getter(AccessLevel.PROTECTED)
    @Setter(AccessLevel.PROTECTED)
    private boolean isTestingSystemSegment = false;

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    //endregion

    //region Tests

    /**
     * Tests the create() method.
     * @throws Exception If any unexpected error occurred.
     */
    @Test
    public void testCreate() throws Exception {
        String segmentName = createSegmentName("foo_open");
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            Assert.assertTrue("Expected the segment to exist.", s.exists(segmentName, null).join());
            assertThrows("create() did not throw for existing StreamSegment.",
                    () -> createSegment(segmentName, s),
                    ex -> ex instanceof StreamSegmentExistsException);
        }
    }

    /**
     * Tests the exists API.
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testListSegmentsWithOneSegment() throws Exception {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            Iterator<SegmentProperties> iterator = s.listSegments().get();
            Assert.assertFalse(iterator.hasNext());
            createSegment(segmentName, s);
            iterator = s.listSegments().get();
            Assert.assertTrue(iterator.hasNext());
            SegmentProperties prop = iterator.next();
            Assert.assertEquals(prop.getName(), segmentName);
            Assert.assertFalse(iterator.hasNext());
        }
    }

    /**
     * Tests the ability to list Segments.
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testListSegments() throws Exception {
        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, new SegmentRollingPolicy(1));
        Set<String> sealedSegments = new HashSet<>();
        s.initialize(1);
        int expectedCount = 50;
        byte[] data = "data".getBytes();
        for (int i = 0; i < expectedCount; i++) {
            String segmentName = "segment-" + i;
            val wh1 = s.create(segmentName);
            // Write data.
            s.write(wh1, 0, new ByteArrayInputStream(data), data.length);
            if (rnd.nextInt(2) == 1) {
                s.seal(wh1);
                sealedSegments.add(segmentName);
            }
        }
        Iterator<SegmentProperties> it = s.listSegments();
        int actualCount = 0;
        while (it.hasNext()) {
            SegmentProperties curr = it.next();
            //check the length matches
            Assert.assertEquals(curr.getLength(), data.length);
            if (sealedSegments.contains(curr.getName())) {
                Assert.assertTrue(curr.isSealed());
            } else {
                Assert.assertFalse(curr.isSealed());
            }
            ++actualCount;
        }
        Assert.assertEquals(actualCount, expectedCount);
    }

    /**
     * Tests the ability of next to throw NoSuchElementException when asked for more segments than created.
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testListSegmentsNextNoSuchElementException() throws Exception {
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            Iterator<SegmentProperties> iterator = s.listSegments().get();
            Assert.assertFalse(iterator.hasNext());
            int expectedCount = 10; // Create more segments than 1000 which is the maximum number of segments in one batch.
            for (int i = 0; i < expectedCount; i++) {
                String segmentName = "segment-" + i;
                createSegment(segmentName, s);
            }
            iterator = s.listSegments().get();
            for (int i = 0; i < expectedCount; i++) {
                SegmentProperties prop = iterator.next();
            }
            Iterator<SegmentProperties> finalIterator = iterator;
            AssertExtensions.assertThrows(NoSuchElementException.class, () -> finalIterator.next());
        }
    }

    /**
     * Tests listSegments() on deleting some segments.
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testListSegmentsWithDeletes() throws Exception {
        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, new SegmentRollingPolicy(1));
        s.initialize(DEFAULT_EPOCH);
        Set<String> deletedSegments = new HashSet<>();
        int expectedCount = 50;
        for (int i = 0; i < expectedCount; i++) {
            String segmentName = "segment-" + i;
            SegmentHandle handle = s.create(segmentName);
            if (rnd.nextInt(2) == 1) {
                s.delete(handle);
                deletedSegments.add(segmentName);
            }
        }
        Iterator<SegmentProperties> it = s.listSegments();
        expectedCount -= deletedSegments.size();
        Assert.assertEquals(expectedCount, Iterators.size(it));
    }

    /**
     * Tests the delete() method.
     * @throws Exception If any unexpected error occurred.
     */
    @Test
    public void testDelete() throws Exception {
        String segmentName = createSegmentName("foo_open");
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);

            //Delete the segment.
            s.openWrite(segmentName).thenCompose(handle -> s.delete(handle, null)).join();
            Assert.assertFalse("Expected the segment to not exist.", s.exists(segmentName, null).join());
            assertThrows("getStreamSegmentInfo() did not throw for deleted StreamSegment.",
                    () -> s.getStreamSegmentInfo(segmentName, null).join(),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the openRead() and openWrite() methods.
     * @throws Exception If any unexpected error occurred.
     */
    @Test
    public void testOpen() throws Exception {
        String segmentName = createSegmentName("foo_open");
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            // Segment does not exist.
            assertFutureThrows("openWrite() did not throw for non-existent StreamSegment.",
                    s.openWrite(segmentName),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            assertFutureThrows("openRead() did not throw for non-existent StreamSegment.",
                    s.openRead(segmentName),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the write() method.
     *
     * @throws Exception If any unexpected error occurred.
     */
    @Test
    public void testWrite() throws Exception {
        String segmentName = createSegmentName("foo_write");
        int appendCount = 10;

        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);

            // Invalid handle.
            val readOnlyHandle = s.openRead(segmentName).join();
            assertSuppliedFutureThrows(
                    "write() did not throw for read-only handle.",
                    () -> s.write(readOnlyHandle, 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            assertSuppliedFutureThrows(
                    "write() did not throw for handle pointing to inexistent segment.",
                    () -> s.write(createInexistentSegmentHandle(s, false), 0, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            val writeHandle = s.openWrite(segmentName).join();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                byte[] writeData = String.format(APPEND_FORMAT, segmentName, j).getBytes();

                val dataStream = new ByteArrayInputStream(writeData);
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                offset += writeData.length;
            }

            // Check bad offset.
            final long finalOffset = offset;
            assertSuppliedFutureThrows("write() did not throw bad offset write (smaller).",
                    () -> s.write(writeHandle, finalOffset - 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            assertSuppliedFutureThrows("write() did not throw bad offset write (larger).",
                    () -> s.write(writeHandle, finalOffset + 1, new ByteArrayInputStream("h".getBytes()), 1, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);

            // Check post-delete write.
            s.delete(writeHandle, TIMEOUT).join();
            assertSuppliedFutureThrows("write() did not throw for a deleted StreamSegment.",
                    () -> s.write(writeHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the read() method.
     *
     * @throws Exception If any unexpected error occurred.
     */
    @Test
    public void testRead() throws Exception {
        final String context = createSegmentName("Read");
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            // Check invalid segment name.
            assertSuppliedFutureThrows("read() did not throw for invalid segment name.",
                    () -> s.read(createInexistentSegmentHandle(s, true), 0, new byte[1], 0, 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);

            // Do some reading.
            for (Entry<String, ByteArrayOutputStream> entry : appendData.entrySet()) {
                String segmentName = entry.getKey();
                val readHandle = s.openRead(segmentName).join();
                byte[] expectedData = entry.getValue().toByteArray();

                for (int offset = 0; offset < expectedData.length / 2; offset++) {
                    int length = expectedData.length - 2 * offset;
                    byte[] readBuffer = new byte[length];
                    int bytesRead = s.read(readHandle, offset, readBuffer, 0, readBuffer.length, TIMEOUT).join();
                    Assert.assertEquals(String.format("Unexpected number of bytes read from offset %d.", offset),
                            length, bytesRead);
                    AssertExtensions.assertArrayEquals(String.format("Unexpected read result from offset %d.", offset),
                            expectedData, offset, readBuffer, 0, bytesRead);
                }
            }

            // Test bad parameters.
            val testSegment = getSegmentName(0, context);
            val testSegmentHandle = s.openRead(testSegment).join();
            byte[] testReadBuffer = new byte[10];
            assertSuppliedFutureThrows("read() allowed reading with negative read offset.",
                    () -> s.read(testSegmentHandle, -1, testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertSuppliedFutureThrows("read() allowed reading with offset beyond Segment length.",
                    () -> s.read(testSegmentHandle, s.getStreamSegmentInfo(testSegment, TIMEOUT).join().getLength() + 1,
                            testReadBuffer, 0, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertSuppliedFutureThrows("read() allowed reading with negative read buffer offset.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, -1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertSuppliedFutureThrows("read() allowed reading with invalid read buffer length.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, 1, testReadBuffer.length, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            assertSuppliedFutureThrows("read() allowed reading with invalid read length.",
                    () -> s.read(testSegmentHandle, 0, testReadBuffer, 0, testReadBuffer.length + 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException || ex instanceof ArrayIndexOutOfBoundsException);

            // Check post-delete read.
            s.delete(s.openWrite(testSegment).join(), TIMEOUT).join();
            assertSuppliedFutureThrows("read() did not throw for a deleted StreamSegment.",
                    () -> s.read(testSegmentHandle, 0, new byte[1], 0, 1, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the seal() method.
     *
     * @throws Exception If any unexpected error occurred.
     */
    @Test
    public void testSeal() throws Exception {
        final String context = createSegmentName("Seal");
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            // Check segment not exists.
            assertSuppliedFutureThrows("seal() did not throw for non-existent segment name.",
                    () -> s.seal(createInexistentSegmentHandle(s, false), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);
            int deleteCount = 0;
            for (String segmentName : appendData.keySet()) {
                val readHandle = s.openRead(segmentName).join();
                assertSuppliedFutureThrows("seal() did not throw for read-only handle.",
                        () -> s.seal(readHandle, TIMEOUT),
                        ex -> ex instanceof IllegalArgumentException);

                val writeHandle = s.openWrite(segmentName).join();
                s.seal(writeHandle, TIMEOUT).join();

                //Seal is idempotent. Resealing an already sealed segment should work.
                s.seal(writeHandle, TIMEOUT).join();
                assertSuppliedFutureThrows("write() did not throw for a sealed StreamSegment.",
                        () -> s.write(writeHandle, s.getStreamSegmentInfo(segmentName, TIMEOUT).
                                join().getLength(), new ByteArrayInputStream("g".getBytes()), 1, TIMEOUT),
                        ex -> ex instanceof StreamSegmentSealedException);

                // Check post-delete seal. Half of the segments use the existing handle, and half will re-acquire it.
                // We want to reacquire it because OpenWrite will return a read-only handle for sealed segments.
                boolean reacquireHandle = (deleteCount++) % 2 == 0;
                SegmentHandle deleteHandle = writeHandle;
                if (reacquireHandle) {
                    deleteHandle = s.openWrite(segmentName).join();
                }

                s.delete(deleteHandle, TIMEOUT).join();
                assertSuppliedFutureThrows("seal() did not throw for a deleted StreamSegment.",
                        () -> s.seal(writeHandle, TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);
            }
        }
    }

    /**
     * Tests the concat() method.
     *
     * @throws Exception If any unexpected error occurred.
     */
    @Test
    public void testConcat() throws Exception {
        final String context = createSegmentName("Concat");
        try (Storage s = createStorage()) {
            testConcat(context, s);
        }
    }

    protected void testConcat(String context, Storage s) throws Exception {
        s.initialize(DEFAULT_EPOCH);
        HashMap<String, ByteArrayOutputStream> appendData = populate(s, context);

        // Check invalid segment name.
        val firstSegmentName = getSegmentName(0, context);
        val firstSegmentHandle = s.openWrite(firstSegmentName).join();
        val sealedSegmentName = createSegmentName("SealedSegment");
        createSegment(sealedSegmentName, s);
        val sealedSegmentHandle = s.openWrite(sealedSegmentName).join();
        s.write(sealedSegmentHandle, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT).join();
        s.seal(sealedSegmentHandle, TIMEOUT).join();
        AtomicLong firstSegmentLength = new AtomicLong(s.getStreamSegmentInfo(firstSegmentName,
                TIMEOUT).join().getLength());
        assertSuppliedFutureThrows("concat() did not throw for non-existent target segment name.",
                () -> s.concat(createInexistentSegmentHandle(s, false), 0, sealedSegmentName, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        assertSuppliedFutureThrows("concat() did not throw for invalid source StreamSegment name.",
                () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), "foo2", TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        ArrayList<String> concatOrder = new ArrayList<>();
        concatOrder.add(firstSegmentName);
        for (String sourceSegment : appendData.keySet()) {
            if (sourceSegment.equals(firstSegmentName)) {
                // FirstSegment is where we'll be concatenating to.
                continue;
            }

            assertSuppliedFutureThrows("Concat allowed when source segment is not sealed.",
                    () -> s.concat(firstSegmentHandle, firstSegmentLength.get(), sourceSegment, TIMEOUT),
                    ex -> ex instanceof IllegalStateException);

            // Seal the source segment and then re-try the concat
            val sourceWriteHandle = s.openWrite(sourceSegment).join();
            s.seal(sourceWriteHandle, TIMEOUT).join();
            SegmentProperties preConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
            SegmentProperties sourceProps = s.getStreamSegmentInfo(sourceSegment, TIMEOUT).join();

            s.concat(firstSegmentHandle, firstSegmentLength.get(), sourceSegment, TIMEOUT).join();
            concatOrder.add(sourceSegment);
            SegmentProperties postConcatTargetProps = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
            Assert.assertFalse("concat() did not delete source segment", s.exists(sourceSegment, TIMEOUT).join());

            // Only check lengths here; we'll check the contents at the end.
            Assert.assertEquals("Unexpected target StreamSegment.length after concatenation.",
                    preConcatTargetProps.getLength() + sourceProps.getLength(), postConcatTargetProps.getLength());
            firstSegmentLength.set(postConcatTargetProps.getLength());
        }

        // Check the contents of the first StreamSegment. We already validated that the length is correct.
        SegmentProperties segmentProperties = s.getStreamSegmentInfo(firstSegmentName, TIMEOUT).join();
        byte[] readBuffer = new byte[(int) segmentProperties.getLength()];

        // Read the entire StreamSegment.
        int bytesRead = s.read(firstSegmentHandle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);

        // Check, concat-by-concat, that the final data is correct.
        int offset = 0;
        for (String segmentName : concatOrder) {
            byte[] concatData = appendData.get(segmentName).toByteArray();
            AssertExtensions.assertArrayEquals("Unexpected concat data.", concatData, 0, readBuffer, offset,
                    concatData.length);
            offset += concatData.length;
        }

        Assert.assertEquals("Concat included more bytes than expected.", offset, readBuffer.length);
    }

    /**
     * Verifies that the Storage implementation enforces fencing: if a segment owner changes, no operation is allowed
     * on a segment until open() is called on it.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public abstract void testFencing() throws Exception;

    private String getSegmentName(int id, String context) {
        return String.format("%s_%s", context, id);
    }

    private HashMap<String, ByteArrayOutputStream> populate(Storage s, String context) throws Exception {
        HashMap<String, ByteArrayOutputStream> appendData = new HashMap<>();
        for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            String segmentName = getSegmentName(segmentId, context);
            createSegment(segmentName, s);
            val writeHandle = s.openWrite(segmentName).join();
            ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
            appendData.put(segmentName, writeStream);

            long offset = 0;
            for (int j = 0; j < APPENDS_PER_SEGMENT; j++) {
                byte[] writeData = populate(APPEND_FORMAT.length());

                val dataStream = new ByteArrayInputStream(writeData);
                s.write(writeHandle, offset, dataStream, writeData.length, TIMEOUT).join();
                writeStream.write(writeData);
                offset += writeData.length;
            }
        }
        return appendData;
    }

    protected void populate(byte[] data) {
        rnd.nextBytes(data);
    }

    protected byte[] populate(int size) {
        byte[] bytes = new byte[size];
        populate(bytes);
        return bytes;
    }

    //endregion

    //region Protected utility methods containing common code.

    protected void verifyReadOnlyOperationsSucceed(SegmentHandle handle, Storage storage) {
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment does not exist.", exists);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertNotNull("Unexpected response from getStreamSegmentInfo.", si);

        byte[] readBuffer = new byte[(int) si.getLength()];
        int readBytes = storage.read(handle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);
    }

    protected void verifyWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = populate(5);
        storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT).join();

        final String concatName = "concat" + Long.toString(System.currentTimeMillis());
        storage.create(concatName, TIMEOUT).join();
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        storage.concat(handle, si.getLength() + data.length, concatHandle.getSegmentName(), TIMEOUT).join();
    }

    protected void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = populate(5);
        AssertExtensions.assertSuppliedFutureThrows(
                "Write was not fenced out.",
                () -> storage.write(handle, si.getLength(), new ByteArrayInputStream(data), data.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
    }

    protected void verifyConcatOperationsFail(SegmentHandle handle, Storage storage) {
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        final byte[] data = populate(5);
        // Create a second segment and try to concat it into the primary one.
        final String concatName = "concat" + Long.toString(System.currentTimeMillis());
        createSegment(concatName, storage);
        val concatHandle = storage.openWrite(concatName).join();
        storage.write(concatHandle, 0, new ByteArrayInputStream(data), data.length, TIMEOUT).join();
        storage.seal(concatHandle, TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "Concat was not fenced out.",
                () -> storage.concat(handle, si.getLength(), concatHandle.getSegmentName(), TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        storage.delete(concatHandle, TIMEOUT).join();
    }

    protected void verifyFinalWriteOperationsSucceed(SegmentHandle handle, Storage storage) {
        storage.seal(handle, TIMEOUT).join();
        storage.delete(handle, TIMEOUT).join();

        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment still exists after deletion.", exists);
    }

    protected void verifyFinalWriteOperationsFail(SegmentHandle handle, Storage storage) {
        AssertExtensions.assertSuppliedFutureThrows(
                "Seal was allowed on fenced Storage.",
                () -> storage.seal(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertFalse("Segment was sealed after rejected call to seal.", si.isSealed());

        AssertExtensions.assertSuppliedFutureThrows(
                "Delete was allowed on fenced Storage.",
                () -> storage.delete(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);
        boolean exists = storage.exists(handle.getSegmentName(), TIMEOUT).join();
        Assert.assertTrue("Segment was deleted after rejected call to delete.", exists);
    }

    protected SegmentHandle createInexistentSegmentHandle(Storage s, boolean readOnly) {
        Random rnd = RandomFactory.create();
        String segmentName = (isTestingSystemSegment() ? INTERNAL_NAME_PREFIX : "") + "Inexistent_" + MathHelpers.abs(rnd.nextInt());
        createSegment(segmentName, s);
        return (readOnly ? s.openRead(segmentName) : s.openWrite(segmentName))
                .thenCompose(handle -> s.openWrite(segmentName)
                        .thenCompose(h2 -> s.delete(h2, TIMEOUT))
                        .thenApply(v -> handle))
                .join();
    }

    protected void createSegment(String name, Storage s) {
        s.create(name, null).join();
    }

    protected String createSegmentName(String originName) throws Exception {
        if (isTestingSystemSegment()) {
            return INTERNAL_NAME_PREFIX + originName;
        } else {
            return originName;
        }
    }

    //endregion

    //region Abstract methods

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     * @throws Exception If any unexpected error occurred.
     */
    protected abstract Storage createStorage() throws Exception;

    //endregion
}
