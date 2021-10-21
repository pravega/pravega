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
package io.pravega.segmentstore.storage.rolling;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Random;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the RollingStorage class.
 */
public class RollingStorageTests extends RollingStorageTestBase {
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(100);
    private static final String SEGMENT_NAME = "RollingSegment";
    private static final int SMALL_WRITE_LENGTH = (int) (DEFAULT_ROLLING_POLICY.getMaxLength() * 0.24);
    private static final int LARGE_WRITE_LENGTH = (int) (DEFAULT_ROLLING_POLICY.getMaxLength() * 1.8);
    private static final int WRITE_COUNT = APPENDS_PER_SEGMENT * 2;

    /**
     * Tests the ability to roll over Segments.
     */
    @Test
    public void testRolling() throws Exception {
        // Write small and large writes, alternatively.
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);
        s.create(SEGMENT_NAME);
        val writeHandle = s.openWrite(SEGMENT_NAME);
        val readHandle = s.openRead(SEGMENT_NAME); // Open now, before writing, so we force a refresh.
        val writeStream = new ByteArrayOutputStream();
        populate(s, writeHandle, writeStream);

        // Check that no file has exceeded its maximum length.
        byte[] writtenData = writeStream.toByteArray();
        Assert.assertEquals("Unexpected segment length.", writtenData.length, s.getStreamSegmentInfo(SEGMENT_NAME).getLength());
        int checkedLength = 0;
        while (checkedLength < writtenData.length) {
            String chunkName = NameUtils.getSegmentChunkName(SEGMENT_NAME, checkedLength);
            Assert.assertTrue("Inexistent SegmentChunk: " + chunkName, baseStorage.exists(chunkName));
            val chunkInfo = baseStorage.getStreamSegmentInfo(chunkName);
            int expectedLength = (int) Math.min(DEFAULT_ROLLING_POLICY.getMaxLength(), writtenData.length - checkedLength);
            Assert.assertEquals("Unexpected SegmentChunk length for: " + chunkName, expectedLength, chunkInfo.getLength());
            checkedLength += expectedLength;

            if (checkedLength < writtenData.length) {
                Assert.assertTrue("Expected SegmentChunk to be sealed: " + chunkName, chunkInfo.isSealed());
            }
        }

        checkWrittenData(writtenData, readHandle, s);
    }

    /**
     * Tests the ability to auto-refresh a Write Handle upon offset disagreement.
     */
    @Test
    public void testRefreshHandleBadOffset() throws Exception {
        // Write small and large writes, alternatively.
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // We use this handle for writing.
        val wh1 = s.create(SEGMENT_NAME);

        // We use these handles for attempting to write in parallel or read. Open them now, before writing, so we force refresh.
        val wh2 = s.openWrite(SEGMENT_NAME); // We use this to write in parallel.
        val wh3 = s.openWrite(SEGMENT_NAME); // We use this to read using a Write Handle.
        val rh1 = s.openRead(SEGMENT_NAME); // We use this to read using a Read Handle.

        // Write data.
        byte[] data = "data".getBytes();
        s.write(wh1, 0, new ByteArrayInputStream(data), data.length);
        s.write(wh2, data.length, new ByteArrayInputStream(data), data.length);

        // Check that no file has exceeded its maximum length.
        byte[] expectedData = new byte[data.length * 2];
        System.arraycopy(data, 0, expectedData, 0, data.length);
        System.arraycopy(data, 0, expectedData, data.length, data.length);

        // Read using the handle we just used for writing.
        checkWrittenData(expectedData, wh2, s);

        // Read using an out-of-date read handle.
        checkWrittenData(expectedData, rh1, s);

        // Read using an out-of-date write handle.
        checkWrittenData(expectedData, wh3, s);

        // And then verify we can still use that write handle for additional writing.
        s.write(wh3, expectedData.length, new ByteArrayInputStream(data), data.length);
        byte[] finalExpectedData = new byte[expectedData.length + data.length];
        System.arraycopy(expectedData, 0, finalExpectedData, 0, expectedData.length);
        System.arraycopy(data, 0, finalExpectedData, expectedData.length, data.length);
        checkWrittenData(finalExpectedData, wh3, s);
    }

    /**
     * Tests the ability to truncate Segments.
     */
    @Test
    public void testTruncate() throws Exception {
        // Write small and large writes, alternatively.
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);
        s.create(SEGMENT_NAME);
        val writeHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val readHandle = s.openRead(SEGMENT_NAME); // Open now, before writing, so we force a refresh.
        val writeStream = new ByteArrayOutputStream();
        populate(s, writeHandle, writeStream);
        byte[] writtenData = writeStream.toByteArray();

        // Test that truncate works in this scenario.
        testProgressiveTruncate(writeHandle, readHandle, writtenData, s, baseStorage);

        // Do some more writes and verify they are added properly.
        int startOffset = writtenData.length;
        populate(s, writeHandle, writeStream);
        writtenData = writeStream.toByteArray();
        checkWrittenData(writtenData, startOffset, readHandle, s);

        // Verify we cannot concat a truncated segment into another.
        final String targetSegmentName = "TargetSegment";
        s.create(targetSegmentName);
        val targetSegmentHandle = s.openWrite(targetSegmentName);
        s.seal(writeHandle);
        AssertExtensions.assertThrows(
                "concat() allowed using a truncated segment as a source.",
                () -> s.concat(targetSegmentHandle, 0, SEGMENT_NAME),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the ability to truncate Sealed Segments.
     */
    @Test
    public void testTruncateSealed() throws Exception {
        // Write small and large writes, alternatively.
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a Segment, write some data, then seal it.
        s.create(SEGMENT_NAME);
        val appendHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, appendHandle, writeStream);
        s.seal(appendHandle);
        byte[] writtenData = writeStream.toByteArray();

        val truncateHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        Assert.assertTrue("Handle not read-only after sealing.", truncateHandle.isReadOnly());
        Assert.assertTrue("Handle not sealed after sealing.", truncateHandle.isSealed());

        // Test that truncate works in this scenario.
        testProgressiveTruncate(truncateHandle, truncateHandle, writtenData, s, baseStorage);
    }

    /**
     * Tests the ability to (not) execute modify operations on already sealed segments. Verifies appropriate exceptions
     * are being thrown.
     */
    @Test
    public void testSealedSegment() {
        final String segmentName = "Segment";
        final String sourceSegmentName = "SourceSegment";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);

            // Create and seal the segment.
            s.create(segmentName, TIMEOUT).thenCompose(h -> s.seal(h, TIMEOUT)).join();
            val handle = s.openWrite(segmentName).join();

            // Modify operations should not succeed.
            AssertExtensions.assertSuppliedFutureThrows(
                    "write() worked on previously sealed segment.",
                    () -> s.write(handle, 0, new ByteArrayInputStream(new byte[0]), 0, TIMEOUT),
                    ex -> ex instanceof StreamSegmentSealedException);

            s.create(sourceSegmentName, TIMEOUT).thenCompose(h -> s.seal(h, TIMEOUT)).join();
            AssertExtensions.assertSuppliedFutureThrows(
                    "concat() worked on previously sealed segment.",
                    () -> s.concat(handle, 0, sourceSegmentName, TIMEOUT),
                    ex -> ex instanceof StreamSegmentSealedException);

            // Seal is idempotent.
            s.seal(handle, TIMEOUT).join();
        }
    }

    /**
     * Tests the case when Create was interrupted after it created the Header file but before populating it.
     */
    @Test
    public void testCreateRecovery() throws Exception {
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create an empty header file. This simulates a create() operation that failed mid-way.
        baseStorage.create(NameUtils.getHeaderSegmentName(SEGMENT_NAME));
        Assert.assertFalse("Not expecting Segment to exist.", s.exists(SEGMENT_NAME));
        AssertExtensions.assertThrows(
                "Not expecting Segment to exist (getStreamSegmentInfo).",
                () -> s.getStreamSegmentInfo(SEGMENT_NAME),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "Not expecting Segment to exist (openHandle).",
                () -> s.openRead(SEGMENT_NAME),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Retry the operation and verify everything is in place.
        s.create(SEGMENT_NAME);
        val si = s.getStreamSegmentInfo(SEGMENT_NAME);
        Assert.assertEquals("Expected the Segment to have been created.", 0, si.getLength());
    }

    @Test
    public void testCreateFailure() {
        @Cleanup
        val baseStorage = new TestStorage();
        baseStorage.writeFailure = IntentionalException::new;

        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create an empty header file. This simulates a create() operation that failed mid-way.
        val headerSegmentName = NameUtils.getHeaderSegmentName(SEGMENT_NAME);
        AssertExtensions.assertThrows("", () -> s.create(SEGMENT_NAME), ex -> ex instanceof IntentionalException);
        Assert.assertFalse(baseStorage.exists(headerSegmentName));
    }

    /**
     * Tests the case when Delete worked partially (only some SegmentChunks were deleted, or all SegmentChunks were deleted
     * but the Header still exists).
     */
    @Test
    public void testDeleteFailure() throws Exception {
        final int failAtIndex = 1;
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);
        s.create(SEGMENT_NAME);
        val writeHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        populate(s, writeHandle, null);

        // Simulate a deletion failure that is not a StreamSegmentNotExistsException.
        String failOnDelete = writeHandle.chunks().get(failAtIndex).getName();
        baseStorage.deleteFailure = sn -> sn.equals(failOnDelete) ? new IntentionalException() : null;
        AssertExtensions.assertThrows(
                "delete() did not propagate proper exception on failure.",
                () -> s.delete(writeHandle),
                ex -> ex instanceof IntentionalException);

        Assert.assertTrue("Not expecting segment to be deleted yet.", s.exists(SEGMENT_NAME));
        Assert.assertFalse("Expected first SegmentChunk to be marked as deleted.", writeHandle.chunks().get(failAtIndex - 1).exists());
        Assert.assertTrue("Expected failed-to-delete SegmentChunk to not be marked as deleted.", writeHandle.chunks().get(failAtIndex).exists());
        Assert.assertTrue("Expected subsequent SegmentChunk to not be marked as deleted.", writeHandle.chunks().get(failAtIndex + 1).exists());

        // Clear the intentional failure, but do delete the SegmentChunk, to verify it properly handles missing SegmentChunks.
        baseStorage.deleteFailure = null;
        baseStorage.delete(baseStorage.openRead(failOnDelete));
        s.delete(writeHandle);
        Assert.assertFalse("Expecting the segment to be deleted.", s.exists(SEGMENT_NAME));
        Assert.assertTrue("Expected the handle to be marked as deleted.", writeHandle.isDeleted());
        Assert.assertFalse("Expected all SegmentChunks to be marked as deleted.", writeHandle.chunks().stream().anyMatch(SegmentChunk::exists));
    }

    /**
     * Tests the ability to use native concat for those cases when it's appropriate.
     */
    @Test
    public void testConcatNatively() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final int initialSourceLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() - initialTargetLength;
        final String sourceSegmentName = "SourceSegment";
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a target Segment and write a little data to it.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);

        // Create a source Segment and write a little data to it, making sure it is small enough to fit into the target
        // when we need to concat.
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, 1, initialSourceLength, initialSourceLength, writeStream);
        s.seal(sourceHandle);

        // Concat and verify the handle has been updated accordingly.
        s.concat(targetHandle, initialTargetLength, sourceSegmentName);
        checkConcatResult(s, targetHandle, sourceSegmentName, 1, initialTargetLength + initialSourceLength);
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to use native concat for those cases when it's appropriate.
     */
    @Test
    public void testConcatNativelyFailure() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final int initialSourceLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() - initialTargetLength;
        final String sourceSegmentName = "SourceSegment";

        // Concat succeeds, but can't delete header.
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a target and a source, making sure they have the right sizes for a native concat.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, 1, initialSourceLength, initialSourceLength, writeStream);
        s.seal(sourceHandle);

        // Attempt to concat, but intentionally fail the deletion of the source header.
        baseStorage.deleteFailure = sn -> sn.equals(sourceHandle.getHeaderHandle().getSegmentName()) ? new IntentionalException() : null;
        AssertExtensions.assertThrows(
                "Unexpected exception when doing concat.",
                () -> s.concat(targetHandle, initialTargetLength, sourceSegmentName),
                ex -> ex instanceof IntentionalException);

        // However, the concat should have worked, so the source segment is now inaccessible.
        baseStorage.deleteFailure = null;
        checkConcatResult(s, targetHandle, sourceSegmentName, 1, initialTargetLength + initialSourceLength);
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to concat using the header file for those cases when native concat cannot be used because the
     * source Segment has a single SegmentChunk, but it's too large to fit into the Target's active SegmentChunk.
     */
    @Test
    public void testConcatHeaderSingleFile() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final int bigSourceLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() - initialTargetLength + 1;
        final String sourceSegmentName = "SourceSegment";
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a Target Segment and a Source Segment and write some data to them.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, 1, bigSourceLength, bigSourceLength, writeStream);
        s.seal(sourceHandle);

        // Concat and verify the handle has been updated accordingly.
        s.concat(targetHandle, initialTargetLength, sourceSegmentName);
        checkConcatResult(s, targetHandle, sourceSegmentName, 2, initialTargetLength + bigSourceLength);
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to concat using the header file for those cases when native concat cannot be used because the
     * source Segment has multiple SegmentChunks.
     */
    @Test
    public void testConcatHeaderMultiFile() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final String sourceSegmentName = "SourceSegment";
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a Target Segment and a Source Segment and write some data to them.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, APPENDS_PER_SEGMENT, initialTargetLength, initialTargetLength, writeStream);
        s.seal(sourceHandle);

        // Concat and verify the handle has been updated accordingly.
        s.concat(targetHandle, initialTargetLength, sourceSegmentName);
        checkConcatResult(s, targetHandle, sourceSegmentName, 1 + sourceHandle.chunks().size(), initialTargetLength + (int) sourceHandle.length());
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to handle partially executed concat operations for header concat, such as being able to write
     * the concat entry but not actually concat the source header file.
     */
    @Test
    public void testConcatHeaderFailure() throws Exception {
        final int initialTargetLength = (int) DEFAULT_ROLLING_POLICY.getMaxLength() / 2;
        final String sourceSegmentName = "SourceSegment";
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a Target Segment and a Source Segment and write some data to them.
        s.create(SEGMENT_NAME);
        val targetHandle = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        val writeStream = new ByteArrayOutputStream();
        populate(s, targetHandle, 1, initialTargetLength, initialTargetLength, writeStream);
        s.create(sourceSegmentName);
        val sourceHandle = (RollingSegmentHandle) s.openWrite(sourceSegmentName);
        populate(s, sourceHandle, APPENDS_PER_SEGMENT, initialTargetLength, initialTargetLength, writeStream);
        s.seal(sourceHandle);

        // Simulate a native concat exception, and try a few times.
        baseStorage.concatFailure = sn -> sn.equals(sourceHandle.getHeaderHandle().getSegmentName()) ? new IntentionalException() : null;
        for (int i = 0; i < 4; i++) {
            AssertExtensions.assertThrows(
                    "Unexpected error reported from concat.",
                    () -> s.concat(targetHandle, initialTargetLength, sourceSegmentName),
                    ex -> ex instanceof IntentionalException);
        }

        // Clear the intentional failure and try again, after which check the results.
        baseStorage.concatFailure = null;
        s.concat(targetHandle, initialTargetLength, sourceSegmentName);
        checkConcatResult(s, targetHandle, sourceSegmentName, 1 + sourceHandle.chunks().size(), initialTargetLength + (int) sourceHandle.length());
        checkWrittenData(writeStream.toByteArray(), s.openRead(SEGMENT_NAME), s);
    }

    /**
     * Tests the ability to handle Segment files with no header, which simulates a scenario where we add RollingStorage
     * to a Storage adapter that did not previously handle files this way.
     */
    @Test
    public void testBackwardsCompatibility() throws Exception {
        final String segmentName = "SonHeaderSegment";
        @Cleanup
        val baseStorage = new TestStorage();
        @Cleanup
        val s = new RollingStorage(baseStorage, DEFAULT_ROLLING_POLICY);
        s.initialize(1);

        // Create a plain Segment in the Base Storage; this will not have any headers or any special file layout.
        baseStorage.create(segmentName);

        // Verify create() with existing non-Header Segment.
        AssertExtensions.assertThrows(
                "create() allowed creating a new Segment which already existed.",
                () -> s.create(segmentName),
                ex -> ex instanceof StreamSegmentExistsException);
        Assert.assertTrue("Non-Header Segment does not exist after failed create() attempt.", baseStorage.exists(segmentName));
        Assert.assertFalse("A header was left behind (after create).",
                baseStorage.exists(NameUtils.getHeaderSegmentName(segmentName)));

        // Verify exists().
        Assert.assertTrue("Unexpected result from exists() when called on a non-header Segment.", s.exists(segmentName));

        // Verify openWrite(), write() and seal(). Verify no rolling even if we exceed default rolling policy.
        val writeHandle = s.openWrite(segmentName);
        val os = new ByteArrayOutputStream();
        populate(s, writeHandle, os);
        s.seal(writeHandle);
        byte[] writtenData = os.toByteArray();
        Assert.assertFalse("A header was left behind (after write).",
                baseStorage.exists(NameUtils.getHeaderSegmentName(segmentName)));

        // Verify getInfo().
        val baseInfo = baseStorage.getStreamSegmentInfo(segmentName);
        val rollingInfo = s.getStreamSegmentInfo(segmentName);
        Assert.assertTrue("Segment not sealed.", baseInfo.isSealed());
        Assert.assertEquals("Unexpected Segment length.", writtenData.length, baseInfo.getLength());
        Assert.assertEquals("GetInfo.Name mismatch between base and rolling.", baseInfo.getName(), rollingInfo.getName());
        Assert.assertEquals("GetInfo.Length mismatch between base and rolling.", baseInfo.getLength(), rollingInfo.getLength());
        Assert.assertEquals("GetInfo.Sealed mismatch between base and rolling.", baseInfo.isSealed(), rollingInfo.isSealed());

        // Verify openRead() and read().
        val readHandle = s.openRead(segmentName);
        checkWrittenData(writtenData, readHandle, s);

        // Verify that truncate() is a no-op.
        for (long truncateOffset = 0; truncateOffset < writtenData.length; truncateOffset += 10) {
            s.truncate(writeHandle, truncateOffset);
        }
        checkWrittenData(writtenData, readHandle, s);

        // Verify concat() with Source & Target non-Header Segments.
        final String nonHeaderName = "NonHeaderSegment";
        baseStorage.create(nonHeaderName);
        val nonHeaderHandle = s.openWrite(nonHeaderName);
        s.concat(nonHeaderHandle, 0, segmentName);
        Assert.assertFalse("NonHeader source still exists after concat to NonHeader Segment.", s.exists(segmentName));
        checkWrittenData(writtenData, s.openRead(nonHeaderName), s);

        // Verify concat() with Source as non-Header Segment, but Target is a Header Segment.
        final String withHeaderName = "WithHeader";
        s.create(withHeaderName, DEFAULT_ROLLING_POLICY);
        s.seal(nonHeaderHandle);
        val withHeaderHandle = s.openWrite(withHeaderName);
        s.concat(withHeaderHandle, 0, nonHeaderName);
        Assert.assertFalse("NonHeader source still exists after concat to Header Segment.", s.exists(nonHeaderName));
        val h1 = (RollingSegmentHandle) s.openRead(withHeaderName);
        checkWrittenData(writtenData, h1, s);
        Assert.assertEquals("Unexpected MaxLength after concat.", DEFAULT_ROLLING_POLICY.getMaxLength(), h1.getRollingPolicy().getMaxLength());

        // Verify concat() with Source as Header Segment, but Target as a non-Header Segment.
        baseStorage.create(nonHeaderName); // We reuse this Segment Name since it should have been gone by now.
        populate(s, withHeaderHandle, os); // Need to create a few SegmentChunks to force a Header concat.
        s.seal(withHeaderHandle);
        s.concat(s.openWrite(nonHeaderName), 0, withHeaderName);
        Assert.assertFalse("NonHeader source still exists after concat to Header Segment.", s.exists(withHeaderName));
        val h2 = (RollingSegmentHandle) s.openRead(nonHeaderName);
        checkWrittenData(writtenData, h2, s);
        Assert.assertEquals("Unexpected MaxLength after concat into non-header segment.",
                SegmentRollingPolicy.NO_ROLLING.getMaxLength(), h2.getRollingPolicy().getMaxLength());

        // Verify delete().
        baseStorage.create(segmentName);
        populate(s, s.openWrite(segmentName), new ByteArrayOutputStream());
        s.delete(s.openWrite(segmentName));
        Assert.assertFalse("Segment still exists after deletion.", s.exists(segmentName));
        Assert.assertFalse("Segment still exists after deletion.", baseStorage.exists(segmentName));
    }

    //region StorageTestBase Implementation

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new RollingStorage(new TestStorage(), DEFAULT_ROLLING_POLICY), executorService());
    }

    //endregion

    //region Helpers

    private void populate(RollingStorage s, SegmentHandle writeHandle, ByteArrayOutputStream writeStream) throws Exception {
        populate(s, writeHandle, WRITE_COUNT, SMALL_WRITE_LENGTH, LARGE_WRITE_LENGTH, writeStream);
    }

    private void populate(RollingStorage s, SegmentHandle writeHandle, int writeCount, int smallWriteSize, int largeWriteSize, ByteArrayOutputStream writeStream) throws Exception {
        final Random rnd = new Random(0);
        int offset = (int) s.getStreamSegmentInfo(writeHandle.getSegmentName()).getLength();
        for (int i = 0; i < writeCount; i++) {
            byte[] appendData = new byte[i % 2 == 0 ? smallWriteSize : largeWriteSize];
            rnd.nextBytes(appendData);
            s.write(writeHandle, offset, new ByteArrayInputStream(appendData), appendData.length);
            offset += appendData.length;
            if (writeStream != null) {
                writeStream.write(appendData);
            }
        }
    }

    private void testProgressiveTruncate(RollingSegmentHandle writeHandle, SegmentHandle readHandle, byte[] writtenData, RollingStorage s, SyncStorage baseStorage) throws Exception {
        int truncateOffset = 0;
        while (true) {
            val initialChunkCount = writeHandle.chunks().size();
            s.truncate(writeHandle, truncateOffset);
            if (writeHandle.chunks().size() != initialChunkCount) {
                // Validate that if we did reduce the handle size, we did it by 1. We will check the actual chunk integrity below.
                Assert.assertEquals(initialChunkCount - 1, writeHandle.chunks().size());
            }

            // Verify we can still read properly.
            checkWrittenData(writtenData, truncateOffset, readHandle, s);

            // Verify each SegmentChunk's existence.
            for (SegmentChunk segmentChunk : writeHandle.chunks()) {
                boolean expectedExists;
                if (writeHandle.isSealed() && segmentChunk.getLastOffset() == writeHandle.length()) {
                    expectedExists = true;
                } else {
                    expectedExists = segmentChunk.getLastOffset() > truncateOffset
                            || (segmentChunk.getStartOffset() == segmentChunk.getLastOffset() && segmentChunk.getLastOffset() == truncateOffset);
                }
                Assert.assertEquals("Unexpected SegmentChunk truncation status for " + segmentChunk + ", truncation offset = " + truncateOffset,
                        expectedExists, segmentChunk.exists());
                boolean existsInStorage = baseStorage.exists(segmentChunk.getName());
                Assert.assertEquals("Expected SegmentChunk deletion status for " + segmentChunk + ", truncation offset = " + truncateOffset,
                        expectedExists, existsInStorage);
                if (!expectedExists) {
                    AssertExtensions.assertThrows(
                            "Not expecting a read from a truncated SegmentChunk to work.",
                            () -> s.read(readHandle, segmentChunk.getLastOffset() - 1, new byte[1], 0, 1),
                            ex -> ex instanceof StreamSegmentTruncatedException);
                }
            }

            // Increment truncateOffset by some value, but let's make sure we also truncate at the very end of the Segment.
            if (truncateOffset >= writtenData.length) {
                break;
            }

            truncateOffset = (int) Math.min(writtenData.length, truncateOffset + DEFAULT_ROLLING_POLICY.getMaxLength() / 2);
        }
    }

    private void checkConcatResult(RollingStorage s, RollingSegmentHandle targetHandle, String sourceSegmentName, int expectedChunkCount, int expectedLength) throws Exception {
        Assert.assertFalse("Expecting the source segment to not exist anymore.", s.exists(sourceSegmentName));
        Assert.assertEquals("Unexpected number of SegmentChunks in target.", expectedChunkCount, targetHandle.chunks().size());
        Assert.assertEquals("Unexpected target length.", expectedLength, targetHandle.length());

        // Reload the handle and verify nothing strange happened in Storage.
        val targetHandle2 = (RollingSegmentHandle) s.openWrite(SEGMENT_NAME);
        Assert.assertEquals("Unexpected number of SegmentChunks in reloaded target handle.", expectedChunkCount, targetHandle2.chunks().size());
        Assert.assertEquals("Unexpected reloaded target length.", targetHandle.length(), targetHandle2.length());
    }

    private void checkWrittenData(byte[] writtenData, SegmentHandle readHandle, RollingStorage s) throws StreamSegmentException {
        checkWrittenData(writtenData, 0, readHandle, s);
    }

    private void checkWrittenData(byte[] writtenData, int offset, SegmentHandle readHandle, RollingStorage s) throws StreamSegmentException {
        byte[] readBuffer = new byte[writtenData.length - offset];
        if (readBuffer.length == 0) {
            // Nothing to check.
            return;
        }

        int bytesRead = s.read(readHandle, offset, readBuffer, 0, readBuffer.length);
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);
        AssertExtensions.assertArrayEquals("Unexpected data read back.", writtenData, offset, readBuffer, 0, readBuffer.length);
    }

    //endregion

    //region TestStorage

    private static class TestStorage extends InMemoryStorage {
        private Function<String, IntentionalException> writeFailure;
        private Function<String, IntentionalException> deleteFailure;
        private Function<String, IntentionalException> concatFailure;

        @Override
        public void delete(SegmentHandle handle) throws StreamSegmentNotExistsException {
            maybeThrow(handle.getSegmentName(), this.deleteFailure);
            super.delete(handle);
        }

        @Override
        public void write(SegmentHandle handle, long offset, InputStream data, int length) throws BadOffsetException, StreamSegmentNotExistsException,
                StreamSegmentSealedException {
            maybeThrow(handle.getSegmentName(), this.writeFailure);
            super.write(handle, offset, data, length);
        }

        @Override
        public void concat(SegmentHandle targetHandle, long offset, String sourceSegment) throws StreamSegmentException {
            maybeThrow(sourceSegment, this.concatFailure);
            super.concat(targetHandle, offset, sourceSegment);
        }

        private void maybeThrow(String segmentName, Function<String, IntentionalException> exceptionFunction) {
            IntentionalException toThrow;
            if (exceptionFunction != null && (toThrow = exceptionFunction.apply(segmentName)) != null) {
                throw toThrow;
            }
        }

        @Override
        public boolean supportsReplace() {
            return true;
        }

        @Override
        public void replace(@NonNull SegmentHandle segment, @NonNull BufferView contents) throws StreamSegmentException {
            // Delete existing segment.
            boolean sealed = getStreamSegmentInfo(segment.getSegmentName()).isSealed();
            super.delete(segment);

            // Create a new one.
            segment = super.create(segment.getSegmentName());
            super.write(segment, 0, contents.getReader(), contents.getLength());
            if (sealed) {
                super.seal(segment);
            }
        }

        @Override
        public SyncStorage withReplaceSupport() {
            return this;
        }
    }

    //endregion
}
