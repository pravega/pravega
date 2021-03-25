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
package io.pravega.storage.hdfs;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for HDFSStorage.
 */
public class HDFSStorageTest extends StorageTestBase {
    private static final int WRITE_COUNT = 5;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private File baseDir = null;
    private MiniDFSCluster hdfsCluster = null;
    private HDFSStorageConfig adapterConfig;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(this.baseDir.getAbsolutePath());
        this.adapterConfig = HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()))
                .build();
    }

    @After
    public void tearDown() {
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
            hdfsCluster = null;
            FileHelpers.deleteFileOrDirectory(baseDir);
            baseDir = null;
        }
    }

    //region Fencing tests
    /**
     * A special test case of fencing to verify the behavior of HDFSStorage in the presence of an instance that has
     * been fenced out. This case verifies that any ongoing writes properly fail upon fencing. Specifically, we have a
     * fenced-out instance that keeps writing and we verify that the write fails once the ownership changes.
     * The HDFS behavior is such in this case is that ongoing writes that execute before the rename
     * complete successfully.
     */
    @Test(timeout = 60000)
    public void testZombieFencing() throws Exception {
        final long epochCount = 30;
        final int writeSize = 1000;
        final String segmentName = "Segment";
        @Cleanup
        val writtenData = new ByteBufferOutputStream();
        final Random rnd = new Random(0);
        int currentEpoch = 1;

        // Create initial adapter.
        val currentStorage = new AtomicReference<Storage>();
        currentStorage.set(createStorage());
        currentStorage.get().initialize(currentEpoch);

        // Create the Segment and open it for the first time.
        val currentHandle = new AtomicReference<SegmentHandle>(
                currentStorage.get().create(segmentName, TIMEOUT)
                              .thenCompose(v -> currentStorage.get().openWrite(segmentName))
                              .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        // Run a number of epochs.
        while (currentEpoch <= epochCount) {
            val oldStorage = currentStorage.get();
            val handle = currentHandle.get();
            val writeBuffer = new byte[writeSize];
            val appends = Futures.loop(
                    () -> true,
                    () -> {
                        rnd.nextBytes(writeBuffer);
                        return oldStorage.write(handle, writtenData.size(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT)
                                         .thenRun(() -> writtenData.write(writeBuffer));
                    },
                    executorService());

            // Create a new Storage adapter with a new epoch and open-write the Segment, remembering its handle.
            val newStorage = createStorage();
            try {
                newStorage.initialize(++currentEpoch);
                currentHandle.set(newStorage.openWrite(segmentName).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
            } catch (Exception ex) {
                newStorage.close();
                throw ex;
            }

            currentStorage.set(newStorage);
            try {
                appends.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.fail("Continuous appends on older epoch Adapter did not fail.");
            } catch (Exception ex) {
                val cause = Exceptions.unwrap(ex);
                if (!(cause instanceof StorageNotPrimaryException || cause instanceof StreamSegmentSealedException
                || cause instanceof StreamSegmentNotExistsException)) {
                    // We only expect the appends to fail because they were fenced out or the Segment was sealed.
                    Assert.fail("Unexpected exception " + cause);
                }
            } finally {
                oldStorage.close();
            }
        }

        byte[] expectedData = writtenData.getData().getCopy();
        byte[] readData = new byte[expectedData.length];
        @Cleanup
        val readStorage = createStorage();
        readStorage.initialize(++currentEpoch);
        int bytesRead = readStorage
                .openRead(segmentName)
                .thenCompose(handle -> readStorage.read(handle, 0, readData, 0, readData.length, TIMEOUT))
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of bytes read.", readData.length, bytesRead);
        Assert.assertArrayEquals("Unexpected data read back.", expectedData, readData);
    }

    /**
     * Tests fencing abilities. We create two different Storage objects with different owner ids.
     * Part 1: Creation:
     * * We create the Segment on Storage1:
     * ** We verify that Storage1 can execute all operations.
     * ** We verify that Storage2 can execute only read-only operations.
     * * We open the Segment on Storage2:
     * ** We verify that Storage1 can execute only read-only operations.
     * ** We verify that Storage2 can execute all operations.
     */
    @Test
    @Override
    public void testFencing() {
        final long epoch1 = 1;
        final long epoch2 = 2;
        final String segmentName = "segment";
        try (val storage1 = createStorage();
             val storage2 = createStorage()) {
            storage1.initialize(epoch1);
            storage2.initialize(epoch2);

            // Create segment in Storage1 (thus Storage1 owns it for now).
            storage1.create(segmentName, TIMEOUT).join();

            // Storage1 should be able to execute all operations.
            SegmentHandle handle1 = storage1.openWrite(segmentName).join();
            verifyWriteOperationsSucceed(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Open the segment in Storage2 (thus Storage2 owns it for now).
            SegmentHandle handle2 = storage2.openWrite(segmentName).join();

            // Storage1 should be able to execute only read-only operations.
            verifyWriteOperationsFail(handle1, storage1);
            verifyConcatOperationsFail(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Storage2 should be able to execute all operations.
            verifyReadOnlyOperationsSucceed(handle2, storage2);
            verifyWriteOperationsSucceed(handle2, storage2);

            // Seal and Delete (these should be run last, otherwise we can't run our test).
            verifyFinalWriteOperationsFail(handle1, storage1);
            verifyFinalWriteOperationsSucceed(handle2, storage2);
        }
    }

    //endregion

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new TestHDFSStorage(this.adapterConfig), executorService());
    }

    // region HDFS specific tests

    /**
     * Tests general GetInfoOperation behavior.
     */
    @Test
    public void testGetInfo() throws Exception {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            SegmentHandle handle = s.openWrite(segmentName).join();

            long expectedLength = 0;

            for (int i = 0; i < WRITE_COUNT; i++) {
                byte[] data = new byte[i + 1];
                s.write(handle, expectedLength, new ByteArrayInputStream(data), data.length, null).join();
                expectedLength += data.length;
            }

            SegmentProperties result = s.getStreamSegmentInfo(segmentName, null).join();

            validateProperties("pre-seal", segmentName, result, expectedLength, false);

            // Seal.
            s.seal(handle, null).join();
            result = s.getStreamSegmentInfo(segmentName, null).join();
            validateProperties("post-seal", segmentName, result, expectedLength, true);

            // Inexistent segment.
            AssertExtensions.assertFutureThrows(
                    "GetInfo succeeded on missing segment.",
                    s.getStreamSegmentInfo("non-existent", null),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    private void validateProperties(String stage, String segmentName, SegmentProperties sp, long expectedLength, boolean expectedSealed) {
        Assert.assertNotNull("No result from GetInfoOperation (" + stage + ").", sp);
        Assert.assertEquals("Unexpected name (" + stage + ").", segmentName, sp.getName());
        Assert.assertEquals("Unexpected length (" + stage + ").", expectedLength, sp.getLength());
        Assert.assertEquals("Unexpected sealed status (" + stage + ").", expectedSealed, sp.isSealed());
    }

    /**
     * Tests the exists API.
     */
    @Test
    public void testExists() throws Exception {
        final int epoch = 1;
        final int offset = 0;

        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);

            // Not exists.
            Assert.assertFalse("Unexpected result for missing segment (no files).", s.exists("nonexistent", null).join());
        }
    }

    /**
     * Tests a read scenario with no issues or failures.
     */
    @Test
    public void testNormalRead() throws Exception {
        // Write data.
        String segmentName = "foo_open";
        val rnd = new Random(0);
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            SegmentHandle handle = s.openWrite(segmentName).join();

            long expectedLength = 0;

            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            for (int i = 0; i < WRITE_COUNT; i++) {
                byte[] data = new byte[i + 1];
                rnd.nextBytes(data);
                s.write(handle, expectedLength, new ByteArrayInputStream(data), data.length, null).join();
                writtenData.write(data);
                expectedLength += data.length;
            }

            // Check written data via a Read Operation, from every offset from 0 to length/2
            byte[] expectedData = writtenData.toByteArray();
            val readHandle = s.openRead(segmentName).join();
            for (int startOffset = 0; startOffset < expectedLength / 2; startOffset++) {
                int readLength = (int) (expectedLength - 2 * startOffset);
                byte[] actualData = new byte[readLength];
                int readBytes = s.read(readHandle, startOffset, actualData, 0, actualData.length, null).join();

                Assert.assertEquals("Unexpected number of bytes read with start offset " + startOffset, actualData.length, readBytes);
                AssertExtensions.assertArrayEquals("Unexpected data read back with start offset " + startOffset,
                        expectedData, startOffset, actualData, 0, readLength);
            }
        }
    }

    /**
     * Tests the case when the segment ownership changes while the read operation is on.
     */
    @Test
    public void testRefreshHandleOffset() throws Exception {
        String segmentName = "foo_open";
        val rnd = new Random(0);
        try (Storage s = createStorage();
        Storage s2 = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            s2.initialize(DEFAULT_EPOCH + 1);

            createSegment(segmentName, s);
            SegmentHandle handle = s.openWrite(segmentName).join();

            long expectedLength = 0;

            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            for (int i = 0; i < WRITE_COUNT; i++) {
                byte[] data = new byte[i + 1];
                rnd.nextBytes(data);
                s.write(handle, expectedLength, new ByteArrayInputStream(data), data.length, null).join();
                writtenData.write(data);
                expectedLength += data.length;
            }

            // Check written data via a Read Operation, from every offset from 0 to length/2
            byte[] expectedData = writtenData.toByteArray();
            val readHandle = s.openRead(segmentName).join();
            for (int startOffset = 0; startOffset < expectedLength / 2; startOffset++) {
                int readLength = (int) (expectedLength - 2 * startOffset);
                byte[] actualData = new byte[readLength];
                int readBytes = s.read(readHandle, startOffset, actualData, 0, actualData.length, null).join();

                Assert.assertEquals("Unexpected number of bytes read with start offset " + startOffset, actualData.length, readBytes);
                AssertExtensions.assertArrayEquals("Unexpected data read back with start offset " + startOffset,
                        expectedData, startOffset, actualData, 0, readLength);
                //Change ownership of the segment
                s2.openWrite(segmentName).join();
            }
        }
    }

    // endregion

    //region RollingStorageTests

    /**
     * Tests the HDFSStorage adapter with a RollingStorage wrapper.
     */
    public static class RollingStorageTests extends RollingStorageTestBase {
        @Rule
        public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
        private File baseDir = null;
        private MiniDFSCluster hdfsCluster = null;
        private HDFSStorageConfig adapterConfig;

        @Before
        public void setUp() throws Exception {
            this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(this.baseDir.getAbsolutePath());
            this.adapterConfig = HDFSStorageConfig
                    .builder()
                    .with(HDFSStorageConfig.REPLICATION, 1)
                    .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort()))
                    .build();
        }

        @After
        public void tearDown() {
            if (hdfsCluster != null) {
                hdfsCluster.shutdown();
                hdfsCluster = null;
                FileHelpers.deleteFileOrDirectory(baseDir);
                baseDir = null;
            }
        }

        @Override
        protected Storage createStorage() {
            return wrap(new TestHDFSStorage(this.adapterConfig));
        }

        @Override
        protected long getSegmentRollingSize() {
            // Need to increase this otherwise the test will run for too long.
            return DEFAULT_ROLLING_SIZE * 5;
        }
    }

    //endregion

    //region TestHDFSStorage

    /**
     * Special HDFSStorage that uses a modified version of the MiniHDFSCluster DistributedFileSystem which fixes the
     * 'read-only' permission issues observed with that one.
     **/
    private static class TestHDFSStorage extends HDFSStorage {
        TestHDFSStorage(HDFSStorageConfig config) {
            super(config);
        }

        @Override
        protected FileSystem openFileSystem(Configuration conf) throws IOException {
            return new FileSystemFixer(conf);
        }
    }

    private static class FileSystemFixer extends DistributedFileSystem {
        @SneakyThrows(IOException.class)
        FileSystemFixer(Configuration conf) {
            initialize(getDefaultUri(conf), conf);
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
            if (getFileStatus(f).getPermission().getUserAction() == FsAction.READ) {
                throw new AclException(f.getName());
            }

            return super.append(f, bufferSize, progress);
        }

        @Override
        public void concat(Path targetPath, Path[] sourcePaths) throws IOException {
            if (getFileStatus(targetPath).getPermission().getUserAction() == FsAction.READ) {
                throw new AclException(targetPath.getName());
            }

            super.concat(targetPath, sourcePaths);
        }
    }

    //endregion
}
