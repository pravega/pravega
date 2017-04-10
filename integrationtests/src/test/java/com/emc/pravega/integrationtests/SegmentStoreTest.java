package com.emc.pravega.integrationtests;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.containers.ContainerConfig;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.ServiceConfig;
import com.emc.pravega.service.server.writer.WriterConfig;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.ThreadPooledTestSuite;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog
 */
public class SegmentStoreTest extends ThreadPooledTestSuite {
    private static final int THREADPOOL_SIZE = 20;
    private static final int SEGMENT_COUNT = 1;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final ServiceBuilderConfig BUILDER_CONFIG = ServiceBuilderConfig
            .builder()
            .include(ServiceConfig.builder()
                                  .with(ServiceConfig.CONTAINER_COUNT, 1))
            .include(ContainerConfig
                    .builder()
                    .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, ContainerConfig.MINIMUM_SEGMENT_METADATA_EXPIRATION_SECONDS))
            .include(DurableLogConfig
                    .builder()
                    .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
                    .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                    .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L))
            .include(ReadIndexConfig.builder()
                                    .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024)
                                    .with(ReadIndexConfig.CACHE_POLICY_MAX_SIZE, 64 * 1024 * 1024L)
                                    .with(ReadIndexConfig.CACHE_POLICY_MAX_TIME, 30 * 1000))
            .include(WriterConfig
                    .builder()
                    .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
                    .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
                    .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                    .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L))
            .build();

    //region Test Setup

    @Before
    public void setup() {

    }

    @After
    public void tearDown() {

    }

    @Override
    protected int getThreadPoolSize() {
        return THREADPOOL_SIZE;
    }

    //endregion

    @Test
    public void testWithDistributedLogAndHDFS() throws Exception {
        @Cleanup
        val builder = ServiceBuilder.newInMemoryBuilder(BUILDER_CONFIG, executorService());
        builder.initialize().join();
        val segmentStore = builder.createStreamSegmentService();
        //TODO: setup HDFS and DL

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(segmentStore);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, i);
                opFutures.add(segmentStore.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
            }
        }

        FutureHelpers.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. getSegmentInfo
        for (String segmentName : segmentNames) {
            SegmentProperties sp = segmentStore.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            long expectedLength = lengths.get(segmentName);

            Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, sp.getLength());
            Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
            Assert.assertFalse("Unexpected value for isSealed for segment " + segmentName, sp.isDeleted());
         }

        // 4. Reads (regular reads, not tail reads).
        checkReads(segmentContents, lengths, segmentStore);

        // 5. Writer moving data to Storage.
//        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
//        checkStorage(segmentContents, lengths, context);
    }


    private ArrayList<String> createSegments(StreamSegmentStore store) {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            futures.add(store.createStreamSegment(segmentName, null, TIMEOUT));
        }

        FutureHelpers.allOf(futures).join();
        return segmentNames;
    }

    private byte[] getAppendData(String segmentName, int appendId) {
        return String.format("%s_%d", segmentName, appendId).getBytes();
    }

    private void recordAppend(String segmentName, byte[] data, HashMap<String, ByteArrayOutputStream> segmentContents) throws Exception {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentName, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentName, contents);
        }

        contents.write(data);
    }

    private static String getSegmentName(int i) {
        return "Segment_" + i;
    }

    private void checkReads(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths, StreamSegmentStore store) throws Exception {
        for (String segmentName : segmentContents.keySet()) {
            long expectedLength = lengths.get(segmentName);
            long segmentLength = store.getStreamSegmentInfo(segmentName, false, TIMEOUT).join().getLength();

            Assert.assertEquals("Unexpected Read Index length for segment " + segmentName, expectedLength, segmentLength);
            byte[] expectedData = segmentContents.get(segmentName).toByteArray();

            long expectedCurrentOffset = 0;
            @Cleanup
            ReadResult readResult = store.read(segmentName, expectedCurrentOffset, (int) segmentLength, TIMEOUT).join();
            Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());

            // A more thorough read check is done in testSegmentRegularOperations; here we just check if the data was merged correctly.
            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                AssertExtensions.assertGreaterThan("getRequestedReadLength should be a positive integer for segment " + segmentName, 0, readEntry.getRequestedReadLength());
                Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName, expectedCurrentOffset, readEntry.getStreamSegmentOffset());
                Assert.assertTrue("getContent() did not return a completed future for segment" + segmentName, readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

                ReadResultEntryContents readEntryContents = readEntry.getContent().join();
                byte[] actualData = new byte[readEntryContents.getLength()];
                StreamHelpers.readAll(readEntryContents.getData(), actualData, 0, actualData.length);
                AssertExtensions.assertArrayEquals("Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset, expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());
                expectedCurrentOffset += readEntryContents.getLength();
            }

            Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
        }
    }

}
