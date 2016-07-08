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

package com.emc.logservice.serverhost;

import ch.qos.logback.classic.LoggerContext;
import com.emc.logservice.common.StreamHelpers;
import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryContents;
import com.emc.logservice.server.Cache;
import com.emc.logservice.server.DataCorruptionException;
import com.emc.logservice.server.UpdateableContainerMetadata;
import com.emc.logservice.server.UpdateableSegmentMetadata;
import com.emc.logservice.server.containers.StreamSegmentContainerMetadata;
import com.emc.logservice.server.logs.MemoryLogUpdater;
import com.emc.logservice.server.logs.MemoryOperationLog;
import com.emc.logservice.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.logservice.server.reading.ReadIndex;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Playground Test class.
 */
public class Playground {
    private static final Random RANDOM = new Random();
    private static final String CONTAINER_ID = "123";

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        //context.getLoggerList().get(0).setLevel(Level.INFO);
        context.reset();

        //testReadIndex();
    }

    private static void testReadIndex() throws Exception {
        boolean verbose = false;
        int streamCount = 50;
        int appendsPerStream = 200;

        ExecutorService executor = Executors.newFixedThreadPool(streamCount * 2 + 1);
        try {
            UpdateableContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            Cache index = new ReadIndex(metadata, CONTAINER_ID);
            index.enterRecoveryMode(metadata);
            index.exitRecoveryMode(metadata, true);

            // Map Streams
            HashMap<Long, ArrayList<byte[]>> perStreamData = new HashMap<>();
            HashMap<Long, String> streamContents = new HashMap<>();
            for (long streamId = 0; streamId < streamCount; streamId++) {
                String name = getStreamName((int) streamId);
                metadata.mapStreamSegmentId(name, streamId);
                metadata.getStreamSegmentMetadata(streamId).setDurableLogLength(0);
                metadata.getStreamSegmentMetadata(streamId).setStorageLength(0);
                perStreamData.put(streamId, new ArrayList<>());
                streamContents.put(streamId, "");
            }

            System.out.println("Generating initial data ...");
            for (int i = 0; i < appendsPerStream; i++) {
                for (long streamId = 0; streamId < streamCount; streamId++) {
                    String appendContents = String.format("[0]Stream_%d_Append_%d.", streamId, i); // [0] means Generation 0.
                    byte[] appendData = appendContents.getBytes();
                    perStreamData.get(streamId).add(appendData);

                    UpdateableSegmentMetadata ssm = metadata.getStreamSegmentMetadata(streamId);
                    long appendOffset = ssm.getDurableLogLength();
                    ssm.setDurableLogLength(appendOffset + appendData.length);
                    index.append(streamId, appendOffset, appendData);
                    streamContents.put(streamId, streamContents.get(streamId) + appendContents);

                    if (verbose) {
                        System.out.println(String.format("Stream %d, Append %d: Offset = [%d], Contents = %s", streamId, i, appendOffset, getAppendString(appendData)));
                    }
                }
            }

            //region Initial Data Tests

            System.out.println("INITIAL DATA TESTS:");
            // Read each individual appends that were written. No read exceeds an append boundary.
            System.out.println("One append at a time ...");
            for (long streamId : perStreamData.keySet()) {
                ArrayList<byte[]> streamData = perStreamData.get(streamId);
                long offset = 0;
                for (byte[] expectedData : streamData) {
                    try (ReadResult readResult = index.read(streamId, offset, expectedData.length, Duration.ZERO)) {
                        while (readResult.hasNext()) {
                            ReadResultEntry entry = readResult.next();
                            ReadResultEntryContents contents = entry.getContent().get();
                            byte[] actualData = new byte[contents.getLength()];
                            StreamHelpers.readAll(contents.getData(), actualData, 0, actualData.length);

                            if (verbose) {
                                System.out.println(String.format("Read StreamId = %d, Offset=%d, Consumed=%d/%d. Entry: Offset = %d, Length = %d/%d, Data = %s",
                                        streamId,
                                        readResult.getStreamSegmentStartOffset(),
                                        readResult.getConsumedLength(),
                                        readResult.getMaxResultLength(),
                                        entry.getStreamSegmentOffset(),
                                        contents.getLength(),
                                        entry.getRequestedReadLength(),
                                        getAppendString(actualData)));
                            }

                            if (!areEqual(expectedData, actualData)) {
                                System.out.println(String.format("Read MISMATCH: StreamId = %d, Offset=%d, Consumed=%d/%d. Expected: Data = %s. Actual: Offset = %d, Length = %d/%d, Data = %s",
                                        streamId,
                                        readResult.getStreamSegmentStartOffset(),
                                        readResult.getConsumedLength(),
                                        readResult.getMaxResultLength(),
                                        getAppendString(expectedData),
                                        entry.getStreamSegmentOffset(),
                                        contents.getLength(),
                                        entry.getRequestedReadLength(),
                                        getAppendString(actualData)));
                            }
                        }
                    }

                    offset += expectedData.length;
                }
            }

            System.out.println("One append at a time check complete.");

            // Read the entire stream at once.
            System.out.println("All appends at the same time ...");
            for (long streamId : perStreamData.keySet()) {
                String expected = streamContents.get(streamId);
                int length = expected.length();
                checkStreamContentsFromReadIndex(streamId, 0, length, index, expected, verbose);
            }

            System.out.println("All appends at the same time check complete.");

            // Read at random offsets within the stream.
            System.out.println("Random offset reads  ...");
            for (long streamId : perStreamData.keySet()) {
                String totalExpected = streamContents.get(streamId);
                int length = totalExpected.length();

                for (int offset = 0; offset < length / 2; offset++) {
                    int readLength = length - 2 * offset; // We reduce by 1 at either ends.
                    checkStreamContentsFromReadIndex(streamId, offset, readLength, index, totalExpected, verbose);
                }
            }

            System.out.println("Random offset reads check complete.");

            //endregion

            //region Future Reads

            System.out.println("FUTURE READS:");
            HashSet<String> futureWrites = new HashSet<>();
            HashSet<String> futureReads = new HashSet<>();
            HashMap<Long, CompletableFuture<Void>> readers = new HashMap<>();
            for (long streamId : perStreamData.keySet()) {
                String expected = streamContents.get(streamId);
                int length = expected.length();

                CompletableFuture<Void> cf = CompletableFuture.runAsync(() ->
                {
                    try (ReadResult readResult = index.read(streamId, length, length, Duration.ofMinutes(1))) {
                        while (readResult.hasNext()) {
                            ReadResultEntry entry = readResult.next();
                            ReadResultEntryContents contents;
                            byte[] actualData;
                            try {
                                contents = entry.getContent().get();
                                actualData = new byte[contents.getLength()];
                                StreamHelpers.readAll(contents.getData(), actualData, 0, actualData.length);
                            } catch (Exception ex) {
                                System.err.println(ex);
                                return;
                            }

                            synchronized (futureReads) {
                                futureReads.add(String.format("%d_%d:%s,", streamId, entry.getStreamSegmentOffset(), getAppendString(actualData)));
                                if (verbose) {
                                    System.out.println(String.format("StreamId %d, Offset = %d, Consumed = %d/%d, Contents: Length = %d, Data = %s",
                                            streamId,
                                            entry.getStreamSegmentOffset(),
                                            readResult.getConsumedLength(),
                                            readResult.getMaxResultLength(),
                                            contents.getLength(),
                                            getAppendString(actualData)));
                                }
                            }
                        }
                    }
                }, executor);
                readers.put(streamId, cf);
            }

            // Write more data.
            CompletableFuture<Void> producer = CompletableFuture.runAsync(() ->
            {
                System.out.println("Generating more data.");
                ArrayList<UUID> clients = generateClientIds(1);
                MemoryOperationLog memorylog = new MemoryOperationLog();
                MemoryLogUpdater appender = new MemoryLogUpdater(memorylog, index);
                long seqNo = 1;
                for (int i = 0; i < appendsPerStream; i++) {
                    // Append a whole bunch of more appends
                    for (long streamId = 0; streamId < streamCount; streamId++) {
                        String appendContents = String.format("[1]Stream_%d_Append_%d.", streamId, i); // Generation [1].
                        byte[] appendData = appendContents.getBytes();
                        perStreamData.get(streamId).add(appendData);

                        UpdateableSegmentMetadata ssm = metadata.getStreamSegmentMetadata(streamId);
                        long appendOffset = ssm.getDurableLogLength();
                        ssm.setDurableLogLength(appendOffset + appendData.length);
                        try {
                            StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(streamId, appendData, getAppendContext(clients, (int) seqNo));
                            op.setStreamSegmentOffset(appendOffset);
                            op.setSequenceNumber(seqNo++);
                            appender.add(op);
                        } catch (DataCorruptionException ex) {
                            System.err.println(ex);
                            return;
                        }

                        appender.flush();
                        streamContents.put(streamId, streamContents.get(streamId) + appendContents);
                        futureWrites.add(String.format("%d_%d:%s,", streamId, appendOffset, appendContents));

                        if (verbose) {
                            System.out.println(String.format("Stream %d, Append %d: Offset = [%d], Contents = %s", streamId, i, appendOffset, getAppendString(appendData)));
                        }
                    }
                }
            }, executor);

            System.out.println("Waiting for future reads to complete.");
            for (long streamId : readers.keySet()) {
                CompletableFuture<Void> cf = readers.get(streamId);
                if (verbose) {
                    System.out.println("Waiting for stream " + streamId);
                }

                cf.get();
            }

            producer.get();

            if (futureWrites.size() != futureReads.size()) {
                System.out.println(String.format("Unexpected number of future reads. Expected %d, actual %d.", futureWrites.size(), futureReads.size()));
            } else {
                for (String write : futureWrites) {
                    if (!futureReads.contains(write)) {
                        System.out.println(String.format("Missing future write: '%s'.", write));
                    }
                }
            }

            System.out.println("Future read check complete.");

            //endregion

            //region Merging Streams

            System.out.println("ReadIndex.beginMerge ...");

            // Create a new batch stream
            long parentStreamId = 0;
            long batchStreamId = streamContents.size() + 1;
            String name = getStreamName((int) batchStreamId);
            metadata.mapStreamSegmentId(name, batchStreamId, parentStreamId);
            UpdateableSegmentMetadata batchMetadata = metadata.getStreamSegmentMetadata(batchStreamId);
            batchMetadata.setDurableLogLength(0);
            batchMetadata.setStorageLength(0);
            perStreamData.put(batchStreamId, new ArrayList<>());
            streamContents.put(batchStreamId, "");

            // Add data to it.
            for (int i = 0; i < appendsPerStream; i++) {
                String appendContents = String.format("[2]BatchStream_%d_Append_%d.", batchStreamId, i); // Generation 2.
                byte[] appendData = appendContents.getBytes();
                perStreamData.get(batchStreamId).add(appendData);

                long appendOffset = batchMetadata.getDurableLogLength();
                batchMetadata.setDurableLogLength(appendOffset + appendData.length);
                index.append(batchStreamId, appendOffset, appendData);
                streamContents.put(batchStreamId, streamContents.get(batchStreamId) + appendContents);

                if (verbose) {
                    System.out.println(String.format("BatchStream %d, Append %d: Offset = [%d], Contents = %s", batchStreamId, i, appendOffset, getAppendString(appendData)));
                }
            }

            metadata.getStreamSegmentMetadata(batchStreamId).markSealed();

            // Merge it.
            UpdateableSegmentMetadata parentMetadata = metadata.getStreamSegmentMetadata(parentStreamId);
            long targetOffset = parentMetadata.getDurableLogLength();
            long batchLength = batchMetadata.getDurableLogLength();
            long parentStreamLength = targetOffset + batchLength;
            parentMetadata.setDurableLogLength(parentStreamLength);
            index.beginMerge(parentStreamId, targetOffset, batchStreamId, batchLength);

            // Verify we can't read from it anymore.
            try {
                index.read(batchStreamId, 0, (int) batchLength, Duration.ZERO);
                System.out.println("ReadIndex allowed reading from a merged stream segment.");
                return;
            } catch (Exception ex) {
            }

            //Check read result.
            String expectedContentsAfterMerge = streamContents.get(parentStreamId).concat(streamContents.get(batchStreamId));
            checkStreamContentsFromReadIndex(parentStreamId, 0, (int) parentStreamLength, index, expectedContentsAfterMerge, verbose);
            System.out.println("ReadIndex.beginMerge check complete.");

            System.out.println("ReadIndex.completeMerge ...");
            batchMetadata.markDeleted(); // In order for completeMerge to work, the batch metadata needs to be deleted.

            // Append some extra data to the base stream. This way the merged batch will be somewhere in the middle.
            String newData = "new data appended";
            parentMetadata.setDurableLogLength(parentStreamLength + newData.length());
            index.append(parentStreamId, parentStreamLength, newData.getBytes());
            expectedContentsAfterMerge += newData;
            parentStreamLength += newData.length();
            index.completeMerge(parentStreamId, batchStreamId);

            //Check read result (again)
            checkStreamContentsFromReadIndex(parentStreamId, 0, (int) parentStreamLength, index, expectedContentsAfterMerge, verbose);

            System.out.println("ReadIndex.completeMerge check complete.");
            //endregion
        } finally {
            executor.shutdown();
        }
    }

    //region Helpers

    private static void checkStreamContentsFromReadIndex(long streamId, long offset, int length, Cache index, String expectedContents, boolean verbose) throws Exception {
        try (ReadResult readResult = index.read(streamId, offset, length, Duration.ZERO)) {
            byte[] actualData = new byte[(int) length];
            int readSoFar = 0;
            while (readResult.hasNext()) {
                ReadResultEntry entry = readResult.next();
                ReadResultEntryContents contents = entry.getContent().get();
                readSoFar += StreamHelpers.readAll(contents.getData(), actualData, readSoFar, actualData.length - readSoFar);
            }

            String actual = getAppendString(actualData);
            if (verbose) {
                System.out.println(String.format("Read StreamId = %d, Offset=%d, Consumed=%d/%d. Data = %s",
                        streamId,
                        readResult.getStreamSegmentStartOffset(),
                        readResult.getConsumedLength(),
                        readResult.getMaxResultLength(),
                        actual));
            }

            if (offset != 0 || offset + length != expectedContents.length()) {
                expectedContents = expectedContents.substring((int) offset, (int) offset + length);
            }

            if (!expectedContents.equals(actual)) {
                System.out.println(String.format("Read MISMATCH: StreamId = %d, Offset=%d, Consumed=%d/%d. Expected: %s. Actual: %s",
                        streamId,
                        readResult.getStreamSegmentStartOffset(),
                        readResult.getConsumedLength(),
                        readResult.getMaxResultLength(),
                        expectedContents,
                        actual));
            }
        }
    }

    private static String getStreamName(int index) {
        return "/foo/foo." + index + ".stream";
    }

    private static String getAppendString(byte[] data) {
        return new String(data);
    }

    private static ArrayList<UUID> generateClientIds(int count) {
        ArrayList<UUID> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(new UUID(RANDOM.nextLong(), RANDOM.nextLong()));
        }

        return result;
    }

    private static AppendContext getAppendContext(ArrayList<UUID> clientIds, int index) {
        UUID clientId = clientIds.get(index % clientIds.size());
        return new AppendContext(clientId, index);
    }

    private static boolean areEqual(byte[] b1, byte[] b2) {
        if (b1.length != b2.length) {
            System.out.println(String.format("L1=%d, L2=%d", b1.length, b2.length));
            return false;
        }

        for (int i = 0; i < b1.length; i++) {
            if (b1[i] != b2[i]) {
                System.out.println(String.format("b1[%d]=%d, b2[%d]=%d", i, b1[i], i, b2[i]));
                return false;
            }
        }

        return true;
    }

    //endregion
}
