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
import com.emc.logservice.common.FutureHelpers;
import com.emc.logservice.common.StreamHelpers;
import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryContents;
import com.emc.logservice.server.Cache;
import com.emc.logservice.server.DataCorruptionException;
import com.emc.logservice.server.MetadataRepository;
import com.emc.logservice.server.SegmentContainer;
import com.emc.logservice.server.SegmentContainerFactory;
import com.emc.logservice.server.SegmentMetadata;
import com.emc.logservice.server.UpdateableContainerMetadata;
import com.emc.logservice.server.UpdateableSegmentMetadata;
import com.emc.logservice.server.containers.StreamSegmentContainerFactory;
import com.emc.logservice.server.containers.StreamSegmentContainerMetadata;
import com.emc.logservice.server.containers.StreamSegmentMapper;
import com.emc.logservice.server.containers.TruncationMarkerCollection;
import com.emc.logservice.server.logs.DurableLog;
import com.emc.logservice.server.logs.DurableLogFactory;
import com.emc.logservice.server.logs.MemoryLogUpdater;
import com.emc.logservice.server.logs.MemoryOperationLog;
import com.emc.logservice.server.logs.OperationMetadataUpdater;
import com.emc.logservice.server.logs.OperationProcessor;
import com.emc.logservice.server.logs.operations.BatchMapOperation;
import com.emc.logservice.server.logs.operations.MergeBatchOperation;
import com.emc.logservice.server.logs.operations.MetadataOperation;
import com.emc.logservice.server.logs.operations.MetadataPersistedOperation;
import com.emc.logservice.server.logs.operations.Operation;
import com.emc.logservice.server.logs.operations.StorageOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentMapOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentSealOperation;
import com.emc.logservice.server.mocks.InMemoryMetadataRepository;
import com.emc.logservice.server.reading.ReadIndex;
import com.emc.logservice.server.reading.ReadIndexFactory;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.storageabstraction.Storage;
import com.emc.logservice.storageabstraction.mocks.InMemoryDurableDataLogFactory;
import com.emc.logservice.storageabstraction.mocks.InMemoryStorage;
import com.emc.logservice.storageabstraction.mocks.InMemoryStorageFactory;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Playground Test class.
 */
public class Playground {
    private static final Random RANDOM = new Random();
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String CONTAINER_ID = "123";

    public static void main(String[] args) throws Exception {
        // testService();

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        //context.getLoggerList().get(0).setLevel(Level.INFO);
        context.reset();

        //testStreamSegmentContainer();
        testDurableLog();
        testReadIndex();
        testOperationQueueProcessor();
    }

    private static void testService() throws Exception {
        ExecutorService e = Executors.newFixedThreadPool(10);
        System.out.println("SINGLE THREAD");
        SingleThreadTestService s = new SingleThreadTestService();
        s.addListener(new TestServiceListener(), e);
        s.startAsync().awaitRunning();
        s.awaitTerminated();

        System.out.println("GENERAL");
        GeneralTestService s2 = new GeneralTestService();
        s2.addListener(new TestServiceListener(), e);
        s2.startAsync().awaitRunning();
        Thread.sleep(500);
        s2.stopAsync().awaitTerminated();

        e.shutdownNow();
    }

    private static class GeneralTestService extends AbstractService {
        @Override
        protected void doStart() {
            System.out.println("GeneralTestService.doStart");
            this.notifyStarted();
        }

        @Override
        protected void doStop() {
            System.out.println("GeneralTestService.doStop");
            this.notifyStopped();
        }
    }

    private static class SingleThreadTestService extends AbstractExecutionThreadService {
        @Override
        protected void startUp() throws Exception {
            System.out.println("SingleThreadTestService.startUp");
        }

        @Override
        protected void shutDown() throws Exception {
            System.out.println("SingleThreadTestService.shutDown");
        }

        @Override
        protected void run() throws Exception {
            System.out.println("SingleThreadTestService.run.enter");
            Thread.sleep(500);
            System.out.println("SingleThreadTestService.run.exit");
        }
    }

    private static class TestServiceListener extends Service.Listener {
        @Override
        public void starting() {
            System.out.println("Monitor: Service Started");
        }

        @Override
        public void running() {
            System.out.println("Monitor: Service Running");
        }

        @Override
        public void stopping(Service.State from) {
            System.out.println("Monitor: Service Stopping (from " + from + ")");
        }

        @Override
        public void terminated(Service.State from) {
            System.out.println("Monitor: Service Terminated (from " + from + ")");
        }

        @Override
        public void failed(Service.State from, Throwable failure) {
            System.out.println("Monitor: Service Failed (from " + from + ") with exception " + failure.toString());
        }
    }

    private static void testStreamSegmentContainer() throws Exception {
        final String containerId = "123";
        final Duration timeout = Duration.ofSeconds(30);

        int streamCount = 10;
        int batchPerStreamCount = 10;

        ExecutorService es = Executors.newScheduledThreadPool(10);
        try {
            MetadataRepository metadataRepository = new InMemoryMetadataRepository();
            SegmentContainerFactory containerFactory = new StreamSegmentContainerFactory(metadataRepository, new DurableLogFactory(new InMemoryDurableDataLogFactory(), es), new ReadIndexFactory(), new InMemoryStorageFactory(), es);
            List<String> streamNames = new ArrayList<>();
            List<CompletableFuture> batchNameFutures = new ArrayList<>();
            try (SegmentContainer c = containerFactory.createStreamSegmentContainer(containerId)) {
                c.startAsync().awaitRunning();

                //create some streams
                for (int i = 0; i < streamCount; i++) {
                    String name = getStreamName(i);
                    streamNames.add(name);
                    c.createStreamSegment(name, timeout).get();
                    for (int j = 0; j < batchPerStreamCount; j++) {
                        batchNameFutures.add(c.createBatch(name, timeout));
                    }
                }

                for (CompletableFuture<String> cf : batchNameFutures) {
                    streamNames.add(cf.get());
                }

                // more tests to be done as part of unit testing.

                printMetadata(metadataRepository.getMetadata(containerId), streamNames);

                c.stopAsync().awaitTerminated();
            }
        } finally {
            es.shutdown();
        }
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

    private static void testDurableLog() throws Exception {
        boolean isSynchronousAppend = false;
        int maxAppendLength = 64 * 1024;
        int streamCount = 50;
        int appendsPerStream = 500;
        int clientCount = 7;
        boolean sealAllStreams = true;

        ExecutorService es = Executors.newScheduledThreadPool(10);
        try {
            // Write a bunch of entries to DurableLog.

            ArrayList<String> streamNames = new ArrayList<>();
            ArrayList<Operation> writeEntries = new ArrayList<>();
            HashMap<Long, ArrayList<StreamSegmentAppendOperation>> appendsByStream = new HashMap<>();
            ConcurrentHashMap<Long, Long> latencies = new ConcurrentHashMap<>();
            ArrayList<Operation> readEntries;
            ArrayList<Operation> readEntriesAfterRecovery;
            long createStreamsElapsedNanos;
            long totalAppendSize = 0;
            long writeTimeMillis;
            long readElapsedMillis;

            ArrayList<UUID> clients = generateClientIds(clientCount);
            Storage storage = new InMemoryStorage();
            StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            ReadIndex readIndex = new ReadIndex(metadata, CONTAINER_ID);
            DurableDataLogFactory dataLogFactory = new InMemoryDurableDataLogFactory();
            try (DurableLog dl = new DurableLog(metadata, dataLogFactory, readIndex, es)) {
                StreamSegmentMapper streamSegmentMapper = new StreamSegmentMapper(metadata, dl, storage);

                dl.startAsync().awaitRunning();

                // Map the streams.
                System.out.println("Creating streams ...");
                long createStreamsStartNanos = System.nanoTime();
                for (long streamId = 0; streamId < streamCount; streamId++) {
                    String name = getStreamName((int) streamId);
                    streamSegmentMapper.createNewStreamSegment(name, TIMEOUT).get();
                    streamNames.add(name);
                }

                createStreamsElapsedNanos = System.nanoTime() - createStreamsStartNanos;

                System.out.println("Generating entries ...");
                for (int i = 0; i < appendsPerStream; i++) {
                    for (String streamName : streamNames) {
                        long streamId = metadata.getStreamSegmentId(streamName);
                        byte[] appendData = getAppendData(maxAppendLength);
                        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(streamId, appendData, getAppendContext(clients, (int) totalAppendSize));
                        writeEntries.add(op);
                        ArrayList<StreamSegmentAppendOperation> opList = appendsByStream.getOrDefault(streamId, null);
                        if (opList == null) {
                            opList = new ArrayList<>();
                            appendsByStream.put(streamId, opList);
                        }

                        opList.add(op);
                        totalAppendSize += appendData.length;
                    }
                }

                if (sealAllStreams) {
                    for (String streamName : streamNames) {
                        long streamId = metadata.getStreamSegmentId(streamName);
                        writeEntries.add(new StreamSegmentSealOperation(streamId));
                    }
                }

                //Add some appends
                System.out.println("Queuing entries ...");
                long writeStartNanos = System.nanoTime();
                ArrayList<CompletableFuture<Long>> entryResults = new ArrayList<>();
                for (Operation entry : writeEntries) {
                    long startNanos = System.nanoTime();
                    CompletableFuture<Long> resultFuture = dl.add(entry, TIMEOUT);
                    resultFuture.thenAcceptAsync(seqNo -> latencies.put(seqNo, System.nanoTime() - startNanos));
                    entryResults.add(resultFuture);
                    if (isSynchronousAppend) {
                        resultFuture.get();
                    }
                }

                // Wait for all the entries to complete...and there must be a more elegant way of doing this...
                for (CompletableFuture<Long> er : entryResults) {
                    er.get();
                }

                writeTimeMillis = (System.nanoTime() - writeStartNanos) / 1000 / 1000;
                System.out.println("Finished producing.");

                // Order write entries by seq no (this is the order in which they were processed).
                sortOperationList(writeEntries);
                appendsByStream.values().forEach(Playground::sortOperationList);

                // Read from DurableLog.
                System.out.println("Reading entries from DurableLog ...");
                long readStartNanos = System.nanoTime();
                readEntries = readDurableLog(dl);

                readElapsedMillis = (System.nanoTime() - readStartNanos) / 1000 / 1000;
                System.out.println("Finished reading.");

                // Check readDurableLog result
                if (!checkAndPrintComparison(writeEntries, 0, readEntries, streamCount)) {
                    return;
                }

                System.out.println("DurableLog Read check complete.");

                System.out.println("Reading entries from ReadIndex ...");
                checkReadIndex(readIndex, appendsByStream);
                System.out.println("Read index check complete.");

                // Close DurableLog and create a new one (recover)
                dl.stopAsync().awaitTerminated();
            }

            System.out.println("Performing recovery ...");
            long recoveryStartNanos = System.nanoTime();
            long recoveryElapsedMillis;
            try (DurableLog dl = new DurableLog(metadata, dataLogFactory, readIndex, es)) {
                dl.startAsync().awaitRunning();

                recoveryElapsedMillis = (System.nanoTime() - recoveryStartNanos) / 1000 / 1000;
                System.out.println("Finished recovery.");

                // Read from DurableLog.
                readEntriesAfterRecovery = readDurableLog(dl);
                if (!checkAndPrintComparison(readEntries, 0, readEntriesAfterRecovery, 0)) {
                    dl.close();
                    return;
                }

                System.out.println("DurableLog Read (post recovery) check complete.");
                checkReadIndex(readIndex, appendsByStream);
                System.out.println("Read index (post recovery) check complete.");

                dl.stopAsync().awaitTerminated();
            }

            printMetadata(metadata, streamNames, clients);

            double writeOpsPerSecond = writeEntries.size() / (writeTimeMillis / 1000.0);
            double writeKbPerSecond = (totalAppendSize / 1024.0) / (writeTimeMillis / 1000.0);
            System.out.println();
            System.out.println(String.format("Elapsed time: CreateStreams = %d ms, Write = %d ms, %f ops/s, %f KB/s, Read = %d ms, Recovery = %d ms",
                    createStreamsElapsedNanos / 1000 / 1000,
                    writeTimeMillis,
                    writeOpsPerSecond,
                    writeKbPerSecond,
                    readElapsedMillis,
                    recoveryElapsedMillis));

            System.out.println();
            long max = Long.MIN_VALUE;
            long min = Long.MAX_VALUE;
            long sum = 0;
            for (long l : latencies.values()) {
                sum += l;
                max = Math.max(max, l);
                min = Math.min(min, l);
            }

            System.out.println(String.format("Operation latencies: Count = %d, Avg = %f, Min = %d, Max = %d", latencies.size(), sum / latencies.size() / 1000 / 1000.0, min / 1000 / 1000, max / 1000 / 1000));
        } finally {
            es.shutdownNow();
        }
    }

    private static void testOperationQueueProcessor() throws Exception {
        int maxAppendLength = 64 * 1024;
        int streamCount = 50;
        int appendsPerStream = 1000;
        int clientCount = 7;
        boolean sealAllStreams = true;

        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
        InMemoryDurableDataLogFactory dataLogFactory = new InMemoryDurableDataLogFactory();
        DurableDataLog dataLog = dataLogFactory.createDurableDataLog(CONTAINER_ID);
        dataLog.initialize(TIMEOUT).join();
        TruncationMarkerCollection truncationMarkerCollection = new TruncationMarkerCollection();
        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(metadata, truncationMarkerCollection);
        MemoryLogUpdater logUpdater = new MemoryLogUpdater(new MemoryOperationLog(), new ReadIndex(metadata, CONTAINER_ID));
        OperationProcessor qp = new OperationProcessor(CONTAINER_ID, metadataUpdater, logUpdater, dataLog);
        qp.startAsync().awaitRunning();

        // Map the streams.
        System.out.println("Creating streams ...");
        long createStreamsStartNanos = System.nanoTime();
        ArrayList<String> streamNames = new ArrayList<>();
        for (long streamId = 0; streamId < streamCount; streamId++) {
            String name = getStreamName((int) streamId);
            streamNames.add(name);
            metadata.mapStreamSegmentId(name, streamId);
            metadata.getStreamSegmentMetadata(streamId).setDurableLogLength(0);
            metadata.getStreamSegmentMetadata(streamId).setStorageLength(0);
        }

        long createStreamsElapsedNanos = System.nanoTime() - createStreamsStartNanos;

        System.out.println("Generating entries ...");
        ArrayList<UUID> clients = generateClientIds(clientCount);
        long totalAppendSize = 0;
        ArrayList<Operation> entries = new ArrayList<>();
        for (int i = 0; i < appendsPerStream; i++) {
            for (String streamName : streamNames) {
                long streamId = metadata.getStreamSegmentId(streamName);
                byte[] appendData = getAppendData(maxAppendLength);
                entries.add(new StreamSegmentAppendOperation(streamId, appendData, getAppendContext(clients, (int) totalAppendSize)));
                totalAppendSize += appendData.length;
            }
        }

        if (sealAllStreams) {
            for (String streamName : streamNames) {
                long streamId = metadata.getStreamSegmentId(streamName);
                entries.add(new StreamSegmentSealOperation(streamId));
            }
        }

        AtomicLong processingEndTimeNanos = new AtomicLong(0);
        //Add some appends
        System.out.println("Queuing entries ...");
        long produceStartNanos = System.nanoTime();
        for (Operation entry : entries) {
            CompletableFuture<Long> cf = qp.process(entry);
            cf.thenAcceptAsync(seqNo -> {
                if (seqNo >= entries.get(entries.size() - 1).getSequenceNumber()) {
                    processingEndTimeNanos.set(System.nanoTime());
                }
            });

            FutureHelpers.exceptionListener(cf, ex -> {
                System.err.println(String.format("Operation '%s' failed.", entry));
                System.err.println(ex);
            });
        }

        long producingElapsedNanos = System.nanoTime() - produceStartNanos;

        System.out.println("Finished producing.");

        Thread.sleep(2000);
        qp.stopAsync().awaitTerminated();
        System.out.println("QueueProcessor stopped.");

        printMetadata(metadata, streamNames);

        long processingTimeElapsedMillis = (processingEndTimeNanos.get() - produceStartNanos) / 1000 / 1000;
        double opsPerSecond = entries.size() / (processingTimeElapsedMillis / 1000.0);
        double kbPerSecond = (totalAppendSize / 1024.0) / (processingTimeElapsedMillis / 1000.0);
        System.out.println();
        System.out.println(String.format("Elapsed time: CreateStreams = %dms, Produce = %dms, Processing = %dms. OPS/sec = %f, KB/s = %f",
                createStreamsElapsedNanos / 1000 / 1000,
                producingElapsedNanos / 1000 / 1000,
                processingTimeElapsedMillis,
                opsPerSecond,
                kbPerSecond));
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

    private static ArrayList<Operation> readDurableLog(DurableLog dl) throws Exception {
        ArrayList<Operation> readEntries = new ArrayList<>();
        long lastReadSequence = -1;
        while (true) {
            Iterator<Operation> readResult = dl.read(lastReadSequence, 100, TIMEOUT).get();
            int readCount = 0;
            if (readResult != null) {
                while (readResult.hasNext()) {
                    Operation entry = readResult.next();
                    readEntries.add(entry);
                    lastReadSequence = entry.getSequenceNumber();
                    readCount++;
                }
            }

            if (readCount == 0) {
                break;
            }
        }

        return readEntries;
    }

    private static void checkReadIndex(ReadIndex readIndex, HashMap<Long, ArrayList<StreamSegmentAppendOperation>> appendsByStream) throws Exception {
        for (long streamId : appendsByStream.keySet()) {
            ArrayList<StreamSegmentAppendOperation> appends = appendsByStream.get(streamId);
            for (StreamSegmentAppendOperation append : appends) {
                ReadResult readResult = readIndex.read(streamId, append.getStreamSegmentOffset(), append.getData().length, Duration.ZERO);
                if (!readResult.hasNext()) {
                    System.out.println(String.format("Read check failed. StreamId = %d, Offset = %d, ReadLength = %d. No data returned by read index.", streamId, append.getStreamSegmentOffset(), append.getData().length));
                    break;
                }
                ReadResultEntry entry = readResult.next();
                if (entry.isEndOfStreamSegment()) {
                    System.out.println(String.format("Read check failed. StreamId = %d, Offset = %d, ReadLength = %d. Read Index indicates end of stream, but it shouldn't be.", streamId, append.getStreamSegmentOffset(), append.getData().length));
                    break;
                }
                if (!entry.getContent().isDone()) {
                    System.out.println(String.format("Read check failed. StreamId = %d, Offset = %d, ReadLength = %d. Read Index returned a non-completed entry, which is unexpected for a memory read.", streamId, append.getStreamSegmentOffset(), append.getData().length));
                    break;
                }

                ReadResultEntryContents entryContents = entry.getContent().get();
                byte[] readData = new byte[entryContents.getLength()];
                StreamHelpers.readAll(entryContents.getData(), readData, 0, readData.length);
                if (!areEqual(append.getData(), readData)) {
                    System.out.println(String.format("Read check failed. StreamId = %d, Offset = %d, ReadLength = %d. Unexpected result (Length = %d).", streamId, append.getStreamSegmentOffset(), append.getData().length, readData.length));
                    break;
                }
            }
        }
    }

    private static boolean checkAndPrintComparison(ArrayList<Operation> expected, int expectedOffset, ArrayList<Operation> actual, int actualOffset) throws Exception {
        // Check readDurableLog result
        if (expected.size() - expectedOffset != actual.size() - actualOffset) {
            System.out.println(String.format("Expected entry count != actual entry count. Expected %d, actual %d.", expected.size() - expectedOffset, actual.size() - actualOffset));
            return false;
        }

        int maxCount = Math.min(expected.size() - expectedOffset, actual.size() - actualOffset);
        for (int i = 0; i < maxCount; i++) {
            Operation e = expected.get(i + expectedOffset);
            Operation a = actual.get(i + actualOffset);
            if (!areEqual(e, a)) {
                System.out.println(String.format("Entry mismatch. Expected %s, actual %s.", expected, actual));
                return false;
            }
        }

        return true;
    }

    private static void printMetadata(UpdateableContainerMetadata metadata, Collection<String> streamNames) {
        printMetadata(metadata, streamNames, null);
    }

    private static void printMetadata(UpdateableContainerMetadata metadata, Collection<String> streamNames, Collection<UUID> clientIds) {
        System.out.println("Final Stream Metadata:");
        for (String streamName : streamNames) {
            long streamId = metadata.getStreamSegmentId(streamName);
            SegmentMetadata streamSegmentMetadata = metadata.getStreamSegmentMetadata(streamId);

            StringBuilder appendContexts = new StringBuilder();
            boolean anyContexts = false;
            if (clientIds != null && clientIds.size() > 0) {
                appendContexts.append(", AppendContexts: ");
                for (UUID clientId : clientIds) {
                    AppendContext context = streamSegmentMetadata.getLastAppendContext(clientId);
                    if (context != null) {
                        appendContexts.append(String.format("%s-%s = %d, ", Long.toHexString(clientId.getMostSignificantBits()), Long.toHexString(clientId.getLeastSignificantBits()), context.getClientOffset()));
                        anyContexts = true;
                    }
                }
            }

            if (!anyContexts) {
                appendContexts.append("None.");
            }

            System.out.println(String.format("Stream = %s, Name = %s, StorageLength = %d, DurableLogLength = %d, Sealed = %s%s",
                    Long.toHexString(streamSegmentMetadata.getId()),
                    streamSegmentMetadata.getName(),
                    streamSegmentMetadata.getStorageLength(),
                    streamSegmentMetadata.getDurableLogLength(),
                    streamSegmentMetadata.isSealed(),
                    appendContexts.toString()));
        }
    }

    private static String getStreamName(int index) {
        return "/foo/foo." + index + ".stream";
    }

    private static byte[] getAppendData(int maxAppendLength) {
        // TODO: try to use from the same buffer.
        byte[] b = new byte[Math.max(1, RANDOM.nextInt(maxAppendLength))];
        RANDOM.nextBytes(b);
        return b;
    }

    private static String getAppendString(byte[] data) {
        return new String(data);
    }

    private static <T extends Operation> void sortOperationList(ArrayList<T> operations) {
        operations.sort((o1, o2) -> (int) (o1.getSequenceNumber() - o2.getSequenceNumber()));
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

    private static boolean areEqual(Operation entry1, Operation entry2) throws Exception {
        if (!entry1.getClass().equals(entry2.getClass())) {
            return false;
        }

        if (entry1.getSequenceNumber() != entry2.getSequenceNumber()) {
            return false;
        }

        if (entry1 instanceof StorageOperation) {
            if (entry1 instanceof StreamSegmentSealOperation) {
                return areEqual((StreamSegmentSealOperation) entry1, (StreamSegmentSealOperation) entry2);
            } else if (entry1 instanceof StreamSegmentAppendOperation) {
                return areEqual((StreamSegmentAppendOperation) entry1, (StreamSegmentAppendOperation) entry2);
            } else if (entry1 instanceof MergeBatchOperation) {
                return areEqual((MergeBatchOperation) entry1, (MergeBatchOperation) entry2);
            }
        } else if (entry1 instanceof MetadataOperation) {
            if (entry1 instanceof MetadataPersistedOperation) {
                // nothing special here
                return true;
            } else if (entry1 instanceof StreamSegmentMapOperation) {
                return areEqual((StreamSegmentMapOperation) entry1, (StreamSegmentMapOperation) entry2);
            } else if (entry1 instanceof BatchMapOperation) {
                return areEqual((BatchMapOperation) entry1, (BatchMapOperation) entry2);
            }
        }

        return false;
    }

    private static boolean areEqual(StreamSegmentSealOperation e1, StreamSegmentSealOperation e2) {
        return e1.getStreamSegmentId() == e2.getStreamSegmentId()
                && e1.getStreamSegmentLength() == e2.getStreamSegmentLength();
    }

    private static boolean areEqual(StreamSegmentAppendOperation e1, StreamSegmentAppendOperation e2) {
        return e1.getStreamSegmentId() == e2.getStreamSegmentId()
                && e1.getStreamSegmentOffset() == e2.getStreamSegmentOffset()
                && areEqual(e1.getData(), e2.getData());
    }

    private static boolean areEqual(MergeBatchOperation e1, MergeBatchOperation e2) {
        return e1.getBatchStreamSegmentId() == e2.getBatchStreamSegmentId()
                && e1.getBatchStreamSegmentLength() == e2.getBatchStreamSegmentLength()
                && e1.getStreamSegmentId() == e2.getStreamSegmentId()
                && e1.getTargetStreamSegmentOffset() == e2.getTargetStreamSegmentOffset();
    }

    private static boolean areEqual(StreamSegmentMapOperation e1, StreamSegmentMapOperation e2) {
        return e1.getStreamSegmentId() == e2.getStreamSegmentId()
                && e1.getStreamSegmentLength() == e2.getStreamSegmentLength()
                && e1.getStreamSegmentName().equals(e2.getStreamSegmentName());
    }

    private static boolean areEqual(BatchMapOperation e1, BatchMapOperation e2) {
        return e1.getBatchStreamSegmentId() == e1.getBatchStreamSegmentId()
                && e1.getBatchStreamSegmentName().equals(e2.getBatchStreamSegmentName())
                && e1.getParentStreamSegmentId() == e2.getParentStreamSegmentId();
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
