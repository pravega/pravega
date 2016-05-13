package com.emc.logservice;

import com.emc.logservice.core.*;
import com.emc.logservice.logs.*;
import com.emc.logservice.logs.operations.*;
import com.emc.logservice.reading.ReadIndex;
import com.emc.logservice.mocks.InMemoryDataFrameLog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Main Test class.
 */
public class Main {
    private static final Random Random = new Random();
    private static final Duration Timeout = Duration.ofSeconds(30);

    public static void main(String[] args) throws Exception {
        //testDurableLog();
        testReadIndex();
        //testOperationQueueProcessor();
        //testBlockingDrainingQueue();
        //testDataFrameBuilder();
        //testDataFrame();
        //testLogOperations();
    }

    private static void testReadIndex() throws Exception {
        boolean verbose = false;
        int streamCount = 50;
        int appendsPerStream = 100;

        ExecutorService executor = Executors.newFixedThreadPool(streamCount * 2 + 1);
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata();
        ReadIndex index = new ReadIndex(metadata);
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

                StreamSegmentMetadata ssm = metadata.getStreamSegmentMetadata(streamId);
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
        // Read each individual appends that were written. No read exceeds an add boundary.
        System.out.println("One add at a time ...");
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

        System.out.println("One add at a time check complete.");

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
                        }
                        catch (Exception ex) {
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
            MemoryOperationLog memorylog = new MemoryOperationLog();
            MemoryLogUpdater appender = new MemoryLogUpdater(memorylog, index);
            long seqNo = 1;
            for (int i = 0; i < appendsPerStream; i++) {
                // Append a whole bunch of more appends
                for (long streamId = 0; streamId < streamCount; streamId++) {
                    String appendContents = String.format("[1]Stream_%d_Append_%d.", streamId, i); // Generation [1].
                    byte[] appendData = appendContents.getBytes();
                    perStreamData.get(streamId).add(appendData);

                    StreamSegmentMetadata ssm = metadata.getStreamSegmentMetadata(streamId);
                    long appendOffset = ssm.getDurableLogLength();
                    ssm.setDurableLogLength(appendOffset + appendData.length);
                    try {
                        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(streamId, appendData);
                        op.setStreamSegmentOffset(appendOffset);
                        op.setSequenceNumber(seqNo++);
                        appender.add(op);
                    }
                    catch (DataCorruptionException ex) {
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
        }
        else {
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
        StreamSegmentMetadata batchMetadata = metadata.getStreamSegmentMetadata(batchStreamId);
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
        StreamSegmentMetadata parentMetadata = metadata.getStreamSegmentMetadata(parentStreamId);
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
        }
        catch (Exception ex) {
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

        executor.shutdown();
    }

    private static void testDurableLog() throws Exception {
        // Write a bunch of entries to DurableLog.
        boolean isSynchronousAppend = false;
        int maxAppendLength = 64 * 1024;
        int streamCount = 50;
        int appendsPerStream = 500;
        boolean sealAllStreams = true;

        ArrayList<String> streamNames = new ArrayList<>();
        ArrayList<Operation> writeEntries = new ArrayList<>();
        HashMap<Long, ArrayList<StreamSegmentAppendOperation>> appendsByStream = new HashMap<>();
        ConcurrentHashMap<Long, Long> latencies = new ConcurrentHashMap<>();
        ArrayList<Operation> readEntries;
        ArrayList<Operation> readEntriesAfterRecovery;
        ArrayList<TruncationMarker> truncationMarkersBeforeRecovery;
        ArrayList<TruncationMarker> truncationMarkersAfterRecovery;
        long createStreamsElapsedNanos;
        long totalAppendSize = 0;
        long writeTimeMillis;
        long readElapsedMillis;

        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata();
        DataFrameLog dfl = new InMemoryDataFrameLog();
        ReadIndex readIndex = new ReadIndex(metadata);
        try (DurableLog dl = new DurableLog(metadata, dfl, readIndex)) {
            StreamSegmentMapper streamSegmentMapper = new StreamSegmentMapper(metadata, dl);

            dl.initialize(Timeout).get();
            dl.start(Timeout).get();

            // Map the streams.
            System.out.println("Creating streams ...");
            long createStreamsStartNanos = System.nanoTime();
            for (long streamId = 0; streamId < streamCount; streamId++) {
                String name = getStreamName((int) streamId);
                streamSegmentMapper.getOrAssignStreamSegmentId(name, Timeout).get();
                streamNames.add(name);
            }

            createStreamsElapsedNanos = System.nanoTime() - createStreamsStartNanos;

            System.out.println("Generating entries ...");
            for (int i = 0; i < appendsPerStream; i++) {
                for (String streamName : streamNames) {
                    long streamId = metadata.getStreamSegmentId(streamName);
                    byte[] appendData = getAppendData(maxAppendLength);
                    StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(streamId, appendData);
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
                CompletableFuture<Long> resultFuture = dl.add(entry, Timeout);
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
            appendsByStream.values().forEach(Main::sortOperationList);

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
            dl.stop(Timeout).get();
            truncationMarkersBeforeRecovery = getTruncationMarkers(metadata);
        }

        System.out.println("Performing recovery ...");
        long recoveryStartNanos = System.nanoTime();
        long recoveryElapsedMillis;
        try (DurableLog dl = new DurableLog(metadata, dfl, readIndex)) {
            dl.initialize(Timeout).get();
            dl.start(Timeout).get();

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

            dl.stop(Timeout).get();
            truncationMarkersAfterRecovery = getTruncationMarkers(metadata);
        }

        if (truncationMarkersBeforeRecovery.size() != truncationMarkersAfterRecovery.size()) {
            System.out.println(String.format("Truncation marker counts differ. Expected %d, actual %d.", truncationMarkersBeforeRecovery.size(), truncationMarkersAfterRecovery.size()));
        }

        for (int i = 0; i < Math.max(truncationMarkersAfterRecovery.size(), truncationMarkersBeforeRecovery.size()); i++) {
            TruncationMarker expected = i < truncationMarkersBeforeRecovery.size() ? truncationMarkersBeforeRecovery.get(i) : null;
            TruncationMarker actual = i < truncationMarkersAfterRecovery.size() ? truncationMarkersAfterRecovery.get(i) : null;
            if (expected == null || actual == null || (expected.getDataFrameSequenceNumber() != actual.getDataFrameSequenceNumber()) || (expected.getOperationSequenceNumber() != actual.getOperationSequenceNumber())) {
                System.out.println(String.format("TruncationMarker is DIFFERENT[%d]. Expected %s, actual %s.", i, expected, actual));
            }
        }

        System.out.println("Truncation Marker check complete.");

        printMetadata(metadata, streamNames, false);

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
    }

    private static void testOperationQueueProcessor() throws Exception {
        int maxAppendLength = 64 * 1024;
        int streamCount = 50;
        int appendsPerStream = 100;
        boolean sealAllStreams = true;

        OperationQueue queue = new OperationQueue();
        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata();
        InMemoryDataFrameLog dataFrameLog = new InMemoryDataFrameLog();
        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(metadata);
        MemoryLogUpdater logUpdater = new MemoryLogUpdater(new MemoryOperationLog(), new ReadIndex(metadata));
        OperationQueueProcessor qp = new OperationQueueProcessor(queue, metadataUpdater, logUpdater, dataFrameLog);
        qp.initialize(Duration.ZERO);
        qp.start(Duration.ZERO);

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
        long totalAppendSize = 0;
        ArrayList<Operation> entries = new ArrayList<>();
        for (int i = 0; i < appendsPerStream; i++) {
            for (String streamName : streamNames) {
                long streamId = metadata.getStreamSegmentId(streamName);
                byte[] appendData = getAppendData(maxAppendLength);
                entries.add(new StreamSegmentAppendOperation(streamId, appendData));
                totalAppendSize += appendData.length;
            }
        }

        if (sealAllStreams) {
            for (String streamName : streamNames) {
                long streamId = metadata.getStreamSegmentId(streamName);
                entries.add(new StreamSegmentSealOperation(streamId));
            }
        }

        AtomicLong processingEndTimeNanos = new AtomicLong();
        Function<Operation, CompletableOperation> createOperationWithCallback =
                entry ->
                {
                    Consumer<Long> successCallback = (seqNo) ->
                    {
                        if (seqNo >= entries.get(entries.size() - 1).getSequenceNumber()) {
                            processingEndTimeNanos.set(System.nanoTime());
                        }
                    };

                    Consumer<Throwable> failureCallback = (ex) ->
                    {
                        System.err.println(String.format("Operation '%s' failed.", entry));
                        System.err.println(ex);
                    };

                    return new CompletableOperation(entry, successCallback, failureCallback);
                };

        //Add some appends
        System.out.println("Queuing entries ...");
        long produceStartNanos = System.nanoTime();
        for (Operation entry : entries) {
            CompletableOperation ec = createOperationWithCallback.apply(entry);
            queue.add(ec);
        }

        long producingElapsedNanos = System.nanoTime() - produceStartNanos;

        System.out.println("Finished producing.");

        Thread.sleep(2000);
        qp.stop(Duration.ZERO).get();

        printMetadata(metadata, streamNames, true);

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

    private static void testBlockingDrainingQueue() throws Exception {
        BlockingDrainingQueue<Integer> q = new BlockingDrainingQueue<>();
        Random r = new Random();
        int produceCount = 1000;
        ArrayList<Integer> expected = new ArrayList<>();
        ArrayList<Integer> actual = new ArrayList<>();
        Thread t1 = new Thread(() ->
        {
            for (int i = 0; i < produceCount; i++) {
                q.add(i);
                expected.add(i);
                try {
                    Thread.sleep(r.nextInt(3));
                }
                catch (Exception ex) {
                }
            }
        });

        Thread t2 = new Thread(() -> {
            while (true) {
                //System.out.println("Waiting for entries...");
                try {
                    Iterable<Integer> e1 = q.takeAllEntries();
                    for (Integer i : e1) {
                        actual.add(i);
                        System.out.print(i + " ");
                    }
                    System.out.println();
                    Thread.sleep(r.nextInt(6));
                }
                catch (Exception ex) {
                }
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join(500);
        t1.interrupt();
        if (expected.size() != actual.size()) {
            System.out.printf("Sizes differ. Expected = %d, Actual = %d.\n", expected.size(), actual.size());
        }
        else {
            boolean areSame = true;
            for (int i = 0; i < expected.size(); i++) {
                if (!expected.get(i).equals(actual.get(i))) {
                    areSame = false;
                    System.out.printf("Contents differ at index %d. Expected = %d, Actual = %d.\n", i, expected.get(i), actual.get(i));
                    break;
                }
            }
            if (areSame) {
                System.out.println("Expected == Actual.");
            }
        }
    }

    private static void testDataFrameBuilder() throws Exception {
        int entryCount = 5000;
        int maxAppendLength = 1024 * 1024;
        int frameSize = 1024 * 1024;
        ArrayList<Function<Integer, Operation>> creators = getOperationCreators(maxAppendLength);

        System.out.println("Generating entries...");
        long totalAppendLength = 0;
        ArrayList<CompletableOperation> entries = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            Function<Integer, Operation> creator = creators.get(i % creators.size());
            Operation entry = creator.apply(i);
            if (entry instanceof StreamSegmentAppendOperation) {
                totalAppendLength += ((StreamSegmentAppendOperation) entry).getData().length;
            }

            entry.setSequenceNumber(i);
            entries.add(new CompletableOperation(entry, null, null));
        }
        System.out.println(String.format("%d entries generated.", entryCount));

        InMemoryDataFrameLog dataFrameLog = new InMemoryDataFrameLog();
        AtomicLong lastAck = new AtomicLong(-1);
        Consumer<DataFrameBuilder.DataFrameCommitArgs> ackCallback = args ->
        {
            if (lastAck.get() >= args.getLastFullySerializedSequenceNumber()) {
                System.out.println(String.format("Unexpected sequence number acked. Expected > %d, actual %d.", lastAck.get(), args));
            }

            lastAck.set(args.getLastFullySerializedSequenceNumber());
        };

        Consumer<Exception> failCallback = ex -> System.out.println(String.format("Failed with exception.", ex));

        DataFrameBuilder b = new DataFrameBuilder(frameSize, dataFrameLog, ackCallback, failCallback);

        long startTime = System.nanoTime();
        for (CompletableOperation e : entries) {
            b.append(e.getOperation());
        }

        b.close();

        long elapsedMillis = (System.nanoTime() - startTime) / 1000 / 1000;
        double opsPerSecond = entries.size() / (elapsedMillis / 1000.0);
        double kbPerSecond = totalAppendLength / (elapsedMillis / 1000.0) / 1024;
        System.out.println(String.format("Finished writing. EntryCount = %d, Duration = %dms, Ops/Sec = %f, KB/s = %f.", entries.size(), elapsedMillis, opsPerSecond, kbPerSecond));

        if ((int) lastAck.get() != entries.size() - 1) {
            System.out.println(String.format("Not all entries were acked. Expected: %d, actual %d.", entries.size() - 1, lastAck.get()));
        }

        // DataFrameReader
        startTime = System.nanoTime();
        DataFrameReader reader = new DataFrameReader(dataFrameLog);
        Iterator<CompletableOperation> entryIterator = entries.iterator();
        int readCount = 0;
        while (true) {
            Operation expectedEntry = entryIterator.hasNext() ? entryIterator.next().getOperation() : null;
            DataFrameReader.ReadResult readResult = reader.getNextOperation(Duration.ofMinutes(1)).get();

            if (expectedEntry == null) {
                if (readResult != null) {
                    System.out.println(String.format("DataFrameReader has more entries than expected (returned SeqNo %d).", readResult.getOperation().getSequenceNumber()));
                }

                break;
            }
            else if (readResult == null) {
                System.out.println(String.format("DataFrameReader has no more entries, but at least one was expected (returned SeqNo %d).", expectedEntry.getSequenceNumber()));
                break;
            }
            else {
                if (!areEqual(expectedEntry, readResult.getOperation())) {
                    System.out.println(String.format("Read Operation differs from original. Expected = '%s', Actual = '%s'.", expectedEntry, readResult.getOperation()));
                }
            }

            readCount++;
        }

        elapsedMillis = (System.nanoTime() - startTime) / 1000 / 1000;
        opsPerSecond = readCount / (elapsedMillis / 1000.0);
        kbPerSecond = totalAppendLength / (elapsedMillis / 1000.0) / 1024;
        System.out.println(String.format("Finished reading & verification. EntryCount = %d, Duration = %dms, Ops/Sec = %f, KB/s = %f.", readCount, elapsedMillis, opsPerSecond, kbPerSecond));
    }

    private static void testDataFrame() throws Exception {
        int maxFrameSize = 1024;
        ArrayList<byte[]> records = new ArrayList<>();
        ByteArraySegment frameData = null;
        long wfStartMagic;
        long wfEndMagic;
        long wfLength;
        DataFrame wf = new DataFrame(1, maxFrameSize);
        records.add("the quick brown fox jumps over the lazy dog".getBytes());
        records.add("and only use the returned List, and never the original ArrayList\" is a very important addition here".getBytes());
        records.add(new byte[]{ 1, 2, 3, 4, 5 });
        records.add(new byte[0]);
        records.add("the unmodifiable list isn't an extension; it's a delegator. You can always copy the contents to a new list and modify that, but there is no mechanism provided for modifying the contents on the unmodifiable list, which is kinda the point".getBytes());
        records.add("Note this provides only runtime safety as returned wrapper has mutators which just throw exceptions when called. Its better to use a readonly wrapper which has no mutators to get compile time safety".getBytes());
        for (byte[] record : records) {
            wf.startNewEntry(true);
            int size = wf.append(new ByteArraySegment(record));
            System.out.println(String.format("Append: Length=%d, Appended Length=%d.", record.length, size));
            if (size < record.length) {
                break;
            }
            wf.endEntry(true);
        }

        wf.seal();

        wfStartMagic = wf.getStartMagic();
        wfEndMagic = wf.getEndMagic();
        wfLength = wf.getLength();

        frameData = wf.getData();

        // TODO: We are reading a new Data Frame. Make sure we can also readDurableLog a writeable data frame.
        DataFrame rf = new DataFrame(frameData);
        System.out.println(String.format("StartMagic: W=%d, R=%d.", wfStartMagic, rf.getStartMagic()));
        System.out.println(String.format("EndMagic: W=%d, R=%d.", wfEndMagic, rf.getEndMagic()));
        System.out.println(String.format("Length: W=%d, R=%d.", wfLength, rf.getLength()));

        IteratorWithException<DataFrame.DataFrameEntry, SerializationException> readEntries = rf.getEntries();
        for (int i = 0; i < records.size(); i++) {
            byte[] expectedRecord = records.get(i);
            DataFrame.DataFrameEntry actualEntry = readEntries.pollNextElement();
            System.out.printf("Entry %d: ExpectedLength=%d, ActualLength=%d, First = %s, Last = %s, ", i, expectedRecord.length, actualEntry.getData().getLength(), actualEntry.isFirstRecordEntry(), actualEntry.isLastRecordEntry());
            if (expectedRecord.length != actualEntry.getData().getLength()) {
                System.out.println("Length Mismatch.");
            }
            else {
                int differentIndex = -1;
                for (int j = 0; j < expectedRecord.length; j++) {
                    if (expectedRecord[j] != actualEntry.getData().get(j)) {
                        differentIndex = j;
                        break;
                    }
                }
                System.out.println(differentIndex >= 0 ? "Records differ at index " + differentIndex + "." : "Records are identical.");
            }
        }

        if (readEntries.hasNext()) {
            System.out.println("More elements were returned by the 'getEntries' method.");
        }
    }

    private static void testLogOperations() throws Exception {
        final int count = 10;
        AtomicLong seqNo = new AtomicLong();

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        ArrayList<Function<Integer, Operation>> creators = getOperationCreators(1024 * 1024);

        ArrayList<Operation> writtenEntries = new ArrayList<>();

        for (Function<Integer, Operation> creator : creators) {
            for (int i = 0; i < count; i++) {
                Operation le = creator.apply(i);
                le.setSequenceNumber(seqNo.getAndIncrement());
                writtenEntries.add(le);
                le.serialize(os);
                //System.out.println("Wrote: " + le.toString());
            }
        }

        os.flush();
        byte[] data = os.toByteArray();
        ByteArrayInputStream is = new ByteArrayInputStream(data);

        for (int i = 0; i < writtenEntries.size(); i++) {
            Operation actualEntry = Operation.deserialize(is);
            Operation expectedEntry = writtenEntries.get(i);
            System.out.println(String.format("Expected[%d]: %s", i, getEntryString(expectedEntry)));
            System.out.println(String.format("Actual[%d]  : %s", i, getEntryString(actualEntry)));
        }
    }

    //region Helpers

    private static void checkStreamContentsFromReadIndex(long streamId, long offset, int length, ReadIndex index, String expectedContents, boolean verbose) throws Exception {
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
            Iterator<Operation> readResult = dl.read(lastReadSequence, 100, Timeout).get();
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

    private static void printMetadata(StreamSegmentContainerMetadata metadata, Collection<String> streamNames, boolean includeTruncationMarkers) {
        System.out.println("Final Stream Metadata:");
        for (String streamName : streamNames) {
            long streamId = metadata.getStreamSegmentId(streamName);
            StreamSegmentMetadata streamSegmentMetadata = metadata.getStreamSegmentMetadata(streamId);
            System.out.println(String.format("Stream = %d, Name = %s, StorageLength = %d, DurableLogLength = %d, Sealed = %s",
                    streamSegmentMetadata.getId(),
                    streamSegmentMetadata.getName(),
                    streamSegmentMetadata.getStorageLength(),
                    streamSegmentMetadata.getDurableLogLength(),
                    streamSegmentMetadata.isSealed()));
        }

        if (includeTruncationMarkers) {
            System.out.println("Final Truncation Metadata");
            ArrayList<TruncationMarker> truncationMarkers = getTruncationMarkers(metadata);
            for (TruncationMarker truncationMarker : truncationMarkers) {
                System.out.println(String.format("Truncation Marker: OperationSN = %d, DataFrameSN = %d", truncationMarker.getOperationSequenceNumber(), truncationMarker.getDataFrameSequenceNumber()));
            }
        }
    }

    private static ArrayList<TruncationMarker> getTruncationMarkers(StreamSegmentContainerMetadata metadata) {
        ArrayList<TruncationMarker> result = new ArrayList<>();
        TruncationMarker lastTruncationMarker = null;
        long maxSeqNo = metadata.getNewOperationSequenceNumber();
        for (long i = 0; i < maxSeqNo; i++) {
            TruncationMarker tm = metadata.getClosestTruncationMarker(i);
            if (tm == null) {
                continue;
            }

            if (lastTruncationMarker == null || lastTruncationMarker.getOperationSequenceNumber() != tm.getOperationSequenceNumber()) {
                lastTruncationMarker = tm;
                result.add(tm);
            }
        }

        return result;
    }

    private static ArrayList<Function<Integer, Operation>> getOperationCreators(int maxAppendLength) {
        ArrayList<Function<Integer, Operation>> creators = new ArrayList<>();
        creators.add((index) -> new StreamSegmentMapOperation(getStreamId(index), new StreamSegmentInformation(getStreamName(index), 123, true, false, new Date())));
        creators.add((index) ->
        {
            StreamSegmentSealOperation sse = new StreamSegmentSealOperation(getStreamId(index));
            sse.setStreamSegmentLength(index * index);
            return sse;
        });
        creators.add((index) -> new BatchMapOperation(getStreamId(index + 1), getStreamId(index), getStreamName(index)));
        creators.add((index) ->
        {
            MergeBatchOperation mbe = new MergeBatchOperation(getStreamId(index + 1), getStreamId(index));
            mbe.setBatchStreamSegmentLength(index);
            mbe.setTargetStreamSegmentOffset(index * index);
            return mbe;
        });
        creators.add((index) -> new MetadataPersistedOperation());
        creators.add((index) ->
        {
            StreamSegmentAppendOperation sae = new StreamSegmentAppendOperation(getStreamId(index), getAppendData(maxAppendLength));
            sae.setStreamSegmentOffset(index);
            return sae;
        });

        return creators;
    }

    private static long getStreamId(int index) {
        return index * index;
    }

    private static String getStreamName(int index) {
        return "/foo/foo." + index + ".stream";
    }

    private static byte[] getAppendData(int maxAppendLength) {
        // TODO: try to use from the same buffer.
        byte[] b = new byte[Math.max(1, Random.nextInt(maxAppendLength))];
        Random.nextBytes(b);
        return b;
    }

    private static String getAppendString(byte[] data) {
        return new String(data);
    }

    private static String getEntryString(Operation le) {
        StreamSegmentAppendOperation sae = (le instanceof StreamSegmentAppendOperation) ? (StreamSegmentAppendOperation) le : null;
        return le.toString() + ((sae == null) ? "" : ", Data = " + getAppendString(sae.getData()));
    }

    private static <T extends Operation> void sortOperationList(ArrayList<T> operations) {
        operations.sort(((o1, o2) -> (int) (o1.getSequenceNumber() - o2.getSequenceNumber())));
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
            }
            else if (entry1 instanceof StreamSegmentAppendOperation) {
                return areEqual((StreamSegmentAppendOperation) entry1, (StreamSegmentAppendOperation) entry2);
            }
            else if (entry1 instanceof MergeBatchOperation) {
                return areEqual((MergeBatchOperation) entry1, (MergeBatchOperation) entry2);
            }
        }
        else if (entry1 instanceof MetadataOperation) {
            if (entry1 instanceof MetadataPersistedOperation) {
                // nothing special here
                return true;
            }
            else if (entry1 instanceof StreamSegmentMapOperation) {
                return areEqual((StreamSegmentMapOperation) entry1, (StreamSegmentMapOperation) entry2);
            }
            else if (entry1 instanceof BatchMapOperation) {
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
