package com.emc.logservice.server.logs;

import com.emc.logservice.common.*;
import com.emc.logservice.server.*;
import com.emc.logservice.server.logs.operations.Operation;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogException;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

/**
 * Decomposes Data Frames into the Log Operations that were serialized into them. Uses a DataFrameLog as an input, reads
 * it in order from the beginning, and returns all successfully serialized Log Operations from them in the order in which
 * they were serialized.
 */
@Slf4j
public class DataFrameReader<T extends LogItem> implements AsyncIterator<DataFrameReader.ReadResult<T>> {
    //region Members

    private final FrameEntryEnumerator frameContentsEnumerator;
    private final String traceObjectId;
    private final LogItemFactory<T> logItemFactory;
    private long lastReadSequenceNumber;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrameReader class.
     *
     * @param log            The DataFrameLog to read data frames from.
     * @param logItemFactory A LogItemFactory to create LogItems upon deserialization.
     * @param containerId    The Container Id for the DataFrameReader (used primarily for logging).
     * @throws NullPointerException    If log is null.
     * @throws DurableDataLogException If the given log threw an exception while initializing a Reader.
     */
    public DataFrameReader(DurableDataLog log, LogItemFactory<T> logItemFactory, String containerId) throws DurableDataLogException {
        Exceptions.throwIfNull(log, "log");
        Exceptions.throwIfNull(logItemFactory, "logItemFactory");
        Exceptions.throwIfNullOfEmpty(containerId, "containerId");
        this.traceObjectId = String.format("DataFrameReader[%s]", containerId);
        this.frameContentsEnumerator = new FrameEntryEnumerator(log, traceObjectId);
        this.lastReadSequenceNumber = Operation.NoSequenceNumber;
        this.logItemFactory = logItemFactory;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.frameContentsEnumerator.close();
        this.closed = true;
    }

    //endregion

    //region AsyncIterator Implementation

    /**
     * Attempts to return the next Operation from the DataFrameLog.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when complete, will contain a ReadResult with the requested Operation. If no more
     * Operations exist, null will be returned.
     */
    public CompletableFuture<ReadResult<T>> getNext(Duration timeout) {
        Exceptions.throwIfClosed(this.closed, closed);

        // Get the ByteArraySegments for the next entry (there could be one or more, depending on how many DataFrames
        // were used to split the original Operation).
        CompletableFuture<ReadResult<T>> result = getNextOperationSegments(timeout)
                .thenApply(segments ->
                {
                    if (segments == null || !segments.hasData()) {
                        // We have reached the end.
                        return null;
                    }
                    else {
                        // Get the unified input stream for all the segments.
                        InputStream source = segments.getInputStream();

                        try {
                            // Attempt to deserialize the entry. If the serialization was bad, this will throw an exception which we'll pass along.
                            // In case of such failure, we still advance, because the serialization exception is not our issue to handle.
                            T logItem = this.logItemFactory.deserialize(source);
                            long seqNo = logItem.getSequenceNumber();
                            if (seqNo <= this.lastReadSequenceNumber) {
                                throw new DataCorruptionException(String.format("Invalid Operation Sequence Number. Expected: larger than %d, found: %d.", this.lastReadSequenceNumber, seqNo));
                            }

                            this.lastReadSequenceNumber = seqNo;
                            return new ReadResult<>(logItem, segments.getDataFrameSequence(), segments.isLastFrameEntry());
                        }
                        catch (DataCorruptionException ex) {
                            throw new CompletionException(ex);
                        }
                        catch (SerializationException ex) {
                            throw new CompletionException(new DataCorruptionException("Deserialization failed.", ex));
                        }
                        // Any other exceptions are considered to be non-DataCorruption.
                    }
                });

        FutureHelpers.exceptionListener(result, ex -> {
            // If we encountered any kind of reader exception, close the reader right away.
            // We do not do retries at this layer. Retries should be handled by the DataLog.
            // At this time, we close the reader for any kind of exception. In the future, we may decide to only do this
            // for critical exceptions, such as DataCorruptionException or DataLogNotAvailableException, but be able
            // to recover from other kinds of exceptions.
            // Since there are many layers of iterators (DataFrame, DataFrameEntry, LogItems), handling an exception at
            // the very top level is problematic, mostly because we would have to "rewind" some of the other iterators
            // to a previous position, otherwise any retries may read the wrong data.
            this.close();
        });

        return result;
    }

    //endregion

    //region Read Implementation

    /**
     * Gets a collection of ByteArraySegments (SegmentCollection) that makes up the next Log Operation to be returned.
     *
     * @param timeout The timeout for the operation.
     * @return A CompletableFuture that, when complete, will contain a SegmentCollection with the requested result. If no
     * more segments are available, either a null value or an empty SegmentCollection will be returned.
     */
    private CompletableFuture<SegmentCollection> getNextOperationSegments(Duration timeout) {
        return CompletableFuture.supplyAsync(() ->
        {
            SegmentCollection result = new SegmentCollection();
            TimeoutTimer timer = new TimeoutTimer(timeout);
            while (true) {
                DataFrame.DataFrameEntry nextEntry = this.frameContentsEnumerator.getNext(timer.getRemaining()).join();
                if (nextEntry == null) {
                    // 'null' means no more entries (or frames). Since we are still in the while loop, it means we were in the middle
                    // of an entry that hasn't been fully committed. We need to discard it and mark the end of the 'Operation stream'.
                    return null;
                }
                else {
                    if (nextEntry.isFirstRecordEntry()) {
                        // We encountered a 'First entry'. We need to discard whatever we have so far, and start
                        // constructing a new Operation. This happens if an entry was committed partially, but we were
                        // unable to write the rest of it.
                        result.clear();
                    }

                    // Add the current entry's contents to the result.
                    try {
                        result.add(nextEntry.getData(), nextEntry.getDataFrameSequence(), nextEntry.isLastEntryInDataFrame());
                    }
                    catch (DataCorruptionException ex) {
                        throw new CompletionException(ex);
                    }

                    if (nextEntry.isLastRecordEntry()) {
                        // We are done. We found the last entry for a record.
                        return result;
                    }
                }
            }
        });
    }

    //endregion

    //region ReadResult

    /**
     * Represents a DataFrame Read Result, wrapping a LogItem.
     */
    public static class ReadResult<T extends LogItem> {
        private final T logItem;
        private final long dataFrameSequence;
        private final boolean lastFrameEntry;

        /**
         * Creates a new instance of the ReadResult class.
         *
         * @param logItem           The LogItem to wrap.
         * @param dataFrameSequence The Sequence Number of the Last Data Frame containing the LogItem.
         * @param lastFrameEntry    Whether this LogItem was the last entry in its Data Frame.
         */
        protected ReadResult(T logItem, long dataFrameSequence, boolean lastFrameEntry) {
            this.logItem = logItem;
            this.dataFrameSequence = dataFrameSequence;
            this.lastFrameEntry = lastFrameEntry;
        }

        /**
         * Gets a reference to the wrapped Log Operation.
         *
         * @return
         */
        public T getItem() {
            return this.logItem;
        }

        /**
         * Gets a value indicating the Sequence Number of the Last Data Frame containing the LogItem. If the LogItem
         * fits on exactly one DataFrame, this will return the Sequence number for that Data Frame; if it spans multiple
         * data frames, only the last data frame Sequence Number is returned.
         *
         * @return
         */
        public long getDataFrameSequence() {
            return this.dataFrameSequence;
        }

        /**
         * Gets a value indicating whether the wrapped LogItem is the last entry in its Data Frame.
         *
         * @return
         */
        public boolean isLastFrameEntry() {
            return this.lastFrameEntry;
        }

        @Override
        public String toString() {
            return String.format("%s, DataFrameSN = %d, LastInDataFrame = %s", getItem(), getDataFrameSequence(), isLastFrameEntry());
        }
    }

    //endregion

    //region SegmentCollection

    /**
     * A collection of ByteArraySegments that, together, make up the serialization for a Log Operation.
     */
    private static class SegmentCollection {
        private final LinkedList<ByteArraySegment> segments;
        private long dataFrameSequence;
        private boolean lastFrameEntry;

        /**
         * Creates a new instance of the SegmentCollection class.
         */
        protected SegmentCollection() {
            this.segments = new LinkedList<>();
            this.dataFrameSequence = -1;
            this.lastFrameEntry = false;
        }

        /**
         * Adds a new segment to the collection.
         *
         * @param segment           The segment to append.
         * @param dataFrameSequence The Sequence Number for the Data Frame containing the segment.
         * @param lastFrameEntry    Whether this segment is the last entry in the Data Frame.
         * @throws NullPointerException     If segment is null.
         * @throws IllegalArgumentException If dataFrameSequence is invalid.
         */
        public void add(ByteArraySegment segment, long dataFrameSequence, boolean lastFrameEntry) throws DataCorruptionException {
            Exceptions.throwIfNull(segment, "segment");

            if (dataFrameSequence < this.dataFrameSequence) {
                throw new DataCorruptionException(String.format("Invalid DataFrameSequence. Expected at least '%d', found '%d'.", this.dataFrameSequence, dataFrameSequence));
            }

            this.dataFrameSequence = dataFrameSequence;
            this.lastFrameEntry = lastFrameEntry;
            this.segments.add(segment);
        }

        /**
         * Gets a value indicating whether this collection has any items.
         *
         * @return
         */
        public boolean hasData() {
            return this.segments.size() > 0;
        }

        /**
         * Clears the collection.
         */
        public void clear() {
            this.dataFrameSequence = -1;
            this.lastFrameEntry = false;
            this.segments.clear();
        }

        /**
         * Returns an InputStream that reads from all ByteArraySegments making up this collection.
         *
         * @return
         */
        public InputStream getInputStream() {
            Stream<InputStream> ss = this.segments.stream().map(ByteArraySegment::getReader);
            return new SequenceInputStream(new IteratorToEnumeration<>(ss.iterator()));
        }

        /**
         * Gets a value indicating the Sequence Number of the Data Frame containing the last segment in this collection.
         * The return value of this method is irrelevant if hasData() == false.
         *
         * @return
         */
        public long getDataFrameSequence() {
            return this.dataFrameSequence;
        }

        /**
         * Gets a value indicating whether the last segment in this collection is also the last entry in its Data Frame.
         * The return value of this method is irrelevant if hasData() == false.
         *
         * @return
         */
        public boolean isLastFrameEntry() {
            return this.lastFrameEntry;
        }
    }

    //endregion

    //region FrameEntryEnumerator

    /**
     * Enumerates DataFrameEntries from all frames, in sequence.
     */
    private static class FrameEntryEnumerator implements AutoCloseable {
        //region Members

        private final String traceObjectId;
        private final DataFrameEnumerator dataFrameEnumerator;
        private IteratorWithException<DataFrame.DataFrameEntry, SerializationException> currentFrameContents;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the FrameEntryEnumerator class.
         *
         * @param log The DataFrameLog to read from.
         * @throws NullPointerException    If log is null.
         * @throws DurableDataLogException If the given log threw an exception while initializing a Reader.
         */
        public FrameEntryEnumerator(DurableDataLog log, String traceObjectId) throws DurableDataLogException {
            this.traceObjectId = traceObjectId;
            this.dataFrameEnumerator = new DataFrameEnumerator(log);
        }

        //endregion

        //region AutoCloseable Implementation

        @Override
        public void close() {
            this.dataFrameEnumerator.close();
        }

        //endregion

        //region Operations

        /**
         * Attempts to return the next DataFrameEntry from the DataFrameLog.
         *
         * @param timeout The timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain the next available DataFrameEntry. If no such
         * entry exists, it will contain a null value.
         */
        public CompletableFuture<DataFrame.DataFrameEntry> getNext(Duration timeout) {
            // Check to see if we are in the middle of a frame, in which case, just return the next element.
            if (this.currentFrameContents != null && this.currentFrameContents.hasNext()) {
                try {
                    DataFrame.DataFrameEntry result = this.currentFrameContents.pollNext();
                    if (result != null) {
                        return CompletableFuture.completedFuture(result);
                    }
                }
                catch (Exception ex) {
                    return FutureHelpers.failedFuture(ex);
                }
            }

            // We need to fetch a new frame.
            return this.dataFrameEnumerator
                    .getNext(timeout)
                    .thenApply(dataFrame -> {
                        if (dataFrame == null) {
                            // No more frames to retrieve
                            this.currentFrameContents = null;
                            return null;
                        }
                        else {
                            // We just got a new frame.
                            log.debug("{}: Read DataFrame (SequenceNumber = {}, Length = {}).", this.traceObjectId, dataFrame.getFrameSequence(), dataFrame.getLength());
                            this.currentFrameContents = dataFrame.getEntries();
                            if (this.currentFrameContents.hasNext()) {
                                try {
                                    return this.currentFrameContents.pollNext();
                                }
                                catch (Exception ex) {
                                    throw new CompletionException(ex);
                                }
                            }
                            else {
                                // The DataFrameEnumerator should not return empty frames. We can either go in a loop and try to get next,
                                // or throw (which is correct, since we rely on DataFrameEnumerator to behave correctly.
                                throw new CompletionException(new DataCorruptionException("Found empty DataFrame when non-empty was expected."));
                            }
                        }
                    });
        }

        //endregion
    }

    //endregion

    //region DataFrameEnumerator

    /**
     * Enumerates DataFrames from a DataFrameLog.
     */
    private static class DataFrameEnumerator implements AutoCloseable {
        //region Members

        private static final long InitialLastReadFrameSequence = -1;
        private final DurableDataLog log;
        private long lastReadFrameSequence;
        private AsyncIterator<DurableDataLog.ReadItem> reader;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the DataFrameEnumerator class.
         *
         * @param log The DataFrameLog to read from.
         * @throws NullPointerException    If log is null.
         * @throws DurableDataLogException If the given log threw an exception while initializing a Reader.
         */
        public DataFrameEnumerator(DurableDataLog log) throws DurableDataLogException {
            Exceptions.throwIfNull(log, "log");

            this.log = log;
            this.lastReadFrameSequence = InitialLastReadFrameSequence;
            if (this.reader == null) {
                // We start from the beginning.
                this.reader = this.log.getReader(InitialLastReadFrameSequence);
            }
        }

        //endregion

        //region AutoCloseable Implementation

        @Override
        public void close() {
            if (this.reader != null) {
                this.reader.close();
            }
        }

        //endregion

        //region Operations

        /**
         * Attempts to get the next DataFrame from the log.
         *
         * @param timeout The timeout for the operation.
         * @return A CompletableFuture that, when complete, will contain the next DataFrame from the log. If no such
         * frame exists, it will contain a null value.
         */
        public CompletableFuture<DataFrame> getNext(Duration timeout) {
            return this.reader.getNext(timeout).thenApply(nextItem -> {
                if (nextItem == null) {
                    // We have reached the end. Stop here.
                    return null;
                }

                DataFrame frame;
                try {
                    frame = new DataFrame(nextItem.getPayload());
                    frame.setFrameSequence(nextItem.getSequence());
                }
                catch (SerializationException ex) {
                    throw new CompletionException(new DataCorruptionException(String.format("Unable to deserialize DataFrame. LastReadFrameSequence =  %d.", this.lastReadFrameSequence), ex));
                }

                if (frame.getFrameSequence() <= this.lastReadFrameSequence) {
                    // FrameSequence must be a strictly monotonically increasing number.
                    throw new CompletionException(new DataCorruptionException(String.format("Found DataFrame out of order. Expected frame sequence greater than %d, found %d.", this.lastReadFrameSequence, frame.getFrameSequence())));
                }

                if (this.lastReadFrameSequence != InitialLastReadFrameSequence && this.lastReadFrameSequence != frame.getPreviousFrameSequence()) {
                    // Previous Frame Sequence is not match what the Current Frame claims it is.
                    throw new CompletionException(new DataCorruptionException(String.format("DataFrame with Sequence %d has a PreviousFrameSequence (%d) that does not match the previous DataFrame FrameSequence (%d).", frame.getFrameSequence(), frame.getPreviousFrameSequence(), this.lastReadFrameSequence)));
                }

                this.lastReadFrameSequence = frame.getFrameSequence();
                return frame;
            });
        }

        //endregion
    }

    //endregion
}
