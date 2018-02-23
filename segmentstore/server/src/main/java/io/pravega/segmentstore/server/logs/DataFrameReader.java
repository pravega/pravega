/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.LogItem;
import io.pravega.segmentstore.server.LogItemFactory;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Decomposes Data Frames into the Log Operations that were serialized into them. Uses a DataFrameLog as an input, reads
 * it in order from the beginning, and returns all successfully serialized Log Operations from them in the order in which
 * they were serialized.
 */
@Slf4j
class DataFrameReader<T extends LogItem> implements CloseableIterator<DataFrameReader.ReadResult<T>, Exception> {
    //region Members

    private final FrameEntryEnumerator frameContentsEnumerator;
    private final OperationInputStream operationInputStream;
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
     * @throws NullPointerException    If any of the arguments are null.
     * @throws DurableDataLogException If the given log threw an exception while initializing a Reader.
     */
    DataFrameReader(DurableDataLog log, LogItemFactory<T> logItemFactory, int containerId) throws DurableDataLogException, IOException {
        Preconditions.checkNotNull(log, "log");
        Preconditions.checkNotNull(logItemFactory, "logItemFactory");
        this.frameContentsEnumerator = new FrameEntryEnumerator(log, String.format("DataFrameReader[%d]", containerId));
        this.operationInputStream = new OperationInputStream(this.frameContentsEnumerator);
        this.lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
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

    //region CloseableIterator Implementation

    /**
     * Attempts to return the next Operation from the DataFrameLog.
     *
     * @return A CompletableFuture that, when complete, will contain a ReadResult with the requested Operation. If no more
     * Operations exist, null will be returned.
     */
    @Override
    public ReadResult<T> getNext() throws DataCorruptionException {
        Exceptions.checkNotClosed(this.closed, closed);

        try {
            while (this.operationInputStream.hasMoreData()) {
                try {
                    // Attempt to deserialize the entry. If the serialization was bad, this will throw an exception which we'll pass along.
                    T logItem = this.logItemFactory.deserialize(this.operationInputStream);
                    long seqNo = logItem.getSequenceNumber();
                    if (seqNo <= this.lastReadSequenceNumber) {
                        throw new DataCorruptionException(String.format("Invalid Operation Sequence Number. Expected: larger than %d, found: %d.",
                                this.lastReadSequenceNumber, seqNo));
                    }

                    this.lastReadSequenceNumber = seqNo;
                    return new ReadResult<>(logItem, this.operationInputStream.getLastUsedDataFrameAddress(),
                            this.operationInputStream.getLastFullDataFrameAddress(), this.operationInputStream.isLastFrameEntry());
                } catch (OperationInputStream.RecordResetException rex) {
                    // We partially "deserialized" a record, but realized it was garbage (a product of a failed, partial
                    // serialization). Discard whatever we have and try again.
                } catch (OperationInputStream.NoMoreRecordsException rex) {
                    // We are done.
                    return null;
                } catch (IOException ex) {
                    // This catches all SerializationExceptions too.
                    throw new DataCorruptionException("Deserialization failed.", ex);
                }
                // Any other exceptions are considered to be non-DataCorruption.
            }

            // No more data.
            return null;
        } catch (Exception ex) {
            // If we encountered any kind of reader exception, close the reader right away.
            // We do not do retries at this layer. Retries should be handled by the DataLog.
            // At this time, we close the reader for any kind of exception. In the future, we may decide to only do this
            // for critical exceptions, such as DataCorruptionException or DataLogNotAvailableException, but be able
            // to recover from other kinds of exceptions.
            // Since there are many layers of iterators (DataFrame, DataFrameEntry, LogItems), handling an exception at
            // the very top level is problematic, mostly because we would have to "rewind" some of the other iterators
            // to a previous position, otherwise any retries may read the wrong data.
            close();
            throw ex;
        }
    }

    //endregion

    //region Read Implementation

    private static class OperationInputStream extends InputStream {
        private final FrameEntryEnumerator frameEntryEnumerator;
        /**
         * The Address of the Data Frame containing the last segment in this collection.
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private LogAddress lastUsedDataFrameAddress;

        /**
         * The Address of the last Data Frame that ends with a segment in this collection.
         * If the number of segments is 1, then getLastFullDataFrameAddress() == getLastUsedDataFrameAddress().
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private LogAddress lastFullDataFrameAddress;

        /**
         * Indicates whether the last segment in this collection is also the last entry in its Data Frame.
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private boolean lastFrameEntry;
        private boolean hasMoreData;
        private ReadFrame.DataFrameEntry currentEntry;
        private int readEntryCount;
        private boolean alreadyReadEntry;

        OperationInputStream(FrameEntryEnumerator frameEntryEnumerator) {
            this.frameEntryEnumerator = Preconditions.checkNotNull(frameEntryEnumerator, "frameEntryEnumerator");
            this.hasMoreData = true;
        }

        @Override
        public void close() {
            this.currentEntry = null;
            this.hasMoreData = false;
        }

        @Override
        public int read() throws IOException {
            fetchNextEntryIfNecessary();
            while (this.hasMoreData) {
                int r = this.currentEntry.getData().read();
                if (r >= 0) {
                    // We have data to return.
                    return r;
                } else {
                    // Nothing to return. Fetch next entry and repeat.
                    fetchNextEntry();
                }
            }

            // We've reached the end.
            return -1;
        }

        @Override
        public int read(byte[] buffer, int index, int length) throws IOException {
            Preconditions.checkNotNull(buffer, "buffer");
            if (index < 0 || length < 0 | index + length > buffer.length) {
                throw new IndexOutOfBoundsException();
            }

            int count = 0;
            fetchNextEntryIfNecessary();
            while (count < length && this.hasMoreData) {
                int r = this.currentEntry.getData().read(buffer, index + count, length - count);
                if (r >= 0) {
                    // We found data.
                    count += r;
                } else {
                    // Fetch next entry and repeat.
                    fetchNextEntry();
                }
            }

            if (count == 0 && length > 0) {
                // Reached the end of the data.
                return -1;
            }

            return count;
        }

        public boolean hasMoreData() {
            return this.hasMoreData;
        }

        private void resetContext() {
            this.lastUsedDataFrameAddress = null;
            this.lastFullDataFrameAddress = null;
            this.currentEntry = null;
            this.lastFrameEntry = false;
        }

        private void fetchNextEntryIfNecessary() throws IOException {
            if (this.hasMoreData && this.currentEntry == null) {
                fetchNextEntry();
            }
        }

        private void fetchNextEntry() throws IOException {
            if (this.alreadyReadEntry) {
                assert this.currentEntry != null;
                this.alreadyReadEntry = false;
                return;
            }

            while (true) {
                ReadFrame.DataFrameEntry nextEntry = this.frameEntryEnumerator.getNext();
                if (nextEntry == null) {
                    // 'null' means no more entries (or frames). Since we are still in the while loop, it means we were in the middle
                    // of an entry that hasn't been fully committed. We need to discard it and mark the end of the 'Operation stream'.
                    close();
                    if (this.currentEntry != null && !this.currentEntry.isLastRecordEntry()) {
                        throw new EOFException();
                    } else {
                        throw new NoMoreRecordsException();
                    }
                }

                if (nextEntry.isFirstRecordEntry()) {
                    // We encountered a 'First entry'. We need to discard whatever we have so far, and start
                    // constructing a new Operation. This happens if an entry was committed partially, but we were
                    // unable to write the rest of it.
                    if (this.currentEntry != null && !this.currentEntry.isLastRecordEntry()) {
                        resetContext();
                        setEntry(nextEntry);
                        this.alreadyReadEntry = true;
                        throw new RecordResetException();
                    }
                } else if (this.currentEntry == null) {
                    // We found an entry that is not marked as "First Record Entry", yet we are expecting one marked as such
                    // This happens when the DurableDataLog has been truncated, and an entry has been "cut" in two.
                    // In this case, this entry is garbage, so it should be skipped.
                    if (this.readEntryCount > 0) {
                        // But this should ONLY happen at the very beginning of a read. If we encounter something like
                        // this in the middle of a log, we very likely have some sort of corruption.
                        throw new SerializationException(String.format("Found a DataFrameEntry which is not marked as " +
                                        "'First Record Entry', but no active record is being read. DataFrameAddress = %s",
                                nextEntry.getFrameAddress()));
                    }

                    continue;
                }

                setEntry(nextEntry);
                break;
            }
        }

        private void setEntry(ReadFrame.DataFrameEntry nextEntry) throws IOException {
            long dataFrameSequence = nextEntry.getFrameAddress().getSequence();
            if (this.lastUsedDataFrameAddress != null && dataFrameSequence < this.lastUsedDataFrameAddress.getSequence()) {
                throw new SerializationException(String.format("Invalid DataFrameSequence. Expected at least '%d', found '%d'.",
                        this.lastUsedDataFrameAddress.getSequence(), dataFrameSequence));
            }

            if (nextEntry.isLastEntryInDataFrame()) {
                // This is the last segment in this DataFrame, so we need to set the lastFullDataFrameAddress to the right value.
                this.lastFullDataFrameAddress = nextEntry.getFrameAddress();
            }

            this.lastUsedDataFrameAddress = nextEntry.getFrameAddress();
            this.lastFrameEntry = nextEntry.isLastEntryInDataFrame();
            this.currentEntry = nextEntry;
            if (this.currentEntry.isLastRecordEntry()) {
                this.readEntryCount++;
            }
        }

        static class NoMoreRecordsException extends IOException {
        }

        static class RecordResetException extends IOException {
        }
    }


    //endregion

    //region ReadResult

    /**
     * Represents a DataFrame Read Result, wrapping a LogItem.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class ReadResult<T extends LogItem> {
        /**
         * The wrapped Log Operation.
         */
        @Getter
        private final T item;

        /**
         * The Address of the Last Data Frame containing the LogItem. If the LogItem fits on exactly one DataFrame, this
         * will contain the Address for that Data Frame; if it spans multiple data frames, this stores the last data frame address.
         */
        @Getter
        private final LogAddress lastUsedDataFrameAddress;

        /**
         * The Address of the Last Data Frame that ends with a part of the LogItem. If
         * the LogItem fits on exactly one DataFrame, this will return the Address for that Data Frame; if it spans
         * multiple data frames, it returns the Address of the last Data Frame that ends with a part of the LogItem
         * (in general, this is the Data Frame immediately preceding that returned by getLastUsedDataFrameAddress()).
         */
        @Getter
        private final LogAddress lastFullDataFrameAddress;

        /**
         * Whether the wrapped LogItem is the last entry in its Data Frame.
         */
        @Getter
        private final boolean lastFrameEntry;

        @Override
        public String toString() {
            return String.format("%s, DataFrameSN = %d, LastInDataFrame = %s", getItem(), this.lastUsedDataFrameAddress.getSequence(), isLastFrameEntry());
        }
    }

    //endregion

    //region FrameEntryEnumerator

    /**
     * Enumerates DataFrameEntries from all frames, in sequence.
     */
    private static class FrameEntryEnumerator implements CloseableIterator<ReadFrame.DataFrameEntry, Exception> {
        //region Members

        private final String traceObjectId;
        private final DataFrameEnumerator dataFrameEnumerator;
        private CloseableIterator<ReadFrame.DataFrameEntry, IOException> currentFrameContents;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the FrameEntryEnumerator class.
         *
         * @param log The DataFrameLog to read from.
         * @throws NullPointerException    If log is null.
         * @throws DurableDataLogException If the given log threw an exception while initializing a Reader.
         */
        FrameEntryEnumerator(DurableDataLog log, String traceObjectId) throws DurableDataLogException {
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
         * @return A CompletableFuture that, when completed, will contain the next available DataFrameEntry. If no such
         * entry exists, it will contain a null value.
         */
        @Override
        public ReadFrame.DataFrameEntry getNext() throws IOException {
            // Check to see if we are in the middle of a frame, in which case, just return the next element.
            ReadFrame.DataFrameEntry result;
            if (this.currentFrameContents != null) {
                result = this.currentFrameContents.getNext();
                if (result != null) {
                    return result;
                }
            }

            // We need to fetch a new frame.
            ReadFrame dataFrame = this.dataFrameEnumerator.getNext();
            if (dataFrame == null) {
                // No more frames to retrieve
                this.currentFrameContents = null;
                return null;
            } else {
                // We just got a new frame.
                log.debug("{}: Read DataFrame (Address = {}, Length = {}).", this.traceObjectId, dataFrame.getAddress(), dataFrame.getLength());
                this.currentFrameContents = dataFrame.getEntries();
                result = this.currentFrameContents.getNext();
                if (result != null) {
                    return result;
                } else {
                    // The DataFrameEnumerator should not return empty frames. We can either go in a loop and try to get next,
                    // or throw (which is correct, since we rely on DataFrameEnumerator to behave correctly).
                    throw new SerializationException("Found empty DataFrame when non-empty was expected.");
                }
            }
        }

        //endregion
    }

    //endregion

    //region DataFrameEnumerator

    /**
     * Enumerates DataFrames from a DataFrameLog.
     */
    private static class DataFrameEnumerator implements CloseableIterator<ReadFrame, Exception> {
        //region Members

        private static final long INITIAL_LAST_READ_FRAME_SEQUENCE = -1;
        private final DurableDataLog log;
        private long lastReadFrameSequence;
        private CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the DataFrameEnumerator class.
         *
         * @param log The DataFrameLog to read from.
         * @throws NullPointerException    If log is null.
         * @throws DurableDataLogException If the given log threw an exception while initializing a Reader.
         */
        DataFrameEnumerator(DurableDataLog log) throws DurableDataLogException {
            Preconditions.checkNotNull(log, "log");

            this.log = log;
            this.lastReadFrameSequence = INITIAL_LAST_READ_FRAME_SEQUENCE;
            if (this.reader == null) {
                // We start from the beginning.
                this.reader = this.log.getReader();
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
         * @return A CompletableFuture that, when complete, will contain the next DataFrame from the log. If no such
         * frame exists, it will contain a null value.
         */
        @Override
        public ReadFrame getNext() throws IOException {
            DurableDataLog.ReadItem nextItem;
            try {
                nextItem = this.reader.getNext();
            } catch (DurableDataLogException ex) {
                throw new IOException(ex);
            }

            if (nextItem == null) {
                // We have reached the end. Stop here.
                return null;
            }

            ReadFrame frame;
            try {
                frame = new ReadFrame(nextItem.getPayload(), nextItem.getLength());
                frame.setAddress(nextItem.getAddress());
            } catch (SerializationException ex) {
                throw new SerializationException(String.format("Unable to deserialize DataFrame. LastReadFrameSequence =  %d.",
                        this.lastReadFrameSequence), ex);
            }

            long sequence = frame.getAddress().getSequence();
            if (sequence <= this.lastReadFrameSequence) {
                // FrameSequence must be a strictly monotonically increasing number.
                throw new SerializationException(String.format("Found DataFrame out of order. Expected frame sequence greater than %d, found %d.",
                        this.lastReadFrameSequence, sequence));
            }

            this.lastReadFrameSequence = sequence;
            return frame;
        }

        //endregion
    }

    //endregion
}
