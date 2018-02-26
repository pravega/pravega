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
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.LogItem;
import io.pravega.segmentstore.server.LogItemFactory;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import java.io.IOException;
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

    private final DataFrameInputStream dataFrameInputStream;
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
    DataFrameReader(DurableDataLog log, LogItemFactory<T> logItemFactory, int containerId) throws DurableDataLogException {
        Preconditions.checkNotNull(log, "log");
        Preconditions.checkNotNull(logItemFactory, "logItemFactory");
        this.dataFrameInputStream = new DataFrameInputStream(log.getReader(), String.format("DataFrameReader[%d]", containerId));
        this.lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        this.logItemFactory = logItemFactory;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.dataFrameInputStream.close();
            this.closed = true;
        }
    }

    //endregion

    //region CloseableIterator Implementation

    /**
     * Attempts to return the next Operation from the DataFrameLog.
     *
     * @return A ReadResult with the requested operation. If no more Operations are available, null is returned.
     */
    @Override
    public ReadResult<T> getNext() throws DataCorruptionException {
        Exceptions.checkNotClosed(this.closed, closed);

        try {
            while (!this.dataFrameInputStream.isClosed()) {
                try {
                    if (!this.dataFrameInputStream.beginRecord()) {
                        // We've reached the end of the DataFrameInputStream.
                        return null;
                    }

                    // Attempt to deserialize the next record. If the serialization was bad, this will throw an exception which we'll pass along.
                    T logItem = this.logItemFactory.deserialize(this.dataFrameInputStream);
                    DataFrameInputStream.RecordInfo recordInfo = this.dataFrameInputStream.endRecord();
                    long seqNo = logItem.getSequenceNumber();
                    if (seqNo <= this.lastReadSequenceNumber) {
                        throw new DataCorruptionException(String.format("Invalid Operation Sequence Number. Expected: larger than %d, found: %d.",
                                this.lastReadSequenceNumber, seqNo));
                    }

                    this.lastReadSequenceNumber = seqNo;
                    return new ReadResult<>(logItem, recordInfo);
                } catch (DataFrameInputStream.RecordResetException | DataFrameInputStream.NoMoreRecordsException ex) {
                    // We partially "deserialized" a record, but realized it was garbage (a product of a failed, partial
                    // serialization). Discard whatever we have and try again.
                } catch (IOException ex) {
                    // This catches all EOFExceptions, EndOfRecordExceptions and SerializationExceptions too.
                    // Any other exceptions are considered to be non-DataCorruption.
                    throw new DataCorruptionException("Deserialization failed.", ex);
                }
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

    //region ReadResult

    /**
     * Represents a DataFrame Read Result, wrapping a LogItem.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class ReadResult<T extends LogItem> {
        /**
         * The wrapped Log Operation.
         */
        @Getter
        private final T item;

        private final DataFrameInputStream.RecordInfo recordInfo;

        /**
         * The Address of the Last Data Frame containing the LogItem. If the LogItem fits on exactly one DataFrame, this
         * will contain the Address for that Data Frame; if it spans multiple data frames, this stores the last data frame address.
         */
        LogAddress getLastUsedDataFrameAddress() {
            return this.recordInfo.getLastUsedDataFrameAddress();
        }

        /**
         * The Address of the Last Data Frame that ends with a part of the LogItem. If
         * the LogItem fits on exactly one DataFrame, this will return the Address for that Data Frame; if it spans
         * multiple data frames, it returns the Address of the last Data Frame that ends with a part of the LogItem
         * (in general, this is the Data Frame immediately preceding that returned by getLastUsedDataFrameAddress()).
         */
        LogAddress getLastFullDataFrameAddress() {
            return this.recordInfo.getLastFullDataFrameAddress();
        }

        /**
         * Whether the wrapped LogItem is the last entry in its Data Frame.
         */
        boolean isLastFrameEntry() {
            return this.recordInfo.isLastFrameEntry();
        }

        @Override
        public String toString() {
            return String.format("%s, DataFrameSN = %d, LastInDataFrame = %s", getItem(), this.getLastUsedDataFrameAddress().getSequence(), isLastFrameEntry());
        }
    }

    //endregion
}
