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

@Slf4j
public class DataFrameInputStream extends InputStream {
    //region Members

    private final String traceObjectId;
    private final CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader;
    private DataFrame.DataFrameEntryIterator currentFrameContents;
    private DataFrame.DataFrameEntry currentEntry;
    private long lastReadFrameSequence;
    private LogAddress lastUsedDataFrameAddress;
    private LogAddress lastFullDataFrameAddress;
    private boolean isLastFrameEntry;
    @Getter
    private boolean closed;
    private int readEntryCount;
    private boolean prefetchedEntry;

    //endregion

    //region Constructor

    DataFrameInputStream(CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader, String traceObjectId) {
        this.reader = Preconditions.checkNotNull(reader, "reader");
        this.traceObjectId = Exceptions.checkNotNullOrEmpty(traceObjectId, "traceObjectId");
        this.lastReadFrameSequence = -1;
    }

    //endregion

    //region InputStream Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.currentEntry = null;
            this.reader.close();
            this.closed = true;
        }
    }

    @Override
    public int read() throws IOException {
        while (!this.closed) {
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
        while (count < length && !this.closed) {
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

    //endregion

    //region Frame Processing

    void beginRecord() throws IOException {
        if (this.currentEntry != null && !this.prefetchedEntry) {
            endRecord();
        }

        fetchNextEntry();
    }

    RecordInfo endRecord() throws IOException {
        RecordInfo r = new RecordInfo(this.lastUsedDataFrameAddress, this.lastFullDataFrameAddress, this.isLastFrameEntry);
        while (this.currentEntry != null) {
            if (this.currentEntry.isLastRecordEntry()) {
                this.currentEntry.getData().close();
                resetContext();
            } else {
                fetchNextEntry();
            }
        }
        return r;
    }

    private void resetContext() {
        this.lastUsedDataFrameAddress = null;
        this.lastFullDataFrameAddress = null;
        this.currentEntry = null;
        this.isLastFrameEntry = false;
        this.prefetchedEntry = false;
    }

    private void fetchNextEntry() throws IOException {
        Exceptions.checkNotClosed(this.closed, this);
        if (this.prefetchedEntry) {
            assert this.currentEntry != null : "prefetchEntry==true, but currentEntry==null";
            this.prefetchedEntry = false;
            return;
        }

        while (!this.closed) {
            DataFrame.DataFrameEntry nextEntry;
            try {
                nextEntry = getNextFrameEntry();
            } catch (DurableDataLogException ex) {
                throw new IOException(ex);
            }

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
                    setCurrentFrameEntry(nextEntry);
                    this.prefetchedEntry = true;
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

            setCurrentFrameEntry(nextEntry);
            break;
        }
    }

    private void setCurrentFrameEntry(DataFrame.DataFrameEntry nextEntry) throws IOException {
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
        this.isLastFrameEntry = nextEntry.isLastEntryInDataFrame();
        this.currentEntry = nextEntry;
        if (this.currentEntry.isLastRecordEntry()) {
            this.readEntryCount++;
        }
    }

    private DataFrame.DataFrameEntry getNextFrameEntry() throws DurableDataLogException, IOException {
        // Check to see if we are in the middle of a frame, in which case, just return the next element.
        DataFrame.DataFrameEntry result;
        if (this.currentFrameContents != null) {
            result = this.currentFrameContents.getNext();
            if (result != null) {
                return result;
            }
        }

        // We need to fetch a new frame.
        this.currentFrameContents = getNextFrame();
        if (this.currentFrameContents == null) {
            // No more frames to retrieve
            return null;
        } else {
            // We just got a new frame.
            log.debug("{}: Read DataFrame (Address = {}, Length = {}).", this.traceObjectId,
                    this.currentFrameContents.getFrameAddress(), this.currentFrameContents.getLength());
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

    private DataFrame.DataFrameEntryIterator getNextFrame() throws DurableDataLogException, IOException {
        DurableDataLog.ReadItem nextItem = this.reader.getNext();
        if (nextItem == null) {
            // We have reached the end. Stop here.
            return null;
        }

        DataFrame.DataFrameEntryIterator frameContents;
        try {
            frameContents = DataFrame.read(nextItem.getPayload(), nextItem.getLength(), nextItem.getAddress());
        } catch (SerializationException ex) {
            throw new SerializationException(String.format("Unable to deserialize DataFrame. LastReadFrameSequence =  %d.",
                    this.lastReadFrameSequence), ex);
        }

        long sequence = nextItem.getAddress().getSequence();
        if (sequence <= this.lastReadFrameSequence) {
            // FrameSequence must be a strictly monotonically increasing number.
            throw new SerializationException(String.format("Found DataFrame out of order. Expected frame sequence greater than %d, found %d.",
                    this.lastReadFrameSequence, sequence));
        }

        this.lastReadFrameSequence = sequence;
        return frameContents;
    }

    //endregion

    //region RecordInfo

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class RecordInfo {
        /**
         * The Address of the Data Frame containing the last segment in this collection.
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private final LogAddress lastUsedDataFrameAddress;

        /**
         * The Address of the last Data Frame that ends with a segment in this collection.
         * If the number of segments is 1, then getLastFullDataFrameAddress() == getLastUsedDataFrameAddress().
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private final LogAddress lastFullDataFrameAddress;

        /**
         * Indicates whether the last segment in this collection is also the last entry in its Data Frame.
         * The return value of this method is irrelevant if hasData() == false.
         */
        @Getter
        private final boolean lastFrameEntry;
    }

    //endregion

    //region Exceptions

    /**
     * Exception that is thrown when attempting to begin reading a new record but there is no more data to be read. When
     * encountering this Exception, the caller will know it has reached the end of the DataFrameInputStream.
     * NOTE: this is different from java.io.EOFException, which is thrown if the end of the DataFrameInputStream has been
     * reached while processing a record.
     */
    static class NoMoreRecordsException extends IOException {
    }

    /**
     * Exception that is thrown whenever a partial record was read but it was detected that its serialization was
     * interrupted and it does not have any more data. When encountering this Exception, the caller must discard the
     * current record and proceed to the next.
     */
    static class RecordResetException extends IOException {
    }

    //endregion
}
