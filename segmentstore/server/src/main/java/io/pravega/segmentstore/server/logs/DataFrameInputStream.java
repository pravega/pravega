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
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.LogAddress;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * An InputStream that abstracts reading from DataFrames that were previously serialized into a DurableDataLog. This can
 * be used to read back data that was written using DataFrameOutputStream.
 */
@Slf4j
@NotThreadSafe
public class DataFrameInputStream extends InputStream {
    //region Members

    private final String traceObjectId;
    private final CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader;
    private DataFrame.DataFrameEntryIterator currentFrameContents;
    private DataFrame.DataFrameEntry currentEntry;
    private long lastReadFrameSequence;

    private DataFrameRecord.RecordInfo.RecordInfoBuilder currentRecordBuilder;
    @Getter
    private boolean closed;
    private boolean hasReadAnyData;
    /**
     * The {@link DataFrameInputStream} provides an {@link InputStream}-like interface on top of
     * {@link DataFrame.DataFrameEntry} instances that make up the {@link DurableDataLog}. Every call to {@link #read} may
     * either read from the currently loaded {@link DataFrame.DataFrameEntry} or fetch another one ({@link #fetchNextEntry()}.
     *
     * However, in certain exceptional cases (such as when an Operation has been split but only one part was successfully
     * written to the {@link DurableDataLog}), we may have had to request more {@link DataFrame.DataFrameEntry} instances
     * in order to figure out the situation (which means we may have also read part of the next (valid) operation). When
     * this happens, we need to notify the upstream code (via a {@link RecordResetException}) and set ourselves in a state
     * where we can only proceed once that upstream code has recovered from this situation and is ready to begin reading
     * the next item ({@link #beginRecord}.
     *
     * Upstream code may wrap this instance in other types of {@link InputStream}s which may invoke {@link #skip} when
     * closed (for exceptional cases). We need to make sure we don't skip over bytes that may actually be needed, while
     * at the same time do support {@link #skip} for other, valid cases.
     */
    private boolean prefetchedEntry;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrameInputStream class.
     *
     * @param reader        An Iterator that produces DurableDataLog.ReadItems, which are then interpreted as DataFrames.
     * @param traceObjectId Used for logging.
     */
    DataFrameInputStream(CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> reader, String traceObjectId) {
        this.reader = Preconditions.checkNotNull(reader, "reader");
        this.traceObjectId = Exceptions.checkNotNullOrEmpty(traceObjectId, "traceObjectId");
        this.lastReadFrameSequence = -1;
        this.currentRecordBuilder = DataFrameRecord.RecordInfo.builder();
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
    @SneakyThrows(DurableDataLogException.class)
    public int read() throws IOException {
        Preconditions.checkState(!this.prefetchedEntry, "Must call beginRecord() before reading or skipping from a prefetched entry.");
        while (!this.closed) {
            int r = this.currentEntry.getData().read();
            if (r >= 0) {
                // We have data to return.
                return r;
            } else {
                // Reached the end of the current entry.
                checkEndOfRecord();
                fetchNextEntry();
            }
        }

        // We've reached the end of the DataFrameInputStream.
        return -1;
    }

    @Override
    @SneakyThrows(DurableDataLogException.class)
    public int read(byte[] buffer, int index, int length) throws IOException {
        Preconditions.checkState(!this.prefetchedEntry, "Must call beginRecord() before reading or skipping from a prefetched entry.");
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
                // Reached the end of the current entry.
                checkEndOfRecord();
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

    /**
     * Indicates that a new record is to be expected. When invoked, if in the middle of a record, it will be skipped over
     * and the DataFrameInputStream will be positioned at the beginning of the next record.
     *
     * @return True if a new record can be read, false if we reached the end of the DataFrameInputStream and can no longer
     * read.
     * @throws IOException If an IO Exception occurred.
     * @throws DurableDataLogException If a non-IO Exception has occurred, usually thrown by the underlying DurableDataLog.
     */
    boolean beginRecord() throws IOException, DurableDataLogException {
        try {
            if (this.currentEntry != null && !this.prefetchedEntry) {
                endRecord();
            }

            fetchNextEntry();
            return true;
        } catch (NoMoreRecordsException ex) {
            // We've reached the end of the DataFrameInputStream. We are done.
            return false;
        }
    }

    /**
     * Indicates that the current record has finished processing. When invoked, the DataFrameInputStream will be repositioned
     * at the start of the next record. This method only needs to be called upon a successful record read. If an IOException
     * occurred while reading data from a record, either the DataFrameInputStream will be auto-closed or a subsequent call
     * to beginRecord() will reposition the stream accordingly.
     *
     * @return A RecordInfo containing metadata about the record that just ended, such as addressing information.
     * @throws IOException If an IO Exception occurred.
     * @throws DurableDataLogException If a non-IO Exception has occurred, usually thrown by the underlying DurableDataLog.
     */
    DataFrameRecord.RecordInfo endRecord() throws IOException, DurableDataLogException {
        DataFrameRecord.RecordInfo r = this.currentRecordBuilder.build();
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

    private void checkEndOfRecord() throws IOException {
        if (this.currentEntry.isLastRecordEntry()) {
            // We've reached the end of the current record, but the caller wants to read more. This is usually
            // indicative of some data corruption.
            throw new EndOfRecordException("Reached the end of the current record.");
        }
    }

    private void resetContext() {
        this.currentRecordBuilder = DataFrameRecord.RecordInfo.builder();
        this.currentEntry = null;
        this.prefetchedEntry = false;
    }

    private void fetchNextEntry() throws IOException, DurableDataLogException {
        Exceptions.checkNotClosed(this.closed, this);
        if (this.prefetchedEntry) {
            assert this.currentEntry != null : "prefetchEntry==true, but currentEntry==null";
            this.prefetchedEntry = false;
            return;
        }

        while (!this.closed) {
            DataFrame.DataFrameEntry nextEntry = getNextFrameEntry();

            if (nextEntry == null) {
                // 'null' means no more entries (or frames). Since we are still in the while loop, it means we were in the
                // middle of an entry that hasn't been fully committed. We need to discard it and mark the end of the
                // DataFrameInputStream.
                close();
                throw new NoMoreRecordsException();
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
                if (this.hasReadAnyData) {
                    // But this should ONLY happen at the very beginning of a read. If we encounter something like
                    // this in the middle of a log, we very likely have some sort of corruption.
                    throw new SerializationException(String.format("Found a DataFrameRecord which is not marked as " +
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
        LogAddress lastUsedAddress = this.currentRecordBuilder.getLastUsedDataFrameAddress();
        if (lastUsedAddress != null && dataFrameSequence < lastUsedAddress.getSequence()) {
            throw new SerializationException(String.format("Invalid DataFrameSequence. Expected at least '%d', found '%d'.",
                    lastUsedAddress.getSequence(), dataFrameSequence));
        }

        if (nextEntry.isLastEntryInDataFrame()) {
            // This is the last segment in this DataFrame, so we need to set the lastFullDataFrameAddress to the right value.
            this.currentRecordBuilder.lastFullDataFrameAddress(nextEntry.getFrameAddress());
        }

        this.currentRecordBuilder.lastUsedDataFrameAddress(nextEntry.getFrameAddress());
        this.currentRecordBuilder.lastFrameEntry(nextEntry.isLastEntryInDataFrame());
        this.currentEntry = nextEntry;
        if (this.currentEntry.isLastRecordEntry()) {
            this.hasReadAnyData = true;
        }

        this.currentRecordBuilder.withEntry(nextEntry.getFrameAddress(), nextEntry.getFrameOffset(), nextEntry.getLength(), nextEntry.isLastEntryInDataFrame());
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

    //region Exceptions

    /**
     * Exception that is thrown when attempting to begin reading a new record but there is no more data to be read. When
     * encountering this Exception, the caller will know it has reached the end of the DataFrameInputStream.
     * NOTE: this is different from java.io.EOFException, which is thrown if the end of the DataFrameInputStream has been
     * reached while processing a record.
     */
    static class NoMoreRecordsException extends IOException {

        private static final long serialVersionUID = 1L;
    }

    /**
     * Exception that is thrown whenever a partial record was read but it was detected that its serialization was
     * interrupted and it does not have any more data. When encountering this Exception, the caller must discard the
     * current record and proceed to the next.
     */
    static class RecordResetException extends IOException {

        private static final long serialVersionUID = 1L;
    }

    /**
     * Exception that is thrown whenever we attempt to read beyond the end of a Record. The DataFrameInputStream allows
     * reading bytes continuously, but the caller needs to call beginRecord() and endRecord() to confirm that the record
     * data has been successfully processed.
     */
    static class EndOfRecordException extends IOException {

        private static final long serialVersionUID = 1L;

        private EndOfRecordException(String message) {
            super(message);
        }
    }

    //endregion
}
