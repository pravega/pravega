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
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.contracts.SequencedElement;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Decomposes Data Frames into the Log Operations that were serialized into them. Uses a DataFrameLog as an input, reads
 * it in order from the beginning, and returns all successfully serialized Log Operations from them in the order in which
 * they were serialized.
 */
@Slf4j
@Getter (AccessLevel.PROTECTED)
@NotThreadSafe
class DataFrameReader<T extends SequencedElement> implements CloseableIterator<DataFrameRecord<T>, Exception> {
    //region Members

    private final DataFrameInputStream dataFrameInputStream;
    private final Serializer<T> serializer;
    @Setter (AccessLevel.PROTECTED)
    private long lastReadSequenceNumber;
    private boolean closed;
    private final String traceObjectId;
    private final int maxOverlapToCheckForDuplicates;
    private final DuplicateEntryTracker duplicateEntryTracker = new DuplicateEntryTracker();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrameReader class.
     *
     * @param log            The DataFrameLog to read data frames from.
     * @param serializer A Serializer to create LogItems upon deserialization.
     * @param containerId    The Container Id for the DataFrameReader (used primarily for logging).
     * @throws NullPointerException    If any of the arguments are null.
     * @throws DurableDataLogException If the given log threw an exception while initializing a Reader.
     */
    DataFrameReader(DurableDataLog log, Serializer<T> serializer, int containerId) throws DurableDataLogException {
        this(log, serializer, containerId, 0); // By default, tolerate no duplicates.
    }

    /**
     * Creates a new instance of the DataFrameReader class.
     *
     * @param log            The DataFrameLog to read data frames from.
     * @param serializer A Serializer to create LogItems upon deserialization.
     * @param containerId    The Container Id for the DataFrameReader (used primarily for logging).
     * @param maxOverlapToCheckForDuplicates Max number of log items to keep track of to check for duplicates.
     * @throws NullPointerException    If any of the arguments are null.
     * @throws DurableDataLogException If the given log threw an exception while initializing a Reader.
     */
    DataFrameReader(DurableDataLog log, Serializer<T> serializer, int containerId, int maxOverlapToCheckForDuplicates) throws DurableDataLogException {
        Preconditions.checkNotNull(log, "log");
        Preconditions.checkNotNull(serializer, "serializer");
        this.lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        this.traceObjectId = String.format("DataFrameReader[%d]", containerId);
        this.dataFrameInputStream = new DataFrameInputStream(log.getReader(), this.traceObjectId);
        this.serializer = serializer;
        this.maxOverlapToCheckForDuplicates = maxOverlapToCheckForDuplicates;
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
     * @return A DataFrameRecord with the requested operation. If no more Operations are available, null is returned.
     */
    @Override
    public DataFrameRecord<T> getNext() throws DataCorruptionException, DurableDataLogException {
        Exceptions.checkNotClosed(this.closed, closed);

        try {
            while (!this.dataFrameInputStream.isClosed()) {
                try {
                    if (!this.dataFrameInputStream.beginRecord()) {
                        // We've reached the end of the DataFrameInputStream.
                        return null;
                    }

                    // Attempt to deserialize the next record. If the serialization was bad, this will throw an exception which we'll pass along.
                    T logItem = this.serializer.deserialize(this.dataFrameInputStream);
                    DataFrameRecord.RecordInfo recordInfo = this.dataFrameInputStream.endRecord();
                    long seqNo = logItem.getSequenceNumber();
                    trackLastReadLogItem(seqNo, recordInfo);
                    if (seqNo <= this.lastReadSequenceNumber) {
                        // We have found a repeated item while reading the log. We need to discriminate between a harmless
                        // duplicate entry or some other kind of data corruption.
                        if (isHarmlessDuplicateEntry(seqNo, recordInfo)) {
                            // In case there is a duplicate entry, we can safely discard it (see https://github.com/pravega/pravega/issues/6444)
                            log.warn("{}: Invalid Operation Sequence Number. Expected: larger than {}, found: {}. Discarding entry.",
                                    this.traceObjectId, this.lastReadSequenceNumber, seqNo);
                        } else {
                            // The duplicate entry found is different from the existing one, so it is corrupted.
                            throw new DataCorruptionException(String.format("Corrupted repeated Operation. Expected: larger than %d, found: %d.",
                                    this.lastReadSequenceNumber, seqNo));
                        }
                    } else {
                        this.lastReadSequenceNumber = seqNo;
                        return new DataFrameRecord<>(logItem, recordInfo);
                    }
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
            // Since there are many layers of iterators (DataFrame, DataFrameRecord, LogItems), handling an exception at
            // the very top level is problematic, mostly because we would have to "rewind" some of the other iterators
            // to a previous position, otherwise any retries may read the wrong data.
            close();
            throw ex;
        }
    }

    /**
     * Adds a new entry to the processed log items tracking map (only in case the log entry is related to a new sequence
     * number). Also, deletes the oldest element in the map in case its size exceeds maxTrackedEntriesToCheckDuplicates.
     *
     * @param seqNo Sequence number of the new log item to track.
     * @param logItem Internal information for this log item.
     */
    private void trackLastReadLogItem(long seqNo, DataFrameRecord.RecordInfo logItem) {
        if (this.duplicateEntryTracker.containsKey(seqNo)) {
            log.warn("{}: Found duplicate entry {}. Do not track it.", this.traceObjectId, seqNo);
            return;
        }

        // We have a new valid log entry to track, just add it to the tracking map.
        this.duplicateEntryTracker.put(seqNo, logItem);

        // Remove exceeding elements from tracking map.
        if (this.duplicateEntryTracker.getNumTrackedElements() > this.maxOverlapToCheckForDuplicates) {
            this.duplicateEntryTracker.removeLast();
        }
    }

    private boolean isHarmlessDuplicateEntry(long seqNo, DataFrameRecord.RecordInfo recordInfo) {
        // Check if the duplicate comes from too far behind. In this case, we will report DCE anyway.
        if (!duplicateEntryTracker.containsKey(seqNo)) {
            log.warn("{}: Seems that we have found a duplicate {}, but it exceeds the max number of tracking elements {}.",
                    this.traceObjectId, seqNo, this.maxOverlapToCheckForDuplicates);
            return false;
        }

        // To determine if duplicate log entry is harmless, we use the available internal information of a DataFrame to
        // check for equality: i) length of DataFrameRecord.EntryInfo list, and ii) compare that all DataFrameRecord.EntryInfo
        // have the same length and offset within the DataFrame.
        boolean harmlessDuplicate = duplicateEntryTracker.getEntriesFor(seqNo).size() == recordInfo.getEntries().size();
        int scanUpTo = Math.min(duplicateEntryTracker.getEntriesFor(seqNo).size(), recordInfo.getEntries().size());
        for (int i = 0; i < scanUpTo; i++) {
            harmlessDuplicate &= compareDataFrameEntryInfos(duplicateEntryTracker.getEntriesFor(seqNo).get(i), recordInfo.getEntries().get(i));
        }
        return harmlessDuplicate;
    }

    /**
     * Check if 2 repeated DataFrameRecord.EntryInfo items read from the log are equal comparing their offset and length
     * within the DataFrame.
     *
     * @param originalEntryInfo Sequence number of the new log item to track.
     * @param duplicateEntryInfo Internal information for this log item.
     */
    private boolean compareDataFrameEntryInfos(DataFrameRecord.EntryInfo originalEntryInfo, DataFrameRecord.EntryInfo duplicateEntryInfo) {
        return originalEntryInfo.getFrameOffset() == duplicateEntryInfo.getFrameOffset()
                && originalEntryInfo.getLength() == duplicateEntryInfo.getLength();
    }

    /**
     * Helper class to keep track of recent read items from the log in order to check if it is safe to discard duplicate
     * entries that could have been written to the log.
     */
    @NotThreadSafe
    private static class DuplicateEntryTracker {
        private final Map<Long, DataFrameRecord.RecordInfo> lastProcessedLogItems = new LinkedHashMap<>();

        public boolean containsKey(long seqNo) {
            return this.lastProcessedLogItems.containsKey(seqNo);
        }

        public List<DataFrameRecord.EntryInfo> getEntriesFor(long seqNo) {
            return this.lastProcessedLogItems.get(seqNo).getEntries();
        }

        public void put(long seqNo, DataFrameRecord.RecordInfo logItem) {
            this.lastProcessedLogItems.put(seqNo, logItem);
        }

        public int getNumTrackedElements() {
            return this.lastProcessedLogItems.size();
        }

        public DataFrameRecord.RecordInfo removeLast() {
            long lastKey = this.lastProcessedLogItems.keySet().iterator().next();
            return this.lastProcessedLogItems.remove(lastKey);
        }
    }

    //endregion
}
