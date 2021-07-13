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
import lombok.extern.slf4j.Slf4j;

/**
 * Decomposes Data Frames into the Log Operations that were serialized into them. Uses a DataFrameLog as an input, reads
 * it in order from the beginning, and returns all successfully serialized Log Operations from them in the order in which
 * they were serialized.
 */
@Slf4j
class DataFrameReader<T extends SequencedElement> implements CloseableIterator<DataFrameRecord<T>, Exception> {
    //region Members

    private final DataFrameInputStream dataFrameInputStream;
    private final Serializer<T> serializer;
    private long lastReadSequenceNumber;
    private boolean closed;

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
        Preconditions.checkNotNull(log, "log");
        Preconditions.checkNotNull(serializer, "serializer");
        this.lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        this.dataFrameInputStream = new DataFrameInputStream(log.getReader(), String.format("DataFrameReader[%d]", containerId));
        this.serializer = serializer;
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
                    if (seqNo <= this.lastReadSequenceNumber) {
                        throw new DataCorruptionException(String.format("Invalid Operation Sequence Number. Expected: larger than %d, found: %d.",
                                this.lastReadSequenceNumber, seqNo));
                    }

                    this.lastReadSequenceNumber = seqNo;
                    return new DataFrameRecord<>(logItem, recordInfo);
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

    //endregion
}
