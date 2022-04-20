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

import io.pravega.common.Exceptions;
import io.pravega.segmentstore.contracts.SequencedElement;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DebugDataFrameReader<T extends SequencedElement> extends DataFrameReader<T> {

    private final AtomicBoolean errorOnDataCorruption;

    DebugDataFrameReader(DurableDataLog log, Serializer<T> serializer, int containerId, boolean errorOnDataCorruption) throws DurableDataLogException {
       super(log, serializer, containerId);
       this.errorOnDataCorruption = new AtomicBoolean(errorOnDataCorruption);
    }

    /**
     * This overridden getNext does not throw a DataCorruptionException on
     * finding duplicate entries.
     */
    @Override
    public DataFrameRecord<T> getNext() throws DataCorruptionException, DurableDataLogException {
        Exceptions.checkNotClosed(this.isClosed(), this.isClosed());

        try {
            while (!this.getDataFrameInputStream().isClosed()) {
                try {
                    if (!this.getDataFrameInputStream().beginRecord()) {
                        // We've reached the end of the DataFrameInputStream.
                        return null;
                    }
                    // Attempt to deserialize the next record. If the serialization was bad, this will throw an exception which we'll pass along.
                    T logItem = this.getSerializer().deserialize(this.getDataFrameInputStream());
                    DataFrameRecord.RecordInfo recordInfo = this.getDataFrameInputStream().endRecord();
                    long seqNo = logItem.getSequenceNumber();
                    if (seqNo <= this.getLastReadSequenceNumber()) {
                        if (errorOnDataCorruption.get()) {
                            throw new DataCorruptionException(String.format("Invalid Operation Sequence Number. Expected: larger than %d, found: %d.",
                                    this.getLastReadSequenceNumber(), seqNo));
                        } else {
                            log.error("Invalid Operation Sequence Number. Expected: larger than {}, found: {}.",
                                    this.getLastReadSequenceNumber(), seqNo);
                        }
                    } else {
                        this.setLastReadSequenceNumber(seqNo);
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
}
