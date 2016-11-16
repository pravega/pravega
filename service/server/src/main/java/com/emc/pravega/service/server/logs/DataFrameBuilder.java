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

package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.common.function.ConsumerWithException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.LogItem;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Builds DataFrames from LogItems. Splits the serialization of LogItems across multiple Data Frames, if necessary,
 * and publishes the finished Data Frames to the given DataFrameLog.
 */
@Slf4j
class DataFrameBuilder<T extends LogItem> implements AutoCloseable {
    //region Members

    private static final Duration DATA_FRAME_WRITE_TIMEOUT = Duration.ofSeconds(30); // TODO: actual timeout.
    private final DataFrameOutputStream outputStream;
    private final DurableDataLog targetLog;
    private final ConsumerWithException<DataFrameCommitArgs, Exception> dataFrameCommitSuccessCallback;
    private final Consumer<Throwable> dataFrameCommitFailureCallback;
    private boolean closed;
    private long lastSerializedSequenceNumber;
    private long lastStartedSequenceNumber;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrameBuilder class.
     *
     * @param targetLog                      The DurableDataLog to publish completed Data Frames to.
     * @param dataFrameCommitSuccessCallback A callback that will be invoked upon every successful commit of a Data
     *                                       Frame.
     *                                       When this is called, all entries added via append() that were successful
     *                                       have
     *                                       been 100% committed. In-flight entries (that have been written
     *                                       partially) should not be acked.
     * @param dataFrameCommitFailureCallback A callback that will be invoked upon a failed commit of a Data Frame.
     *                                       When this is called, all entries added via append() that were successful
     *                                       have
     *                                       failed to commit. The in-flight entries will be failed via the append()
     *                                       method.
     * @throws NullPointerException If any of the arguments are null.
     */
    DataFrameBuilder(DurableDataLog targetLog, ConsumerWithException<DataFrameCommitArgs, Exception>
            dataFrameCommitSuccessCallback, Consumer<Throwable> dataFrameCommitFailureCallback) {
        Preconditions.checkNotNull(targetLog, "targetLog");
        Preconditions.checkNotNull(dataFrameCommitFailureCallback, "dataFrameCommitFailureCallback");
        Preconditions.checkNotNull(dataFrameCommitSuccessCallback, "dataFrameCommitSuccessCallback");

        this.targetLog = targetLog;
        this.outputStream = new DataFrameOutputStream(targetLog.getMaxAppendLength(), targetLog::getLastAppendSequence,
                this::handleDataFrameComplete);
        this.lastSerializedSequenceNumber = -1;
        this.lastStartedSequenceNumber = -1;
        this.dataFrameCommitSuccessCallback = dataFrameCommitSuccessCallback;
        this.dataFrameCommitFailureCallback = dataFrameCommitFailureCallback;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws IOException {
        if (!this.closed) {
            // Stop accepting any new items.
            this.closed = true;

            // Seal & ship whatever frame we currently have (if any).
            this.outputStream.flush();

            // Close the underlying stream (which destroys whatever we have in flight - but there shouldn't be any at
            // this point).
            this.outputStream.close();
        }
    }

    //endregion

    //region Appending

    /**
     * Appends a LogItem to the DataFrameBuilder. If any exceptions happened during serialization, whatever contents was
     * written to the DataFrame will be discarded. Note that if a LogItem spans multiple DataFrames, in case of failure,
     * the content serialized to already committed DataFrames will not be discarded. That case will have to be dealt
     * with
     * upon reading DataFrames from the DataFrameLog.
     * <p/>
     * Any exceptions that resulted from the Data Frame failing to commit will be routed through the
     * dataFrameCommitFailureCallback
     * callback, as well as being thrown from this exception.
     *
     * @param logItem The LogItem to append.
     * @throws NullPointerException If logItem is null.
     * @throws IOException          If the LogItem failed to serialize to the DataLog, or if one of the DataFrames
     *                              containing
     *                              the LogItem failed to commit to the DataFrameLog.
     */
    public void append(T logItem) throws IOException {
        Exceptions.checkNotClosed(this.closed, this);
        long seqNo = logItem.getSequenceNumber();
        Exceptions.checkArgument(this.lastSerializedSequenceNumber < seqNo, "logItem",
                "Invalid sequence number. " + "Expected: greater than %d, given: %d.",
                this.lastSerializedSequenceNumber, seqNo);

        // Remember the last Started SeqNo, in case of failure.
        long previousLastStartedSequenceNumber = this.lastStartedSequenceNumber;
        try {
            // Indicate to the output stream that are about to write a new record.
            this.outputStream.startNewRecord();

            // Completely serialize the entry. Note that this may span more than one Data Frame.
            this.lastStartedSequenceNumber = seqNo;
            logItem.serialize(this.outputStream);
            this.lastSerializedSequenceNumber = seqNo;
        } catch (Exception ex) {
            // Discard any information that we have about this record (pretty much revert back to where startNewEntry
            // () would have begun writing).
            // The try-catch inside handleDataFrameComplete() deals with the DataFrame-level handling; here we just
            // deal with this LogItem.
            this.outputStream.discardRecord();
            this.lastStartedSequenceNumber = previousLastStartedSequenceNumber;
            throw ex;
        }

        // Indicate to the output stream that have finished writing the record.
        this.outputStream.endRecord();
    }

    /**
     * Publishes a data frame to the DataFrameLog. The outcome of the publish operation, whether success or failure, is
     * routed to the appropriate callback handlers given in this constructor.
     *
     * @param dataFrame The data frame to publish.
     * @throws NullPointerException     If the data frame is null.
     * @throws IllegalArgumentException If the data frame is not sealed.
     * @throws IOException              When the DataFrame could not be committed.
     */
    private void handleDataFrameComplete(DataFrame dataFrame) throws IOException {
        Exceptions.checkArgument(dataFrame.isSealed(), "dataFrame", "Cannot publish a non-sealed DataFrame.");

        // Write DataFrame to DataFrameLog.
        try {
            LogAddress logAddress = this.targetLog.append(dataFrame.getData(), DATA_FRAME_WRITE_TIMEOUT).get();

            // Need to assign the DataFrameSequence that we got back from the DataLog. This is used to record
            // truncation markers.
            dataFrame.setAddress(logAddress);
            assert dataFrame.getPreviousFrameSequence() < logAddress.getSequence() : "DataLog assigned non-monotonic " +
                    "" + "sequence number";
        } catch (Exception ex) {
            Throwable realException = ExceptionHelpers.getRealException(ex);
            // This failure is due to us being unable to commit the DataFrame; this means the entire DataFrame has to
            // be discarded.
            // The Target Log did try to repeat, but we need to admit failure now.
            this.outputStream.reset();
            CallbackHelpers.invokeSafely(this.dataFrameCommitFailureCallback, realException,
                    cex -> log.error("dataFrameCommitFailureCallback FAILED.", cex));

            // Even though we invoked the dataFrameCommitFailureCallback() - which was for the DurableLog to handle,
            // we still need to fail the current call, which most likely leads to failing the LogItem that triggered
            // this.
            throw new IOException(realException);
        }

        try {
            this.dataFrameCommitSuccessCallback.accept(
                    new DataFrameCommitArgs(this.lastSerializedSequenceNumber, this.lastStartedSequenceNumber,
                            dataFrame));
        } catch (Exception ex) {
            CallbackHelpers.invokeSafely(this.dataFrameCommitFailureCallback, ex,
                    cex -> log.error("dataFrameCommitFailureCallback FAILED.", cex));
            throw new IOException(ex);
        }
    }

    //endregion

    //region DataFrameCommitArgs

    /**
     * Contains Information about the committal of a DataFrame.
     */
    static class DataFrameCommitArgs {
        private final long lastFullySerializedSequenceNumber;
        private final long lastStartedSequenceNumber;
        private final LogAddress logAddress;
        private final long previousDataFrameSequence;
        private final int dataFrameLength;

        /**
         * Creates a new instance of the DataFrameCommitArgs class.
         *
         * @param lastFullySerializedSequenceNumber The Sequence Number of the last LogItem that was fully serialized
         *                                          (and committed).
         * @param lastStartedSequenceNumber         The Sequence Number of the last LogItem that was started (but not
         *                                          necessarily committed).
         * @param dataFrame                         The DataFrame that was just committed.
         */
        private DataFrameCommitArgs(long lastFullySerializedSequenceNumber, long lastStartedSequenceNumber, DataFrame
                dataFrame) {
            assert lastFullySerializedSequenceNumber <= lastStartedSequenceNumber :
                    "lastFullySerializedSequenceNumber (" + lastFullySerializedSequenceNumber + ") is greater than "
                            + "lastStartedSequenceNumber (" + lastStartedSequenceNumber + ")";
            assert dataFrame.getAddress().getSequence() >= 0 : "negative dataFrameSequence";
            assert dataFrame.getAddress().getSequence() > dataFrame.getPreviousFrameSequence() : "dataFrameSequence "
                    + "should be larger than previousDataFrameSequence";

            this.lastFullySerializedSequenceNumber = lastFullySerializedSequenceNumber;
            this.lastStartedSequenceNumber = lastStartedSequenceNumber;
            this.previousDataFrameSequence = dataFrame.getPreviousFrameSequence();
            this.logAddress = dataFrame.getAddress();
            this.dataFrameLength = dataFrame.getLength();
        }

        /**
         * Gets a value indicating the Sequence Number of the last LogItem that was fully serialized (and committed).
         * If this value is different than 'getLastStartedSequenceNumber' then we currently have a LogItem that was
         * split
         * across multiple Data Frames, and the value returned from that function represents the Sequence Number for
         * that entry.
         */
        long getLastFullySerializedSequenceNumber() {
            return this.lastFullySerializedSequenceNumber;
        }

        /**
         * Gets a value indicating the Sequence Number of the last LogItem that was started (but not necessarily
         * committed).
         * If this value is different than 'getLastFullySerializedSequenceNumber' then we currently have a LogItem
         * that was split
         * across multiple Data Frames, and the value returned from this function represents the Sequence Number for
         * that entry.
         */
        long getLastStartedSequenceNumber() {
            return this.lastStartedSequenceNumber;
        }

        /**
         * Gets a value indicating the LogAddress of the Data Frame that was committed.
         */
        LogAddress getLogAddress() {
            return this.logAddress;
        }

        /**
         * Gets a value indicating the Sequence Number of the last Data Frame that was committed prior to this one.
         */
        long getPreviousDataFrameSequence() {
            return this.previousDataFrameSequence;
        }

        /**
         * Gets a value indicating the length of the DataFrame that was just committed.
         */
        int getDataFrameLength() {
            return this.dataFrameLength;
        }

        @Override
        public String toString() {
            return String.format("LastFullySerializedSN = %d, LastStartedSN = %d, DataFrameSN = %d/%d, Length = %d",
                    getLastFullySerializedSequenceNumber(), getLastStartedSequenceNumber(),
                    this.logAddress.getSequence(), getPreviousDataFrameSequence(), getDataFrameLength());
        }
    }

    //endregion
}
