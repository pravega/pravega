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
import io.pravega.common.ObjectClosedException;
import io.pravega.segmentstore.contracts.SequencedElement;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.LogAddress;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds DataFrames from LogItems. Splits the serialization of LogItems across multiple Data Frames, if necessary,
 * and publishes the finished Data Frames to the given DataFrameLog.
 */
@Slf4j
@NotThreadSafe
public class DataFrameBuilder<T extends SequencedElement> implements AutoCloseable {
    //region Members

    private final DataFrameOutputStream outputStream;
    private final DurableDataLog targetLog;
    private final Serializer<T> serializer;
    private final Args args;
    private final AtomicBoolean closed;
    private long lastSerializedSequenceNumber;
    private long lastStartedSequenceNumber;
    private final AtomicReference<Throwable> failureCause;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrameBuilder class.
     *
     * @param targetLog     A Function that, given a DataFrame, commits that DataFrame to a DurableDataLog and returns
     *                      a Future that indicates when the operation completes or errors out.
     * @param serializer    Log Item Serializer to use.
     * @param args          Arguments for the Builder.
     * @throws NullPointerException If any of the arguments are null.
     */
    public DataFrameBuilder(DurableDataLog targetLog, Serializer<T> serializer, Args args) {
        this.targetLog = Preconditions.checkNotNull(targetLog, "targetLog");
        this.serializer = Preconditions.checkNotNull(serializer, "serializer");
        this.args = Preconditions.checkNotNull(args, "args");
        Preconditions.checkNotNull(args.commitSuccess, "args.commitSuccess");
        Preconditions.checkNotNull(args.commitFailure, "args.commitFailure");
        this.outputStream = new DataFrameOutputStream(targetLog.getWriteSettings().getMaxWriteLength(), this::handleDataFrameComplete);
        this.lastSerializedSequenceNumber = -1;
        this.lastStartedSequenceNumber = -1;
        this.failureCause = new AtomicReference<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.compareAndSet(false, true)) {
            // Close the underlying stream (which destroys whatever we have in flight).
            this.outputStream.close();
        }
    }

    //endregion

    //region Operations

    /**
     * Forces a flush of the current DataFrame. This should be invoked if there are no more items to add to the current
     * DataFrame, but it is desired to have its outstanding contents flushed to the underlying DurableDataLog.
     */
    public void flush() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        this.outputStream.flush();
    }

    /**
     * If in a failed state (and thus closed), returns the original exception that caused the failure.
     *
     * @return The causing exception, or null if none.
     */
    public Throwable failureCause() {
        return this.failureCause.get();
    }

    /**
     * Appends a LogItem to the DataFrameBuilder. If any exceptions happened during serialization, whatever contents was
     * written to the DataFrame will be discarded. Note that if a LogItem spans multiple DataFrames, in case of failure,
     * the content serialized to already committed DataFrames will not be discarded. That case will have to be dealt with
     * upon reading DataFrames from the DataFrameLog.
     *
     * Any exceptions that resulted from the Data Frame failing to commit will be routed through the dataFrameCommitFailureCallback
     * callback, as well as being thrown from this exception.
     *
     * @param logItem The LogItem to append.
     * @throws NullPointerException If logItem is null.
     * @throws IllegalArgumentException If attempted to add LogItems out of order (based on Sequence Number).
     * @throws IOException          If the LogItem failed to serialize to the DataLog, or if one of the DataFrames containing
     *                              the LogItem failed to commit to the DataFrameLog.
     * @throws ObjectClosedException If the DataFrameBuilder is closed (or in in a failed state) and cannot be used anymore.
     */
    public void append(T logItem) throws IOException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        long seqNo = logItem.getSequenceNumber();
        Exceptions.checkArgument(this.lastSerializedSequenceNumber < seqNo, "logItem",
                "Invalid sequence number. Expected: greater than %d, given: %d.", this.lastSerializedSequenceNumber, seqNo);

        // Remember the last Started SeqNo, in case of failure.
        long previousLastStartedSequenceNumber = this.lastStartedSequenceNumber;
        try {
            // Indicate to the output stream that are about to write a new record.
            this.outputStream.startNewRecord();

            // Completely serialize the entry. Note that this may span more than one Data Frame.
            this.lastStartedSequenceNumber = seqNo;
            this.serializer.serialize(this.outputStream, logItem);

            // Indicate to the output stream that have finished writing the record.
            this.outputStream.endRecord();
            this.lastSerializedSequenceNumber = seqNo;
        } catch (Exception ex) {
            if (this.closed.get()) {
                // It's possible that an async callback resulted in an error and this object got closed after the check
                // at the beginning of this method (which could result in all sorts of errors. If that's the case, we need
                // to indicate that we are closed by throwing ObjectClosedException.
                throw new ObjectClosedException(this, ex);
            } else if (ex instanceof ObjectClosedException) {
                // TargetLog has closed. We need to close too.
                close();
            } else {
                // Discard any information that we have about this record (pretty much revert back to where startNewEntry()
                // would have begun writing).
                this.outputStream.discardRecord();
                this.lastStartedSequenceNumber = previousLastStartedSequenceNumber;
            }

            throw ex;
        }
    }

    /**
     * Publishes a data frame to the DataFrameLog. The outcome of the publish operation, whether success or failure, is
     * routed to the appropriate callback handlers given in this constructor. This method is called synchronously by the
     * DataFrameOutputStream, via the LogItem.serialize() method through the append() method, and as such, it is executed
     * on the same thread that invoked append().
     *
     * @param dataFrame The data frame to publish.
     * @throws NullPointerException     If the data frame is null.
     * @throws IllegalArgumentException If the data frame is not sealed.
     */
    private void handleDataFrameComplete(DataFrame dataFrame) {
        Exceptions.checkArgument(dataFrame.isSealed(), "dataFrame", "Cannot publish a non-sealed DataFrame.");

        // Write DataFrame to DataFrameLog.
        CommitArgs commitArgs = new CommitArgs(this.lastSerializedSequenceNumber, this.lastStartedSequenceNumber, dataFrame.getLength());

        try {
            this.args.beforeCommit.accept(commitArgs);
            this.targetLog.append(dataFrame.getData(), this.args.writeTimeout)
                    .thenAcceptAsync(logAddress -> {
                        commitArgs.setLogAddress(logAddress);
                        this.args.commitSuccess.accept(commitArgs);
                    }, this.args.executor)
                    .exceptionally(ex -> handleProcessingException(ex, commitArgs));
        } catch (Throwable ex) {
            handleProcessingException(ex, commitArgs);

            // Even though we invoked the dataFrameCommitFailureCallback() - which was for the DurableLog to handle,
            // we still need to fail the current call, which most likely leads to failing the LogItem that triggered this.
            throw ex;
        }
    }

    private Void handleProcessingException(Throwable ex, CommitArgs commitArgs) {
        // This failure is due to us being unable to commit a DataFrame, whether synchronously or via a callback. The
        // DataFrameBuilder cannot recover from this; as such it will close and will leave it to the caller to handle
        // the failure.
        ex = Exceptions.unwrap(ex);
        if (!isShutdownException(ex)) {
            // This is usually from a subsequent call. We want to store the actual failure cause.
            this.failureCause.compareAndSet(null, ex);
        }

        this.args.commitFailure.accept(ex, commitArgs);
        close();
        return null;
    }

    private boolean isShutdownException(Throwable ex) {
        return ex instanceof ObjectClosedException || ex instanceof CancellationException;
    }

    //endregion

    //region CommitArgs

    /**
     * Contains Information about the committal of a DataFrame.
     */
    public static class CommitArgs {
        /**
         * The Sequence Number of the last LogItem that was fully serialized (and committed).
         * If this value is different than 'getLastStartedSequenceNumber' then we currently have a LogItem that was split
         * across multiple Data Frames, and the value returned from that function represents the Sequence Number for that entry.
         */
        @Getter
        private final long lastFullySerializedSequenceNumber;

        /**
         * The Sequence Number of the last LogItem that was started (but not necessarily committed).
         * If this value is different than 'getLastFullySerializedSequenceNumber' then we currently have a LogItem that was split
         * across multiple Data Frames, and the value returned from this function represents the Sequence Number for that entry.
         */
        @Getter
        private final long lastStartedSequenceNumber;

        private final AtomicReference<LogAddress> logAddress;

        /**
         * The length of the DataFrame that was just committed.
         */
        @Getter
        private final int dataFrameLength;

        @Getter
        @Setter
        private long metadataTransactionId;

        @Getter
        @Setter
        private List<CompletableOperation> operations;

        /**
         * Creates a new instance of the CommitArgs class.
         *
         * @param lastFullySerializedSequenceNumber The Sequence Number of the last LogItem that was fully serialized (and committed).
         * @param lastStartedSequenceNumber         The Sequence Number of the last LogItem that was started (but not necessarily committed).
         * @param dataFrameLength                   The length of the DataFrame that is to be committed.
         */
        private CommitArgs(long lastFullySerializedSequenceNumber, long lastStartedSequenceNumber, int dataFrameLength) {
            assert lastFullySerializedSequenceNumber <= lastStartedSequenceNumber : "lastFullySerializedSequenceNumber (" +
                    lastFullySerializedSequenceNumber + ") is greater than lastStartedSequenceNumber (" + lastStartedSequenceNumber + ")";

            this.lastFullySerializedSequenceNumber = lastFullySerializedSequenceNumber;
            this.lastStartedSequenceNumber = lastStartedSequenceNumber;
            this.dataFrameLength = dataFrameLength;
            this.logAddress = new AtomicReference<>();
        }

        /**
         * Gets a value representing the LogAddress of the Data Frame that was committed.
         */
        LogAddress getLogAddress() {
            return this.logAddress.get();
        }

        private void setLogAddress(LogAddress address) {
            this.logAddress.set(address);
        }

        @Override
        public String toString() {
            return String.format("TxnId = %d, LastFullySerializedSN = %d, LastStartedSN = %d, Address = %s, Length = %d",
                    getMetadataTransactionId(), getLastFullySerializedSequenceNumber(), getLastStartedSequenceNumber(), this.logAddress, getDataFrameLength());
        }
    }

    //endregion

    //region Args

    @RequiredArgsConstructor
    public static class Args {
        /**
         * A Callback that will be invoked synchronously upon a DataFrame's sealing, and right before it is about to be
         * submitted to the DurableDataLog processor. The invocation of this method does not imply that the DataFrame
         * has been successfully committed, or even attempted to be committed.
         */
        final Consumer<CommitArgs> beforeCommit;

        /**
         * A Callback that will be invoked asynchronously upon every successful commit of a Data Frame. When this is
         * called, all entries added via append() that have a Sequence Number less than or equal to the arg's
         * LastFullySerializedSequenceNumber have been committed. Any entry with a Sequence Number higher than that
         * is not yet committed.
         */
        final Consumer<CommitArgs> commitSuccess;

        /**
         * A Callback that will be invoked asynchronously upon a failed commit of a Data Frame. When this is called, all
         * entries added via append() that have a sequence number up to, and including, LastStartedSequenceNumber that
         * have not previously been acknowledged, should be failed.
         */
        final BiConsumer<Throwable, CommitArgs> commitFailure;
        final Executor executor;
        final Duration writeTimeout = Duration.ofSeconds(30); // TODO: actual timeout.
    }

    //endregion
}
