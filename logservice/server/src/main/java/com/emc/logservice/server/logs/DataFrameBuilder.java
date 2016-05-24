package com.emc.logservice.server.logs;

import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.server.core.*;
import com.emc.logservice.server.logs.operations.Operation;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.function.*;

/**
 * Builds DataFrames from Log Operations. Splits the serialization of LogOperations across multiple Data Frames, if necessary,
 * and publishes the finished Data Frames to the given DataFrameLog.
 */
public class DataFrameBuilder implements AutoCloseable {
    //region Members

    private static final Duration DataFrameWriteTimeout = Duration.ofSeconds(30); // TODO: actual timeout.
    private final DataFrameOutputStream outputStream;
    private final DurableDataLog targetLog;
    private final Consumer<DataFrameCommitArgs> dataFrameCommitSuccessCallback;
    private final Consumer<Exception> dataFrameCommitFailureCallback;
    private boolean closed;
    private long lastSerializedSequenceNumber;
    private long lastStartedSequenceNumber;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrameBuilder class.
     *
     * @param targetLog                      The DurableDataLog to publish completed Data Frames to.
     * @param dataFrameCommitSuccessCallback A callback that will be invoked upon every successful commit of a Data Frame.
     *                                       When this is called, all entries added via append() that were successful have
     *                                       been 100% committed. In-flight entries (that have been written partially) should not be acked.
     * @param dataFrameCommitFailureCallback A callback that will be invoked upon a failed commit of a Data Frame.
     *                                       When this is called, all entries added via append() that were successful have
     *                                       failed to commit. The in-flight entries will be failed via the append() method.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If maxDataFrameSize <= 0
     */
    public DataFrameBuilder(DurableDataLog targetLog, Consumer<DataFrameCommitArgs> dataFrameCommitSuccessCallback, Consumer<Exception> dataFrameCommitFailureCallback) {
        if (targetLog == null) {
            throw new NullPointerException("targetLog");
        }

        if (dataFrameCommitFailureCallback == null) {
            throw new NullPointerException("dataFrameCommitFailureCallback");
        }

        if (dataFrameCommitSuccessCallback == null) {
            throw new NullPointerException("dataFrameCommitSuccessCallback");
        }

        this.targetLog = targetLog;
        this.outputStream = new DataFrameOutputStream(targetLog.getMaxAppendLength(), targetLog::getLastAppendSequence, this::handleDataFrameComplete);
        this.lastSerializedSequenceNumber = this.lastStartedSequenceNumber = -1;
        this.dataFrameCommitSuccessCallback = dataFrameCommitSuccessCallback;
        this.dataFrameCommitFailureCallback = dataFrameCommitFailureCallback;
    }

    //endregion

    //region Appending

    /**
     * Appends a Log Operation to the DataFrameBuilder. If any exceptions happened during serialization, the Operation is failed,
     * and its contents will be discarded from the Data Frame. Any exceptions that resulted from the Data Frame failing to commit
     * will be routed through the dataFrameCommitFailureCallback callback.
     *
     * @param operation The Operation to append.
     * @throws ObjectClosedException If the DataFrameBuilder is closed.
     */
    public void append(Operation operation) throws SerializationException {
        if (closed) {
            throw new ObjectClosedException(this);
        }

        // TODO: consider checking Operation.getSequenceNumber() monotonicity. Make sure it sticks across multiple instances.

        long previousLastStartedSequenceNumber = this.lastStartedSequenceNumber;
        try {
            // Indicate to the output stream that are about to write a new record.
            this.outputStream.startNewRecord();

            // Completely serialize the entry. Note that this may span more than one Data Frame.
            this.lastStartedSequenceNumber = operation.getSequenceNumber();
            operation.serialize(this.outputStream);
            this.lastSerializedSequenceNumber = operation.getSequenceNumber();
        }
        catch (IOException ex) {
            // Discard any information that we have about this record (pretty much revert back to where startNewEntry() would have begun writing).
            this.outputStream.discardRecord();
            this.lastSerializedSequenceNumber = previousLastStartedSequenceNumber;
            throw new SerializationException("DataFrameBuilder.append", "Unable to append an operation to the DataFrameOutputStream.", ex);
        }

        // Indicate to the output stream that have finished writing the record.
        this.outputStream.endRecord();
    }

    /**
     * Publishes a data frame to the DataFrameLog. The outcome of the publish operation, whether success or failure, is
     * routed to the appropriate callback handlers given in this constructor.
     *
     * @param dataFrame The data frame to publish.
     * @return True if we were able to publish the Data Frame, false otherwise.
     * @throws NullPointerException     If the data frame is null.
     * @throws IllegalArgumentException If the data frame is not sealed.
     */
    private boolean handleDataFrameComplete(DataFrame dataFrame) {
        if (!dataFrame.isSealed()) {
            throw new IllegalArgumentException("Cannot publish a non-sealed frame.");
        }

        // Write DataFrame to DataFrameLog
        try {
            // TODO: Add to thread pool?
            // TODO: if we want to do double buffering, we need to be careful not to break logic in this class and in OperationQueueProcessor,
            //       which assumes that frames are committed before it moves on processing new elements. Doing Double Buffering will make that
            //       class more complex.
            // TODO: this is a very unnecessary array copy. We need to figure a better way that reduces all this copying around
            //       See what we can do in our code and what we can do in the DLog code (but don't assume Dlog does anything here).
            ByteArraySegment frameSerialization = dataFrame.getData();
            byte[] buffer = new byte[frameSerialization.getLength()];
            frameSerialization.copyTo(buffer, 0, buffer.length);
            this.targetLog.append(buffer, DataFrameWriteTimeout).get();
        }
        catch (Exception ex) {
            // Unable to write the Data Frame that contained all pending entries. The Target Log did try to repeat,
            // but we need to admit failure now.
            CallbackHelpers.invokeSafely(this.dataFrameCommitFailureCallback, ex, null);
            return false;
        }

        CallbackHelpers.invokeSafely(this.dataFrameCommitSuccessCallback, new DataFrameCommitArgs(this.lastSerializedSequenceNumber, this.lastStartedSequenceNumber, dataFrame.getFrameSequence()), null);
        return true;
    }

    /**
     * Closes this instance of the DataFrameBuilder.
     *
     * @throws SerializationException If we were unable to properly close the Builder.
     */
    public void close() throws SerializationException {
        if (!this.closed) {
            // Stop accepting any new items.
            this.closed = true;

            try {
                // Seal & ship whatever frame we currently have (if any).
                this.outputStream.sealCurrentFrame();
            }
            catch (IOException ex) {
                throw new SerializationException("DataFrameBuilder.close", "Unable to seal current frame.", ex);
            }

            // Close the underlying stream (which destroys whatever we have in flight - but there shouldn't be any at this point).
            this.outputStream.close();
        }
    }

    //endregion

    //region DataFrameOutputStream

    /**
     * An OutputStream that abstracts writing to Data Frames. Allows writing arbitrary bytes, and seamlessly transitions
     * from one Data Frame to another if the previous Data Frame was full.
     */
    private class DataFrameOutputStream extends OutputStream {
        //region Members

        private final int maxDataFrameSize;
        private final Supplier<Long> getPreviousFrameSequence;
        private final Function<DataFrame, Boolean> dataFrameCompleteCallback;
        private DataFrame currentFrame;
        private boolean hasDataInCurrentFrame;
        private boolean closed;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the DataFrameOutputStream class.
         *
         * @param maxDataFrameSize          The maximum size, in bytes, of a Data Frame.
         * @param getPreviousFrameSequence  A method that retrieves the Frame Sequence for the previous frame.
         * @param dataFrameCompleteCallback A callback that will be invoked when a Data Frame is full.
         * @throws IllegalArgumentException If maxDataFrameSize is not a positive integer.
         * @throws NullPointerException     If any of the arguments are null.
         */
        public DataFrameOutputStream(int maxDataFrameSize, Supplier<Long> getPreviousFrameSequence, Function<DataFrame, Boolean> dataFrameCompleteCallback) {
            if (maxDataFrameSize <= 0) {
                throw new IllegalArgumentException("maxDataFrameSize must be a positive integer.");
            }

            if (getPreviousFrameSequence == null) {
                throw new NullPointerException("getPreviousFrameSequence");
            }

            if (dataFrameCompleteCallback == null) {
                throw new NullPointerException("dataFrameCompleteCallback");
            }

            this.maxDataFrameSize = maxDataFrameSize;
            this.getPreviousFrameSequence = getPreviousFrameSequence;
            this.dataFrameCompleteCallback = dataFrameCompleteCallback;
        }

        //endregion

        //region DataFrameOutputStream Implementation

        /**
         * Indicates to the stream that a new record is about to be started. All subsequent writes will belong to this record.
         * A record may span multiple data frames (and thus have multiple DataFrame entries), but the DataFrameOutputStream
         * abstracts all of that.
         *
         * @throws SerializationException If we are unable to start a new record.
         */
        public void startNewRecord() throws IOException, SerializationException {
            ensureNotClosed();

            // If there is any data in the current frame, seal it and ship it. And create a new one with StartMagic = Last.EndMagic.
            if (this.currentFrame == null) {
                // No active frame, create a new one.
                createNewFrame();
                startNewRecordInCurrentFrame(true);
            }
            else if (!this.currentFrame.startNewEntry(true)) {
                // Current Frame is full. Need to seal it and start a new one.
                sealCurrentFrame();
                createNewFrame();
                startNewRecordInCurrentFrame(true);
            }
        }

        /**
         * Indicates to the stream that the currently open record is now ended.
         */
        public void endRecord() {
            ensureNotClosed();
            this.currentFrame.endEntry(true);
        }

        /**
         * Indicates to the stream that the currently open record is discarded. If the record spans multiple frames (and thus
         * has multiple DataFrame Entries), the already committed entries will not be discarded. Instead, the DataFrameReader
         * will detect that such a record was discarded and skip over it upon reading.
         */
        public void discardRecord() {
            ensureNotClosed();
            this.currentFrame.discardEntry();
        }

        /**
         * Seals the current frame (if any), and invokes the dataFrameCompleteCallback with the finished frame.
         *
         * @throws IOException            If we were unable to publish the frame.
         * @throws SerializationException If we were unable to seal the frame prior to publishing.
         */
        private void sealCurrentFrame() throws IOException, SerializationException {
            ensureNotClosed();
            if (!this.hasDataInCurrentFrame) {
                // Nothing to do.
                return;
            }

            // Seal the current frame for appends.
            this.currentFrame.seal();

            // Invoke the callback. At the end of this, the frame is committed so we can get rid of it.
            boolean commitSuccess = this.dataFrameCompleteCallback.apply(this.currentFrame);
            if (!commitSuccess) {
                throw new IOException("DataFrame was not committed successfully.");
            }
        }

        private void createNewFrame() {
            if (this.currentFrame != null && !this.currentFrame.isSealed()) {
                throw new IllegalStateException("Cannot create a new frame if we currently have a non-sealed frame.");
            }

            this.currentFrame = new DataFrame(this.getPreviousFrameSequence.get(), this.maxDataFrameSize);
            this.hasDataInCurrentFrame = false;
        }

        private void ensureNotClosed() {
            if (this.closed) {
                throw new ObjectClosedException(this);
            }
        }

        private void startNewRecordInCurrentFrame(boolean firstRecordEntry) throws IOException {
            if (!this.currentFrame.startNewEntry(firstRecordEntry)) {
                throw new IOException("Unable to start a new record.");
            }

            this.hasDataInCurrentFrame = true;
        }

        //endregion

        //region OutputStream Implementation

        @Override
        public void write(int b) throws IOException {
            ensureNotClosed();

            int attemptCount = 0;
            int totalBytesWritten = 0;
            while (totalBytesWritten == 0 && attemptCount < 2) {
                // We attempt to write 1 byte. If append() says it wrote 0 bytes, it means the current frame is full. Seal it and create a new one.
                totalBytesWritten += this.currentFrame.append((byte) b);
                if (totalBytesWritten == 0) {
                    this.currentFrame.endEntry(false); // Close the current entry, and indicate it is not the last one of the record.
                    try {
                        sealCurrentFrame();
                    }
                    catch (SerializationException ex) {
                        throw new IOException("Unable to seal the current frame.", ex);
                    }

                    createNewFrame();
                    startNewRecordInCurrentFrame(false);
                }

                attemptCount++;
            }

            if (totalBytesWritten == 0) {
                throw new IOException("Unable to make progress in serializing to DataFrame.");
            }
        }

        @Override
        public void write(byte[] data, int offset, int length) throws IOException {
            ensureNotClosed();

            int totalBytesWritten = 0;
            int attemptsWithNoProgress = 0;
            while (totalBytesWritten < length) {
                int bytesWritten = this.currentFrame.append(new ByteArraySegment(data, offset + totalBytesWritten, length - totalBytesWritten));
                attemptsWithNoProgress = bytesWritten == 0 ? attemptsWithNoProgress + 1 : 0;
                if (attemptsWithNoProgress > 1) {
                    // We had two consecutive attempts to write to a frame with no progress made.
                    throw new IOException("Unable to make progress in serializing to DataFrame.");
                }

                // Update positions.
                totalBytesWritten += bytesWritten;
                if (totalBytesWritten < length) {
                    // We were only able to write this partially because the current frame is full. Seal it and create a new one.
                    this.currentFrame.endEntry(false);
                    try {
                        sealCurrentFrame();
                    }
                    catch (SerializationException ex) {
                        throw new IOException("Unable to seal the current frame.", ex);
                    }
                    createNewFrame();
                    startNewRecordInCurrentFrame(false);
                }
            }
        }

        @Override
        public void flush() {
            ensureNotClosed();

            // Probably nothing to do.
        }

        @Override
        public void close() {
            // drop current frame and stop accepting any new operation after this.
            if (!this.closed) {
                this.closed = true;
                this.currentFrame = null;
            }
        }

        //endregion
    }

    //endregion

    //region DataFrameCommitArgs

    /**
     * Contains Information about the committal of a DataFrame.
     */
    public class DataFrameCommitArgs {
        private final long lastFullySerializedSequenceNumber;
        private final long lastStartedSequenceNumber;
        private final long dataFrameSequence;

        /**
         * Creates a new instance of the DataFrameCommitArgs class.
         *
         * @param lastFullySerializedSequenceNumber The Sequence Number of the last Log Operation that was fully serialized (and committed).
         * @param lastStartedSequenceNumber         The Sequence Number of the last Log Operation that was started (but not necessarily committed).
         * @param dataFrameSequence                 The Sequence Number of the Data Frame that was committed.
         */
        protected DataFrameCommitArgs(long lastFullySerializedSequenceNumber, long lastStartedSequenceNumber, long dataFrameSequence) {
            this.lastFullySerializedSequenceNumber = lastFullySerializedSequenceNumber;
            this.lastStartedSequenceNumber = lastStartedSequenceNumber;
            this.dataFrameSequence = dataFrameSequence;
        }

        /**
         * Gets a value indicating the Sequence Number of the last Log Operation that was fully serialized (and committed).
         * If this value is different than 'getLastStartedSequenceNumber' then we currently have an Operation that was split
         * across multiple Data Frames, and the value returned from that function represents the Sequence Number for that entry.
         *
         * @return
         */
        public long getLastFullySerializedSequenceNumber() {
            return this.lastFullySerializedSequenceNumber;
        }

        /**
         * Gets a value indicating the Sequence Number of the last Log Operation that was started (but not necessarily committed).
         * If this value is different than 'getLastFullySerializedSequenceNumber' then we currently have an Operation that was split
         * across multiple Data Frames, and the value returned from this function represents the Sequence Number for that entry.
         *
         * @return
         */
        public long getLastStartedSequenceNumber() {
            return this.lastStartedSequenceNumber;
        }

        /**
         * Gets a value indicating the Sequence Number of the Data Frame that was committed.
         *
         * @return
         */
        public long getDataFrameSequence() {
            return this.dataFrameSequence;
        }

        @Override
        public String toString() {
            return String.format("LastFullySerializedSN = %d, LastStartedSN = %d, DataFrameSN = %d", getLastFullySerializedSequenceNumber(), getLastStartedSequenceNumber(), getDataFrameSequence());
        }
    }

    //endregion
}
