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

package com.emc.logservice.server.logs;

import com.emc.nautilus.common.util.ByteArraySegment;
import com.emc.nautilus.common.function.ConsumerWithException;
import com.emc.nautilus.common.Exceptions;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

/**
 * An OutputStream that abstracts writing to Data Frames. Allows writing arbitrary bytes, and seamlessly transitions
 * from one Data Frame to another if the previous Data Frame was full.
 */
class DataFrameOutputStream extends OutputStream {
    //region Members

    private final int maxDataFrameSize;
    private final Supplier<Long> getPreviousFrameSequence;
    private final ConsumerWithException<DataFrame, IOException> dataFrameCompleteCallback;
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
    public DataFrameOutputStream(int maxDataFrameSize, Supplier<Long> getPreviousFrameSequence, ConsumerWithException<DataFrame, IOException> dataFrameCompleteCallback) {
        Exceptions.checkArgument(maxDataFrameSize > 0, "maxDataFrameSize", "Must be a positive integer.");
        Preconditions.checkNotNull(getPreviousFrameSequence, "getPreviousFrameSequence");
        Preconditions.checkNotNull(dataFrameCompleteCallback, "dataFrameCompleteCallback");

        this.maxDataFrameSize = maxDataFrameSize;
        this.getPreviousFrameSequence = getPreviousFrameSequence;
        this.dataFrameCompleteCallback = dataFrameCompleteCallback;
    }

    //endregion

    //region OutputStream Implementation

    @Override
    public void write(int b) throws IOException {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.currentFrame != null, "No current frame exists. Most likely no record is started.");

        int attemptCount = 0;
        int totalBytesWritten = 0;
        while (totalBytesWritten == 0 && attemptCount < 2) {
            // We attempt to write 1 byte. If append() says it wrote 0 bytes, it means the current frame is full. Seal it and create a new one.
            totalBytesWritten += this.currentFrame.append((byte) b);
            if (totalBytesWritten == 0) {
                this.currentFrame.endEntry(false); // Close the current entry, and indicate it is not the last one of the record.
                flush();
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
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.currentFrame != null, "No current frame exists. Most likely no record is started.");

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
                flush();
                createNewFrame();
                startNewRecordInCurrentFrame(false);
            }
        }
    }

    /**
     * Seals the current frame (if any), and invokes the dataFrameCompleteCallback with the finished frame.
     * If the dataFrameCompleteCallback failed (and threw an exception), the current frame will be sealed and therefore
     * the DataFrameOutputStream may not be usable.
     *
     * @throws IOException If we were unable to publish the frame.
     */
    @Override
    public void flush() throws IOException {
        Exceptions.checkNotClosed(this.closed, this);
        if (!this.hasDataInCurrentFrame) {
            // Nothing to do.
            return;
        }

        // Seal the current frame for appends.
        this.currentFrame.seal();

        // Invoke the callback. At the end of this, the frame is committed so we can get rid of it.
        if (!this.currentFrame.isEmpty()) {
            // Only flush something if it's not empty.
            this.dataFrameCompleteCallback.accept(this.currentFrame);
        }

        reset();
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

    //region DataFrameOutputStream Implementation

    /**
     * Indicates to the stream that a new record is about to be started. All subsequent writes will belong to this record.
     * A record may span multiple data frames (and thus have multiple DataFrame entries), but the DataFrameOutputStream
     * abstracts all of that.
     *
     * @throws SerializationException If we are unable to start a new record.
     */
    public void startNewRecord() throws IOException {
        Exceptions.checkNotClosed(this.closed, this);

        // If there is any data in the current frame, seal it and ship it. And create a new one with StartMagic = Last.EndMagic.
        if (this.currentFrame == null) {
            // No active frame, create a new one.
            createNewFrame();
            startNewRecordInCurrentFrame(true);
        } else if (!this.currentFrame.startNewEntry(true)) {
            // Current Frame is full. Need to seal it and start a new one.
            flush();
            createNewFrame();
            startNewRecordInCurrentFrame(true);
        }
    }

    /**
     * Indicates to the stream that the currently open record is now ended.
     */
    public void endRecord() {
        Exceptions.checkNotClosed(this.closed, this);
        if (this.currentFrame != null) {
            this.currentFrame.endEntry(true);
        }
    }

    /**
     * Indicates to the stream that the currently open record is discarded. If the record spans multiple frames (and thus
     * has multiple DataFrame Entries), the already committed entries will not be discarded. Instead, the DataFrameReader
     * will detect that such a record was discarded and skip over it upon reading.
     */
    public void discardRecord() {
        Exceptions.checkNotClosed(this.closed, this);
        if (this.currentFrame != null) {
            this.currentFrame.discardEntry();
        }
    }

    /**
     * Discards all the data currently accumulated in the current frame.
     */
    public void reset() {
        Exceptions.checkNotClosed(this.closed, this);
        this.currentFrame = null;
        this.hasDataInCurrentFrame = false;
    }

    private void createNewFrame() {
        Preconditions.checkState(this.currentFrame == null || this.currentFrame.isSealed(), "Cannot create a new frame if we currently have a non-sealed frame.");

        this.currentFrame = new DataFrame(this.getPreviousFrameSequence.get(), this.maxDataFrameSize);
        this.hasDataInCurrentFrame = false;
    }

    private void startNewRecordInCurrentFrame(boolean firstRecordEntry) throws IOException {
        if (!this.currentFrame.startNewEntry(firstRecordEntry)) {
            throw new IOException("Unable to start a new record.");
        }

        this.hasDataInCurrentFrame = true;
    }

    //endregion
}

