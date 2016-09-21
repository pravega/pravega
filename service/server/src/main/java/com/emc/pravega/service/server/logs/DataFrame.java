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
import com.emc.pravega.common.util.BitConverter;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.common.util.CloseableIterator;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;

import java.io.InputStream;

import static com.emc.pravega.common.util.BitConverter.readInt;
import static com.emc.pravega.common.util.BitConverter.writeInt;

/**
 * Helps serialize entries into fixed-size batches. Allows writing multiple records per frame, as well as splitting a record
 * across multiple frames.
 */
public class DataFrame {
    //region Members

    private static final byte CURRENT_VERSION = 0;
    private static final int MIN_ENTRY_LENGTH_NEEDED = EntryHeader.HEADER_SIZE + 1;
    private final ByteArraySegment data;
    private FrameHeader header;
    private ByteArraySegment contents;
    private LogAddress address; // Assigned by DataFrameLog - no need to serialize.

    private int writeEntryStartIndex;
    private EntryHeader writeEntryHeader;
    private int writePosition;
    private boolean sealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrame class with given maximum size and start magic value.
     *
     * @param previousFrameSequence The offset (within the log) of the previous Data Frame. When reading, comparing this
     *                              number with the actual offset from the previous frame ensures that the frames are
     *                              read in order.
     * @param maxSize               The maximum size of the frame, including Frame Header and other control structures
     *                              that the frame may use to organize records.
     * @throws IllegalArgumentException When the value for startMagic is invalid.
     */
    public DataFrame(long previousFrameSequence, int maxSize) {
        this.data = new ByteArraySegment(new byte[maxSize]);
        this.writeEntryStartIndex = -1;
        this.writePosition = 0;
        this.sealed = false;

        formatForWriting(previousFrameSequence);
    }

    /**
     * Creates a new instance of the DataFrame class using the given byte array as serialization source.
     *
     * @param source The source byte array.
     * @throws SerializationException If the source cannot be deserialized into a DataFrame.
     * @throws NullPointerException   If the source is null.
     */
    public DataFrame(byte[] source) throws SerializationException {
        this(new ByteArraySegment(source, 0, source.length));
    }

    /**
     * Creates a new instance of the DataFrame class using the given byte array as serialization source.
     *
     * @param source The source ByteArraySegment.
     * @throws SerializationException If the source cannot be deserialized into a DataFrame.
     * @throws NullPointerException   If the source is null.
     */
    public DataFrame(ByteArraySegment source) throws SerializationException {
        this.data = source.asReadOnly();
        this.writeEntryStartIndex = -1;
        this.writePosition = -1;
        this.sealed = true;
        parse();
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the Frame Address within its serialization chain.
     * This value is not serialized with the data frame, rather it is assigned by the DataFrameBuilder or DataFrameReader
     * when constructing frames.
     */
    public LogAddress getAddress() {
        return this.address;
    }

    /**
     * Sets the Frame Address within its serialization chain..
     * This value is not serialized with the data frame, rather it is assigned by the DataFrameBuilder or DataFrameReader
     * when constructing frames.
     *
     * @param address The address to set.
     */
    public void setAddress(LogAddress address) {
        this.address = address;
    }

    /**
     * Gets a value indicating the the Sequence Number of the previous Frame in the Log. When reading frames, comparing
     * this value with the previous frame's value ensures the frames are read in order.
     */
    public long getPreviousFrameSequence() {
        return this.header.getPreviousFrameSequence();
    }

    /**
     * Gets a value indicating the length, in bytes, of the frame, including the header, contents and any other control
     * structures needed to serialize the frame.
     * When creating new frames (write mode), this value may be less than the 'maxLength' provided in the constructor.
     * When reading frames from a source (read mode), this value may be less than the size of the source.
     * This value is serialized with the frame.
     */
    public int getLength() {
        return this.header.getSerializationLength() + this.header.getContentLength();
    }

    /**
     * Returns a new InputStream representing the serialized form of this frame.
     */
    public InputStream getData() {
        if (this.data.isReadOnly()) {
            return this.data.getReader();
        } else {
            // We have just created this frame. Only return the segment of the buffer that contains data.
            return this.data.getReader(0, this.header.getSerializationLength() + this.header.getContentLength());
        }
    }

    /**
     * Gets a value indicating whether the DataFrame is empty (if it has no entries).
     */
    public boolean isEmpty() {
        return this.header.getContentLength() == 0;
    }

    /**
     * Gets a value indicating whether the DataFrame is sealed.
     */
    public boolean isSealed() {
        return this.sealed || this.contents.isReadOnly();
    }

    //endregion

    //region Writing

    /**
     * Indicates that a new DataFrame Entry should be opened.
     *
     * @param firstRecordEntry If true, this entry will be marked as the first entry in a record (records can be split
     *                         across multiple frames, and this helps ensure that, upon reading, the records are recomposed
     *                         starting with the right Data Frame Entry).
     * @return True if we were able to start an entry in this Frame, or false otherwise (this happens in case the frame is full).
     * If the frame is full, the new entry will need to be started on a new Frame.
     * @throws IllegalStateException If the entry is sealed.
     */
    public boolean startNewEntry(boolean firstRecordEntry) {
        Preconditions.checkState(!this.sealed, "DataFrame is sealed and cannot accept any more entries.");
        endEntry(true);

        if (getAvailableLength() < MIN_ENTRY_LENGTH_NEEDED) {
            // If we cannot fit at least entry header + 1 byte, we cannot record anything in this write frame anymore.
            return false;
        }

        this.writeEntryStartIndex = this.writePosition;
        this.writeEntryHeader = new EntryHeader(this.contents.subSegment(this.writePosition, EntryHeader.HEADER_SIZE));
        this.writeEntryHeader.setFirstRecordEntry(firstRecordEntry);
        this.writePosition += EntryHeader.HEADER_SIZE;
        return true;
    }

    /**
     * Discards the currently started entry and deletes any data associated with it.
     */
    public void discardEntry() {
        if (this.writeEntryStartIndex < 0) {
            // Nothing to do.
        }

        // Revert back to where we began the current entry.
        this.writePosition = this.writeEntryStartIndex;
        this.writeEntryStartIndex = -1;
        this.writeEntryHeader = null;
    }

    /**
     * Indicates that the currently open DataFrame Entry can be ended.
     *
     * @param endOfRecord If true, this entry will be marked as the last entry in a record (records can be split across
     *                    multiple frames, and this helps ensure that, upon read, the records are recomposed ending with
     *                    the right DataFrame Entry).
     * @return True if we have any more space (to start a new record) in this DataFrame, false otherwise.
     */
    public boolean endEntry(boolean endOfRecord) {
        // Check to see if we actually have an entry started.
        if (this.writeEntryStartIndex >= 0) {
            int entryLength = this.writePosition - this.writeEntryStartIndex - EntryHeader.HEADER_SIZE;
            assert entryLength >= 0 : "entryLength is negative.";

            this.writeEntryHeader.setEntryLength(entryLength);
            this.writeEntryHeader.setLastRecordEntry(endOfRecord);
            this.writeEntryHeader.serialize();
            this.writeEntryHeader = null;
            this.writeEntryStartIndex = -1;
        }

        // Check to see if we have enough room to begin another entry.
        return getAvailableLength() >= MIN_ENTRY_LENGTH_NEEDED;
    }

    /**
     * Appends a byte to the DataFrame.
     *
     * @param b The byte to append.
     * @return The number of bytes written. If zero, then the frame is full and we cannot write anything anymore.
     * @throws IllegalStateException If the frame is sealed or no entry has been started.
     */
    public int append(byte b) {
        ensureAppendConditions();
        if (getAvailableLength() >= 1) {
            this.contents.set(writePosition, b);
            writePosition++;
            return 1;
        } else {
            // Current DataFrame is full. we can't write anymore.
            return 0;
        }
    }

    /**
     * Appends the contents of the ByteArraySegment to the DataFrame.
     *
     * @param data The ByteArraySegment to append.
     * @return The number of bytes written. If less than the length of the ByteArraySegment, the frame is full and cannot
     * write anything anymore. The remaining bytes will need to be written to a new frame.
     * @throws IllegalStateException If the frame is sealed or no entry has been started.
     */
    public int append(ByteArraySegment data) {
        ensureAppendConditions();

        int actualLength = Math.min(data.getLength(), getAvailableLength());
        if (actualLength > 0) {
            this.contents.copyFrom(data, writePosition, actualLength);
            writePosition += actualLength;
        }

        return actualLength;
    }

    /**
     * Seals the frame for writing. After this method returns, no more modifications are allowed on this DataFrame.
     * This method has no effect if the Frame is read-only if it is already sealed.
     *
     * @throws IllegalStateException If an open entry exists (entries must be closed prior to sealing).
     */
    public void seal() {
        if (!this.sealed && !this.contents.isReadOnly()) {
            Preconditions.checkState(writeEntryStartIndex < 0, "An open entry exists. Any open entries must be closed prior to sealing.");

            this.header.setContentLength(writePosition);
            this.header.commit();
            this.sealed = true;
        }
    }

    /**
     * Calculates the number of bytes available in the frame for writing.
     *
     * @return
     */
    private int getAvailableLength() {
        return this.contents.getLength() - writePosition;
    }

    /**
     * Formats the DataFrame buffer for writing. Creates a FrameHeader and assigns a content.
     *
     * @param previousFrameSequence
     */
    private void formatForWriting(long previousFrameSequence) {
        Preconditions.checkState(this.header == null && this.contents == null, "DataFrame already contains data; cannot re-format.");

        //We want to use the DataFrame for at least 1 byte of data.
        int sourceLength = this.data.getLength();
        Exceptions.checkArgument(sourceLength > FrameHeader.SERIALIZATION_LENGTH, "data", "Insufficient array length. Byte array must have a length of at least %d.", FrameHeader.SERIALIZATION_LENGTH + 1);

        this.header = new FrameHeader(CURRENT_VERSION, previousFrameSequence, this.data.subSegment(0, FrameHeader.SERIALIZATION_LENGTH));
        this.contents = this.data.subSegment(FrameHeader.SERIALIZATION_LENGTH, sourceLength - FrameHeader.SERIALIZATION_LENGTH);
    }

    private void ensureAppendConditions() {
        Preconditions.checkState(!this.sealed, "DataFrame is sealed.");
        Preconditions.checkState(this.writeEntryStartIndex >= 0, "No entry started.");
    }

    //endregion

    //region Reading

    /**
     * Gets an Iterator that returns all DataFrameEntries in this Data Frame
     *
     * @return
     */
    public CloseableIterator<DataFrameEntry, SerializationException> getEntries() {
        // The true max length differs based on whether we are still writing this frame or if it's read-only.
        int maxLength = this.writePosition >= 0 ? this.writePosition : this.contents.getLength();
        return new DataFrameEntryIterator(this.contents.asReadOnly(), this.address, maxLength);
    }

    private void parse() throws SerializationException {
        // Used for Reading.
        // FrameHeader will validate that the header is indeed a DataFrame header.
        this.header = new FrameHeader(this.data);

        // Check to see that we have enough bytes in the array.
        if (this.data.getLength() < this.header.getSerializationLength() + this.header.getContentLength()) {
            throw new SerializationException("DataFrame.deserialize", String.format("Given buffer has insufficient number of bytes for this DataFrame. Expected %d, actual %d.", this.header.getSerializationLength() + this.header.getContentLength(), this.contents.getLength()));
        }

        if (this.header.getContentLength() == 0) {
            this.contents = this.data.subSegment(0, 0); // Empty contents...
        } else {
            this.contents = this.data.subSegment(this.header.getSerializationLength(), this.header.getContentLength());
        }
    }

    //endregion

    //region EntryHeader

    /**
     * Header for a DataFrameEntry.
     */
    private static class EntryHeader {
        //region Members

        public static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES; // Length(int) + Flags(byte)
        private static final int FLAGS_OFFSET = Integer.BYTES;
        private static final byte FIRST_ENTRY_MASK = 1;
        private static final byte LAST_ENTRY_MASK = 2;
        private int entryLength;
        private boolean firstRecordEntry;
        private boolean lastRecordEntry;
        private ByteArraySegment data;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the EntryHeader class.
         *
         * @param headerContents The ByteArraySegment to use. If the given ByteArraySegment is ReadOnly, it will be used
         *                       for deserialization, otherwise it will be used to serialize the header later on.
         * @throws IllegalArgumentException If the size of the headerContents ByteArraySegment is incorrect.
         */
        public EntryHeader(ByteArraySegment headerContents) {
            Exceptions.checkArgument(headerContents.getLength() == HEADER_SIZE, "headerContents", "Invalid headerContents size. Expected %d, given %d.", HEADER_SIZE, headerContents.getLength());

            if (headerContents.isReadOnly()) {
                // We are reading.
                this.entryLength = readInt(headerContents, 0);
                byte flags = headerContents.get(FLAGS_OFFSET);
                this.firstRecordEntry = (flags & FIRST_ENTRY_MASK) == FIRST_ENTRY_MASK;
                this.lastRecordEntry = (flags & LAST_ENTRY_MASK) == LAST_ENTRY_MASK;
            } else {
                // We are creating a new one.
                this.entryLength = 0;
                this.firstRecordEntry = false;
                this.lastRecordEntry = false;
                this.data = headerContents;
            }
        }

        //endregion

        //region Operations

        /**
         * Serializes the contents of this EntryHeader to the ByteArraySegment given during construction.
         *
         * @throws IllegalStateException If this EntryHeader was deserialized (and not new).
         */
        public void serialize() {
            Preconditions.checkState(this.data != null && !this.data.isReadOnly(), "Cannot serialize a read-only EntryHeader.");

            // Write length.
            BitConverter.writeInt(this.data, 0, this.entryLength);

            // Write flags.
            byte flags = this.firstRecordEntry ? FIRST_ENTRY_MASK : 0;
            flags |= this.lastRecordEntry ? LAST_ENTRY_MASK : 0;
            this.data.set(FLAGS_OFFSET, flags);
        }

        /**
         * Gets a value indicating the length of the Entry, in bytes.
         *
         * @return
         */
        public int getEntryLength() {
            return this.entryLength;
        }

        /**
         * Sets the Entry Length.
         *
         * @param value The length of the Entry, in bytes.
         */
        public void setEntryLength(int value) {
            this.entryLength = value;
        }

        /**
         * Gets a value indicating whether this is the first entry for the record.
         *
         * @return
         */
        public boolean isFirstRecordEntry() {
            return this.firstRecordEntry;
        }

        /**
         * Sets whether this is the first entry for the record.
         *
         * @param value
         */
        public void setFirstRecordEntry(boolean value) {
            this.firstRecordEntry = value;
        }

        /**
         * Gets a value indicating whether this is the last entry for the record.
         *
         * @return
         */
        public boolean isLastRecordEntry() {
            return this.lastRecordEntry;
        }

        /**
         * Sets whether this is the last entry for the record.
         *
         * @param value
         */
        public void setLastRecordEntry(boolean value) {
            this.lastRecordEntry = value;
        }

        @Override
        public String toString() {
            return String.format("Length = %d, FirstEntry = %s, LastEntry = %s", getEntryLength(), isFirstRecordEntry(), isLastRecordEntry());
        }

        //endregion
    }

    //endregion

    //region FrameHeader

    /**
     * Header for the DataFrame.
     */
    private class FrameHeader {
        //region Members

        public static final int SERIALIZATION_LENGTH = Byte.BYTES + Long.BYTES + Integer.BYTES + Byte.BYTES;
        private final long previousFrameSequence;
        private final byte version;
        private int contentLength;
        private int actualSerializationLength;
        private ByteArraySegment buffer;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the FrameHeader class for a writeable frame.
         *
         * @param version               The serialization version for the frame.
         * @param previousFrameSequence The Sequence Number in the Log for the previous Frame.
         * @param target                The target buffer where to write frame contents.
         * @throws NullPointerException     If the target buffer is null.
         * @throws IllegalArgumentException If the target buffer has an incorrect length.
         */
        public FrameHeader(byte version, long previousFrameSequence, ByteArraySegment target) {
            Exceptions.checkArgument(target.getLength() == SERIALIZATION_LENGTH, "target", "Unexpected length for target buffer. Expected %d, given %d.", SERIALIZATION_LENGTH, target.getLength());

            this.version = version;
            this.previousFrameSequence = previousFrameSequence;
            this.actualSerializationLength = SERIALIZATION_LENGTH;
            this.contentLength = 0;
            this.buffer = target;
        }

        /**
         * Creates a new instance of the FrameHeader class for a read-only frame (deserialization constructor).
         *
         * @param source The source ByteArraySegment to read from.
         * @throws SerializationException If we are unable to deserialize the header.
         */
        public FrameHeader(ByteArraySegment source) throws SerializationException {
            if (source == null || source.getLength() == 0) {
                throw new SerializationException("DataFrame.Header.deserialize", "Null or empty source buffer.");
            }

            int sourceOffset = 0;
            this.version = source.get(sourceOffset);
            sourceOffset += Byte.BYTES;
            this.actualSerializationLength = SERIALIZATION_LENGTH; // This will change based on serialization version.
            if (source.getLength() < this.actualSerializationLength) {
                throw new SerializationException("DataFrame.Header.deserialize", "DataFrame.Header has insufficient number of bytes given its serialization version.");
            }

            this.previousFrameSequence = BitConverter.readLong(source, sourceOffset);
            sourceOffset += Long.BYTES;
            this.contentLength = readInt(source, sourceOffset);
            sourceOffset += Integer.BYTES;
            byte flags = source.get(sourceOffset);
            decodeFlags(flags, version);
            this.buffer = null;
        }

        //endregion

        //region Operations

        /**
         * Commits (serializes) the contents of the FrameHeader to the ByteArraySegment given during construction.
         *
         * @throws IllegalStateException If this FrameHeader was created from a read-only buffer (it was deserialized).
         */
        public void commit() {
            Preconditions.checkState(this.buffer != null && !this.buffer.isReadOnly(), "Cannot commit a read-only FrameHeader");
            assert this.buffer.getLength() == SERIALIZATION_LENGTH;

            // We already checked the size of the target buffer (in the constructor); no need to do it here again.
            int bufferOffset = 0;
            this.buffer.set(bufferOffset, this.version);
            bufferOffset += Byte.BYTES;
            bufferOffset += BitConverter.writeLong(this.buffer, bufferOffset, this.previousFrameSequence);
            bufferOffset += writeInt(this.buffer, bufferOffset, this.contentLength);
            this.buffer.set(bufferOffset, encodeFlags());
        }

        /**
         * Gets a value indicating the Sequence Number of the previous Frame in the Log.
         *
         * @return
         */
        public long getPreviousFrameSequence() {
            return this.previousFrameSequence;
        }

        /**
         * Gets a value indicating the Serialization Version for the frame.
         *
         * @return
         */
        public byte getVersion() {
            return this.version;
        }

        /**
         * Gets a value indicating the length of the Frame's payload (contents), in bytes.
         *
         * @return
         */
        public int getContentLength() {
            return this.contentLength;
        }

        /**
         * Sets the length of the Frame's payload (contents), in bytes.
         *
         * @param value
         */
        public void setContentLength(int value) {
            this.contentLength = value;
        }

        /**
         * Gets a value indicating the total number of bytes used for serializing this FrameHeader instance.
         *
         * @return
         */
        public int getSerializationLength() {
            return this.actualSerializationLength;
        }

        @Override
        public String toString() {
            return String.format("Version = %d, PrevOffset = %d, ContentLength = %d", getVersion(), getPreviousFrameSequence(), getContentLength());
        }

        private void decodeFlags(byte flags, byte version) {
            // Placeholder method. Nothing to do yet.
        }

        private byte encodeFlags() {
            // Placeholder method. Nothing to do yet.
            return 0;
        }

        //endregion
    }

    //endregion

    //region DataFrameEntry

    /**
     * Represents an Entry in the DataFrame.
     */
    public class DataFrameEntry {
        private final boolean firstRecordEntry;
        private final boolean lastRecordEntry;
        private final boolean lastEntryInDataFrame;
        private final LogAddress frameAddress;
        private final ByteArraySegment data;

        /**
         * Creates a new instance of the DataFrameEntry class.
         *
         * @param header               The Header for this entry.
         * @param data                 A ByteArraySegment representing the contents of this frame.
         * @param frameAddress         The Address of the containing DataFrame.
         * @param lastEntryInDataFrame Whether this is the last entry in the DataFrame.
         */
        private DataFrameEntry(EntryHeader header, ByteArraySegment data, LogAddress frameAddress, boolean lastEntryInDataFrame) {
            this.firstRecordEntry = header.isFirstRecordEntry();
            this.lastRecordEntry = header.isLastRecordEntry();
            this.lastEntryInDataFrame = lastEntryInDataFrame;
            this.frameAddress = frameAddress;
            this.data = data;
        }

        /**
         * Gets a value indicating whether this is the first entry for a record.
         *
         * @return
         */
        public boolean isFirstRecordEntry() {
            return this.firstRecordEntry;
        }

        /**
         * Gets a value indicating whether this is the last entry for a record.
         *
         * @return
         */
        public boolean isLastRecordEntry() {
            return this.lastRecordEntry;
        }

        /**
         * Gets a value indicating whether this is the last entry in its containing DataFrame.
         *
         * @return
         */
        public boolean isLastEntryInDataFrame() {
            return this.lastEntryInDataFrame;
        }

        /**
         * Gets a value indicating Address of the containing DataFrame.
         *
         * @return
         */
        public LogAddress getDataFrameAddress() {
            return this.frameAddress;
        }

        /**
         * Gets a ByteArraySegment with the contents of the entry.
         *
         * @return
         */
        public ByteArraySegment getData() {
            return this.data;
        }

        @Override
        public String toString() {
            return String.format("Address = %s, Size = %d, First = %s, Last = %s, LastInDataFrame = %s", this.frameAddress, getData().getLength(), isFirstRecordEntry(), isLastRecordEntry(), isLastEntryInDataFrame());
        }
    }

    //endregion

    //region DataFrameEntryIterator

    /**
     * Represents an iterator over all entries within a DataFrame.
     */
    private class DataFrameEntryIterator implements CloseableIterator<DataFrameEntry, SerializationException> {
        private final ByteArraySegment contents;
        private final LogAddress frameAddress;
        private int currentPosition;
        private int maxLength;

        /**
         * Creates a new instance of the DataFrameEntryIterator class.
         *
         * @param contents     A ByteArraySegment with the contents of the DataFrame.
         * @param frameAddress The Address of the Data Frame.
         * @param maxLength    The maximum number of bytes to consider reading from the contents.
         */
        public DataFrameEntryIterator(ByteArraySegment contents, LogAddress frameAddress, int maxLength) {
            this.contents = contents;
            this.frameAddress = frameAddress;
            this.maxLength = maxLength;
            this.currentPosition = 0;
        }

        //region AutoCloseable Implementation

        @Override
        public void close() {
            this.currentPosition = maxLength + 1;
        }

        //endregion

        //region CloseableIterator Implementation

        @Override
        public DataFrameEntry getNext() throws SerializationException {
            if (!hasMoreElements()) {
                return null;
            }

            // Integrity check. This means that we have a corrupt frame.
            if (this.currentPosition + EntryHeader.HEADER_SIZE > this.maxLength) {
                throw new SerializationException("DataFrame.deserialize", String.format("Data Frame is corrupt. Expected Entry Header at position %d but it does not fit in Frame's length of %d.", this.currentPosition, this.maxLength));
            }

            // Determine the length of the next record && advance the position by the appropriate amount of bytes.
            EntryHeader header = new EntryHeader(this.contents.subSegment(this.currentPosition, EntryHeader.HEADER_SIZE)); // TODO: fix for versioning
            this.currentPosition += EntryHeader.HEADER_SIZE;

            // Integrity check. This means that we have a corrupt frame.
            if (this.currentPosition + header.getEntryLength() > this.maxLength) {
                throw new SerializationException("DataFrame.deserialize", String.format("Data Frame is corrupt. Found Entry Length %d at position %d which is outside of the Frame's length of %d.", header.getEntryLength(), this.currentPosition - EntryHeader.HEADER_SIZE, this.maxLength));
            }

            // Get the result contents && advance the positions.
            ByteArraySegment resultContents = this.contents.subSegment(this.currentPosition, header.getEntryLength());
            this.currentPosition += header.getEntryLength();
            return new DataFrameEntry(header, resultContents, this.frameAddress, !this.hasMoreElements());
        }

        private boolean hasMoreElements() {
            return this.currentPosition < this.maxLength;
        }

        //endregion
    }

    //endregion
}
