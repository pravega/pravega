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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.LogAddress;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import static io.pravega.common.util.BitConverter.readInt;
import static io.pravega.common.util.BitConverter.writeInt;

/**
 * Helps serialize entries into fixed-size batches. Allows writing multiple records per frame, as well as splitting a record
 * across multiple frames.
 */
@NotThreadSafe
public class DataFrame {
    //region Members

    static final int MIN_ENTRY_LENGTH_NEEDED = EntryHeader.HEADER_SIZE + 1;
    private static final byte CURRENT_VERSION = 0;
    private final ByteArraySegment data;
    private FrameHeader header;
    private ByteArraySegment contents;

    /**
     * The Frame Address within its serialization chain.
     * This value is not serialized with the data frame, rather it is assigned by the DataFrameBuilder or DataFrameReader
     * when constructing frames.
     */
    @Getter
    @Setter
    private LogAddress address;

    private int writeEntryStartIndex;
    private EntryHeader writeEntryHeader;
    private int writePosition;
    private boolean sealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of a DataFrame.
     *
     * @param source The ByteArraySegment to wrap.
     */
    private DataFrame(ByteArraySegment source) {
        this.data = source;
        this.writeEntryStartIndex = -1;
        this.sealed = source.isReadOnly();
        this.writePosition = this.sealed ? -1 : 0;
    }

    /**
     * Creates a new instance of the DataFrame class with given maximum size.
     * @param maxSize               The maximum size of the frame, including Frame Header and other control structures
     *                              that the frame may use to organize records.
     * @throws IllegalArgumentException When the value for startMagic is invalid.
     */
    @VisibleForTesting
    static DataFrame ofSize(int maxSize) {
        return wrap(new ByteArraySegment(new byte[maxSize]));
    }

    /**
     * Creates a new instance of the DataFrame class using the given InputStream as serialization source.
     *
     * @param source An InputStream containing the serialization for this DataFrame.
     * @param length The length of the serialization in the given InputStream.
     * @throws IOException            If the given InputStream could not be read.
     * @throws SerializationException If the source cannot be deserialized into a DataFrame.
     * @throws NullPointerException   If the source is null.
     */
    static DataFrame from(InputStream source, int length) throws IOException, SerializationException {
        ByteArraySegment s = new ByteArraySegment(StreamHelpers.readAll(source, length), 0, length, true);
        DataFrame f = new DataFrame(s);
        f.parse();
        return f;
    }

    /**
     * Creates a new instance of the DataFrame class using the given byte array as target. The target will be formatted
     * to be writable.
     *
     * @param target The source ByteArraySegment.
     * @throws NullPointerException If the source is null.
     */
    static DataFrame wrap(ByteArraySegment target) {
        Preconditions.checkArgument(!target.isReadOnly(), "Cannot deserialize non-readonly source.");
        DataFrame f = new DataFrame(target);
        f.formatForWriting();
        return f;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the length, in bytes, of the frame, including the header, contents and any other control
     * structures needed to serialize the frame.
     * When creating new frames (write mode), this value may be less than the 'maxLength' provided in the constructor.
     * When reading frames from a source (read mode), this value may be less than the size of the source.
     * This value is serialized with the frame.
     */
    int getLength() {
        return this.header.getSerializationLength() + this.header.getContentLength();
    }

    /**
     * Returns an ArrayView representing the serialized form of this frame.
     */
    ArrayView getData() {
        if (this.data.isReadOnly()) {
            return this.data;
        } else {
            // We have just created this frame. Only return the segment of the buffer that contains data.
            return this.data.subSegment(0, getLength());
        }
    }

    /**
     * Gets a value indicating whether the DataFrame is empty (if it has no entries).
     */
    boolean isEmpty() {
        return this.header.getContentLength() == 0;
    }

    /**
     * Gets a value indicating whether the DataFrame is sealed.
     */
    boolean isSealed() {
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
    boolean startNewEntry(boolean firstRecordEntry) {
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
    void discardEntry() {
        if (this.writeEntryStartIndex < 0) {
            // Nothing to do.
            return;
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
    boolean endEntry(boolean endOfRecord) {
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
    int append(byte b) {
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
    int append(ByteArraySegment data) {
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
    void seal() {
        if (!this.sealed && !this.contents.isReadOnly()) {
            Preconditions.checkState(writeEntryStartIndex < 0, "An open entry exists. Any open entries must be closed prior to sealing.");

            this.header.setContentLength(writePosition);
            this.header.commit();
            this.sealed = true;
        }
    }

    /**
     * Calculates the number of bytes available in the frame for writing.
     */
    private int getAvailableLength() {
        return this.contents.getLength() - writePosition;
    }

    /**
     * Formats the DataFrame buffer for writing. Creates a FrameHeader and assigns a content.
     */
    private void formatForWriting() {
        Preconditions.checkState(this.header == null && this.contents == null, "DataFrame already contains data; cannot re-format.");

        //We want to use the DataFrame for at least 1 byte of data.
        int sourceLength = this.data.getLength();
        Exceptions.checkArgument(sourceLength > FrameHeader.SERIALIZATION_LENGTH, "data",
                "Insufficient array length. Byte array must have a length of at least %d.", FrameHeader.SERIALIZATION_LENGTH + 1);

        this.header = new FrameHeader(CURRENT_VERSION, this.data.subSegment(0, FrameHeader.SERIALIZATION_LENGTH));
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
     */
    CloseableIterator<DataFrameEntry, SerializationException> getEntries() {
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

        static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES; // Length(int) + Flags(byte)
        private static final int FLAGS_OFFSET = Integer.BYTES;
        private static final byte FIRST_ENTRY_MASK = 1;
        private static final byte LAST_ENTRY_MASK = 2;

        /**
         * The length of the Entry, in bytes.
         */
        @Getter
        @Setter
        private int entryLength;

        /**
         * Whether this is the first entry for the record.
         */
        @Getter
        @Setter
        private boolean firstRecordEntry;


        /**
         * Whether this is the last entry for the record.
         */
        @Getter
        @Setter
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
        EntryHeader(ByteArraySegment headerContents) {
            Exceptions.checkArgument(headerContents.getLength() == HEADER_SIZE, "headerContents",
                    "Invalid headerContents size. Expected %d, given %d.", HEADER_SIZE, headerContents.getLength());

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
        void serialize() {
            Preconditions.checkState(this.data != null && !this.data.isReadOnly(), "Cannot serialize a read-only EntryHeader.");

            // Write length.
            BitConverter.writeInt(this.data, 0, this.entryLength);

            // Write flags.
            byte flags = this.firstRecordEntry ? FIRST_ENTRY_MASK : 0;
            flags |= this.lastRecordEntry ? LAST_ENTRY_MASK : 0;
            this.data.set(FLAGS_OFFSET, flags);
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
    private static class FrameHeader {
        //region Members

        static final int SERIALIZATION_LENGTH = Byte.BYTES + Integer.BYTES + Byte.BYTES;

        /**
         * The serialization Version for the frame.
         */
        @Getter
        private final byte version;

        /**
         * The length of the Frame's payload (contents), in bytes.
         */
        @Getter
        @Setter
        private int contentLength;

        /**
         * The total number of bytes used for serializing this FrameHeader instance.
         */
        @Getter
        private int serializationLength;
        private ByteArraySegment buffer;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the FrameHeader class for a writable frame.
         *
         * @param version               The serialization version for the frame.
         * @param target                The target buffer where to write frame contents.
         * @throws NullPointerException     If the target buffer is null.
         * @throws IllegalArgumentException If the target buffer has an incorrect length.
         */
        FrameHeader(byte version, ByteArraySegment target) {
            Exceptions.checkArgument(target.getLength() == SERIALIZATION_LENGTH, "target",
                    "Unexpected length for target buffer. Expected %d, given %d.", SERIALIZATION_LENGTH, target.getLength());

            this.version = version;
            this.serializationLength = SERIALIZATION_LENGTH;
            this.contentLength = 0;
            this.buffer = target;
        }

        /**
         * Creates a new instance of the FrameHeader class for a read-only frame (deserialization constructor).
         *
         * @param source The source ByteArraySegment to read from.
         * @throws SerializationException If we are unable to deserialize the header.
         */
        FrameHeader(ByteArraySegment source) throws SerializationException {
            if (source == null || source.getLength() == 0) {
                throw new SerializationException("DataFrame.Header.deserialize", "Null or empty source buffer.");
            }

            int sourceOffset = 0;
            this.version = source.get(sourceOffset);
            sourceOffset += Byte.BYTES;
            this.serializationLength = SERIALIZATION_LENGTH; // This will change based on serialization version.
            if (source.getLength() < this.serializationLength) {
                throw new SerializationException("DataFrame.Header.deserialize", "DataFrame.Header has insufficient number of bytes given its serialization version.");
            }

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
            bufferOffset += writeInt(this.buffer, bufferOffset, this.contentLength);
            this.buffer.set(bufferOffset, encodeFlags());
        }

        @Override
        public String toString() {
            return String.format("Version = %d, ContentLength = %d", getVersion(), getContentLength());
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
    static class DataFrameEntry {
        /**
         * whether this is the first entry for a record.
         */
        @Getter
        private final boolean firstRecordEntry;

        /**
         * whether this is the last entry for a record.
         */
        @Getter
        private final boolean lastRecordEntry;

        /**
         * whether this is the last entry in its containing DataFrame.
         */
        @Getter
        private final boolean lastEntryInDataFrame;

        /**
         * The address of the containing DataFrame
         */
        @Getter
        private final LogAddress frameAddress;

        /**
         * The contents of the entry
         */
        @Getter
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

        @Override
        public String toString() {
            return String.format("Address = %s, Size = %d, First = %s, Last = %s, LastInDataFrame = %s", this.frameAddress,
                    getData().getLength(), isFirstRecordEntry(), isLastRecordEntry(), isLastEntryInDataFrame());
        }
    }

    //endregion

    //region DataFrameEntryIterator

    /**
     * Represents an iterator over all entries within a DataFrame.
     */
    @RequiredArgsConstructor
    private static class DataFrameEntryIterator implements CloseableIterator<DataFrameEntry, SerializationException> {
        private final ByteArraySegment contents;
        private final LogAddress frameAddress;
        private final int maxLength;
        private int currentPosition = 0;

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
            EntryHeader header = new EntryHeader(this.contents.subSegment(this.currentPosition, EntryHeader.HEADER_SIZE));
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
