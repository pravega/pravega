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
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import lombok.Getter;
import lombok.Setter;

public class WriteFrame extends DataFrame {
    static final int MIN_ENTRY_LENGTH_NEEDED = WriteEntryHeader.HEADER_SIZE + 1;
    private static final byte CURRENT_VERSION = 0;
    private final ByteArraySegment data;
    private final WriteFrameHeader header;
    private final ByteArraySegment contents;
    private int writeEntryStartIndex;
    private WriteEntryHeader writeEntryHeader;
    private int writePosition;
    private boolean sealed;

    /**
     * Creates a new instance of a DataFrame.
     *
     * @param target The ByteArraySegment to wrap.
     */
    WriteFrame(ByteArraySegment target) {
        Preconditions.checkArgument(!target.isReadOnly(), "Cannot create a WriteFrame for a readonly source.");
        this.data = target;
        this.writeEntryStartIndex = -1;
        this.sealed = target.isReadOnly();
        this.writePosition = this.sealed ? -1 : 0;

        //We want to use the DataFrame for at least 1 byte of data.
        int sourceLength = this.data.getLength();
        Exceptions.checkArgument(sourceLength > FrameHeader.SERIALIZATION_LENGTH, "data",
                "Insufficient array length. Byte array must have a length of at least %d.", FrameHeader.SERIALIZATION_LENGTH + 1);

        this.header = new WriteFrameHeader(CURRENT_VERSION, this.data.subSegment(0, FrameHeader.SERIALIZATION_LENGTH));
        this.contents = this.data.subSegment(FrameHeader.SERIALIZATION_LENGTH, sourceLength - FrameHeader.SERIALIZATION_LENGTH);
    }

    /**
     * Creates a new instance of the DataFrame class with given maximum size.
     *
     * @param maxSize The maximum size of the frame, including Frame Header and other control structures
     *                that the frame may use to organize records.
     * @throws IllegalArgumentException When the value for startMagic is invalid.
     */
    @VisibleForTesting
    static WriteFrame ofSize(int maxSize) {
        return new WriteFrame(new ByteArraySegment(new byte[maxSize]));
    }


    @Override
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
        this.writeEntryHeader = new WriteEntryHeader(this.contents.subSegment(this.writePosition, WriteEntryHeader.HEADER_SIZE));
        this.writeEntryHeader.setFirstRecordEntry(firstRecordEntry);
        this.writePosition += WriteEntryHeader.HEADER_SIZE;
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
            int entryLength = this.writePosition - this.writeEntryStartIndex - WriteEntryHeader.HEADER_SIZE;
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

    private void ensureAppendConditions() {
        Preconditions.checkState(!this.sealed, "DataFrame is sealed.");
        Preconditions.checkState(this.writeEntryStartIndex >= 0, "No entry started.");
    }

    //endregion


    //region WriteEntryHeader

    /**
     * Header for a DataFrameEntry.
     */
    private static class WriteEntryHeader extends EntryHeader {
        //region Members

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
        WriteEntryHeader(ByteArraySegment headerContents) {
            Exceptions.checkArgument(headerContents.getLength() == HEADER_SIZE, "headerContents",
                    "Invalid headerContents size. Expected %d, given %d.", HEADER_SIZE, headerContents.getLength());
            // We are creating a new one.
            this.entryLength = 0;
            this.firstRecordEntry = false;
            this.lastRecordEntry = false;
            this.data = headerContents;
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

        //endregion
    }

    //endregion

    private static class WriteFrameHeader extends FrameHeader {
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
        private final int serializationLength;
        private ByteArraySegment buffer;

        /**
         * Creates a new instance of the FrameHeader class for a writable frame.
         *
         * @param version The serialization version for the frame.
         * @param target  The target buffer where to write frame contents.
         * @throws NullPointerException     If the target buffer is null.
         * @throws IllegalArgumentException If the target buffer has an incorrect length.
         */
        WriteFrameHeader(byte version, ByteArraySegment target) {
            Exceptions.checkArgument(target.getLength() == SERIALIZATION_LENGTH, "target",
                    "Unexpected length for target buffer. Expected %d, given %d.", SERIALIZATION_LENGTH, target.getLength());

            this.version = version;
            this.serializationLength = SERIALIZATION_LENGTH;
            this.contentLength = 0;
            this.buffer = target;
        }

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
            bufferOffset += BitConverter.writeInt(this.buffer, bufferOffset, this.contentLength);
            this.buffer.set(bufferOffset, encodeFlags());
        }


        private byte encodeFlags() {
            // Placeholder method. Nothing to do yet.
            return 0;
        }
    }

}
