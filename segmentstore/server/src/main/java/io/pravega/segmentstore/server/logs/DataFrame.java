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
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.LogAddress;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

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
    private WriteFrameHeader header;
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
    private WriteEntryHeader writeEntryHeader;
    private int writePosition;
    private boolean sealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of a DataFrame.
     *
     * @param source The ByteArraySegment to wrap.
     */
    DataFrame(ByteArraySegment source) {
        Preconditions.checkArgument(!source.isReadOnly(), "Cannot create a WriteFrame for a readonly source.");
        this.data = source;
        this.writeEntryStartIndex = -1;
        this.sealed = source.isReadOnly();
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
    static DataFrame ofSize(int maxSize) {
        return new DataFrame(new ByteArraySegment(new byte[maxSize]));
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the length, in bytes, of the frame, including the header, contents and any other control
     * structures needed to serialize the frame.
     * When creating new frames (write mode), this value may be less than the 'maxLength' provided in the constructor.
     * When reading frames from a source (read mode), this value may be less than the size of the source.
     * This value is serialized with the frame.
     * @return The length (bytes) of the frame, including the header, contents and other control structures needed to serialize the frame.
     */
    public int getLength() {
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

    //region Reading

    /**
     * Interprets the given InputStream as a DataFrame and returns a DataFrameEntryIterator for the entries serialized
     * in it.
     *
     * @param source  The InputStream to read from.
     * @param length  The size of the inputStream.
     * @param address The DataFrame's address.
     * @return A new DataFrameEntryIterator.
     * @throws IOException If unable to parse the DataFrame's header from the InputStream.
     */
    public static DataFrameEntryIterator read(InputStream source, int length, LogAddress address) throws IOException {
        // Check to see that we have enough bytes in the InputStream.
        ReadFrameHeader header = new ReadFrameHeader(source);
        if (length < ReadFrameHeader.SERIALIZATION_LENGTH + header.getContentLength()) {
            throw new SerializationException(String.format("Given buffer has insufficient number of bytes for this DataFrame. Expected %d, actual %d.",
                    ReadFrameHeader.SERIALIZATION_LENGTH + header.getContentLength(), length));
        }

        BoundedInputStream contents = new BoundedInputStream(source, header.getContentLength());
        return new DataFrameEntryIterator(contents, address, ReadFrameHeader.SERIALIZATION_LENGTH);
    }

    //endregion

    //region EntryHeader

    private static abstract class EntryHeader {
        static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES; // Length(int) + Flags(byte)
        static final int FLAGS_OFFSET = Integer.BYTES;
        static final byte FIRST_ENTRY_MASK = 1;
        static final byte LAST_ENTRY_MASK = 2;

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

        @Override
        public String toString() {
            return String.format("Length = %d, FirstEntry = %s, LastEntry = %s", getEntryLength(), isFirstRecordEntry(), isLastRecordEntry());
        }
    }

    private static class WriteEntryHeader extends EntryHeader {
        private ByteArraySegment data;

        WriteEntryHeader(ByteArraySegment headerContents) {
            Exceptions.checkArgument(headerContents.getLength() == HEADER_SIZE, "headerContents",
                    "Invalid headerContents size. Expected %d, given %d.", HEADER_SIZE, headerContents.getLength());
            this.data = headerContents;
        }

        void serialize() {
            Preconditions.checkState(this.data != null && !this.data.isReadOnly(), "Cannot serialize a read-only EntryHeader.");

            // Write length.
            BitConverter.writeInt(this.data, 0, getEntryLength());

            // Write flags.
            byte flags = isFirstRecordEntry() ? FIRST_ENTRY_MASK : 0;
            flags |= isLastRecordEntry() ? LAST_ENTRY_MASK : 0;
            this.data.set(FLAGS_OFFSET, flags);
        }
    }

    private static class ReadEntryHeader extends EntryHeader {
        ReadEntryHeader(InputStream inputStream) throws IOException {
            setEntryLength(BitConverter.readInt(inputStream));
            byte flags = (byte) inputStream.read();
            if (flags < 0) {
                throw new EOFException();
            }
            setFirstRecordEntry((flags & FIRST_ENTRY_MASK) == FIRST_ENTRY_MASK);
            setLastRecordEntry((flags & LAST_ENTRY_MASK) == LAST_ENTRY_MASK);
        }
    }

    //endregion

    //region FrameHeader

    /**
     * Header for the DataFrame.
     */
    private static abstract class FrameHeader {
        static final int SERIALIZATION_LENGTH = Byte.BYTES + Integer.BYTES + Byte.BYTES;
        /**
         * The serialization Version for the frame.
         */
        @Getter
        @Setter
        private byte version;

        /**
         * The length of the Frame's payload (contents), in bytes.
         */
        @Getter
        @Setter
        private int contentLength;

        byte encodeFlags() {
            // Placeholder method. Nothing to do yet.
            return 0;
        }

        void decodeFlags(byte flags, byte version) {
            // Placeholder method. Nothing to do yet.
        }

        @Override
        public String toString() {
            return String.format("Version = %d, ContentLength = %d", getVersion(), getContentLength());
        }
    }

    private static class WriteFrameHeader extends FrameHeader {
        /**
         * The total number of bytes used for serializing this FrameHeader instance.
         */
        @Getter
        private final int serializationLength;
        private ByteArraySegment buffer;

        WriteFrameHeader(byte version, ByteArraySegment target) {
            Exceptions.checkArgument(target.getLength() == SERIALIZATION_LENGTH, "target",
                    "Unexpected length for target buffer. Expected %d, given %d.", SERIALIZATION_LENGTH, target.getLength());
            setVersion(version);
            this.serializationLength = SERIALIZATION_LENGTH;
            this.buffer = target;
        }

        void commit() {
            Preconditions.checkState(this.buffer != null && !this.buffer.isReadOnly(), "Cannot commit a read-only FrameHeader");
            assert this.buffer.getLength() == SERIALIZATION_LENGTH;

            // We already checked the size of the target buffer (in the constructor); no need to do it here again.
            int bufferOffset = 0;
            this.buffer.set(bufferOffset, getVersion());
            bufferOffset += Byte.BYTES;
            bufferOffset += BitConverter.writeInt(this.buffer, bufferOffset, getContentLength());
            this.buffer.set(bufferOffset, encodeFlags());
        }
    }

    private static final class ReadFrameHeader extends FrameHeader {
        ReadFrameHeader(InputStream source) throws IOException {
            Preconditions.checkNotNull(source, "source");
            byte version = (byte) source.read();
            setVersion(version);
            if (version < 0) {
                throw new EOFException();
            }

            setContentLength(BitConverter.readInt(source));
            byte flags = (byte) source.read();
            if (flags < 0) {
                throw new EOFException();
            }
            decodeFlags(flags, version);
        }
    }

    //endregion

    //region DataFrameEntry

    /**
     * Represents an Entry in the DataFrame.
     */
    public static class DataFrameEntry {
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
         * The address of the containing DataFrame.
         */
        @Getter
        private final LogAddress frameAddress;

        /**
         * The contents of the entry.
         */
        @Getter
        private final InputStream data;

        /**
         * The length of the entry.
         */
        @Getter
        private final int length;

        /**
         * The offset within the DataFrame where the contents begins.
         */
        @Getter
        private final int frameOffset;

        /**
         * Creates a new instance of the DataFrameEntry class.
         *
         * @param header               The Header for this entry.
         * @param data                 A ByteArraySegment representing the contents of this frame.
         * @param frameAddress         The Address of the containing DataFrame.
         * @param lastEntryInDataFrame Whether this is the last entry in the DataFrame.
         * @param frameOffset          The offset within the DataFrame where this Entry starts.
         */
        private DataFrameEntry(EntryHeader header, BoundedInputStream data, LogAddress frameAddress, boolean lastEntryInDataFrame, int frameOffset) {
            this.firstRecordEntry = header.isFirstRecordEntry();
            this.lastRecordEntry = header.isLastRecordEntry();
            this.lastEntryInDataFrame = lastEntryInDataFrame;
            this.frameAddress = frameAddress;
            this.data = data;
            this.length = data.getBound();
            this.frameOffset = frameOffset;
        }

        @Override
        public String toString() {
            return String.format("Address = %s, Size = %d, First = %s, Last = %s, LastInDataFrame = %s", this.frameAddress,
                    getLength(), isFirstRecordEntry(), isLastRecordEntry(), isLastEntryInDataFrame());
        }
    }

    //endregion

    //region DataFrameEntryIterator

    /**
     * Represents an iterator over all entries within a DataFrame.
     */
    @RequiredArgsConstructor
    static class DataFrameEntryIterator implements CloseableIterator<DataFrameEntry, IOException> {
        private final BoundedInputStream contents;
        @Getter
        private final LogAddress frameAddress;
        private final int bufferOffset;
        private BoundedInputStream lastEntryContents;


        @Override
        @SneakyThrows(IOException.class)
        public void close() {
            closeLast();
            this.contents.close();
        }

        private void closeLast() throws IOException {
            BoundedInputStream last = this.lastEntryContents;
            if (last != null) {
                last.close();
                this.lastEntryContents = null;
            }
        }

        @VisibleForTesting
        int getLength() {
            return this.contents.getBound();
        }

        @Override
        public DataFrameEntry getNext() throws IOException {
            closeLast();
            if (reachedEnd()) {
                return null;
            }

            // Integrity check. This means that we have a corrupt frame.
            if (this.contents.getRemaining() < EntryHeader.HEADER_SIZE) {
                throw new SerializationException(String.format("Data Frame is corrupt. InputStream has insufficient bytes for a new Entry Header (%d).",
                        this.contents.getRemaining()));
            }

            // Determine the length of the next record and advance the position by the appropriate amount of bytes.
            ReadEntryHeader header = new ReadEntryHeader(this.contents);

            // Integrity check. This means that we have a corrupt frame.
            if (this.contents.getRemaining() < header.getEntryLength()) {
                throw new SerializationException(String.format("Data Frame is corrupt. Found Entry Length %d which cannot fit in the Frame's remaining length of %d.",
                        header.getEntryLength(), this.contents.getRemaining()));
            }

            // Get the result contents && advance the positions.
            int frameOffset = this.bufferOffset + this.contents.getBound() - this.contents.getRemaining();
            BoundedInputStream resultContents = this.contents.subStream(header.getEntryLength());
            this.lastEntryContents = resultContents;
            return new DataFrameEntry(header, resultContents, this.frameAddress, reachedEnd(), frameOffset);
        }

        private boolean reachedEnd() {
            return this.contents.getRemaining() <= 0;
        }
    }

    //endregion
}
