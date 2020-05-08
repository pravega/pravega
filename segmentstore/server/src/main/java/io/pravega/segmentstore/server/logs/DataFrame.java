/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import com.google.common.collect.Iterators;
import io.pravega.common.CompositeByteArrayOutputStream;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.LogAddress;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
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
class DataFrame {
    //region Members

    static final int MIN_ENTRY_LENGTH_NEEDED = EntryHeader.HEADER_SIZE + 1;
    private static final byte CURRENT_VERSION = 0;
    private final int maxSize;
    private final CompositeByteArrayOutputStream sharedDataBuffer;
    private final LinkedList<WriteEntry> entries;

    /**
     * The Frame Address within its serialization chain.
     * This value is not serialized with the data frame, rather it is assigned by the DataFrameBuilder or DataFrameReader
     * when constructing frames.
     */
    @Getter
    @Setter
    private LogAddress address;

    private WriteEntry openEntry;
    @Getter
    private int length;
    private boolean sealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of a DataFrame.
     *
     * @param maxSize The maximum size of the frame, including Frame Header and other control structures
     *                that the frame may use to organize records.
     */
    DataFrame(int maxSize) {
        Preconditions.checkArgument(maxSize > FrameHeader.SERIALIZATION_LENGTH,
                "maxSize must be at least %s.", FrameHeader.SERIALIZATION_LENGTH + 1);
        this.maxSize = maxSize;
        this.sealed = false;
        this.entries = new LinkedList<>();
        this.sharedDataBuffer = new CompositeByteArrayOutputStream(maxSize, 1024);
        this.openEntry = null;
        this.length = FrameHeader.SERIALIZATION_LENGTH;
    }

    //endregion

    //region Properties

    /**
     * Returns a {@link BufferView} representing the serialized form of this frame.
     */
    BufferView getData() {
        Preconditions.checkState(this.sealed, "DataFrame must be sealed to get its contents.");
        byte[] sharedHeaderBuffer = new byte[FrameHeader.SERIALIZATION_LENGTH + this.entries.size() * EntryHeader.HEADER_SIZE];
        int aggregateBufferPosition = 0;

        // Serialize Frame Header.
        FrameHeader header = new FrameHeader(getLength() - FrameHeader.SERIALIZATION_LENGTH);
        ByteArraySegment headerSerialization = new ByteArraySegment(sharedHeaderBuffer, 0, FrameHeader.SERIALIZATION_LENGTH);
        header.serialize(headerSerialization);
        aggregateBufferPosition += headerSerialization.getLength();

        // Stitch the result together.
        ArrayList<Iterator<BufferView>> result = new ArrayList<>(1 + this.entries.size() * 2);
        result.add(Iterators.singletonIterator(headerSerialization));
        for (WriteEntry e : this.entries) {
            // Serialize Entry Header.
            EntryHeader entryHeader = new EntryHeader(e.getLength(), e.isFirstRecordEntry(), e.isLastRecordEntry());
            ByteArraySegment entryHeaderSerialization = new ByteArraySegment(sharedHeaderBuffer, aggregateBufferPosition, EntryHeader.HEADER_SIZE);
            entryHeader.serialize(entryHeaderSerialization);
            aggregateBufferPosition += entryHeaderSerialization.getLength();
            result.add(Iterators.singletonIterator(entryHeaderSerialization));

            // Add the entry's components.
            result.add(e.getComponents());
        }

        return BufferView.wrap(Iterators.concat(result.iterator()));
    }

    /**
     * Gets a value indicating whether the DataFrame is empty (if it has no entries).
     */
    boolean isEmpty() {
        return this.entries.isEmpty();
    }

    /**
     * Gets a value indicating whether the DataFrame is sealed.
     */
    boolean isSealed() {
        return this.sealed;
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

        int maxLength = getAvailableLength() - EntryHeader.HEADER_SIZE;
        this.openEntry = new WriteEntry(maxLength, this.sharedDataBuffer);
        this.openEntry.setFirstRecordEntry(firstRecordEntry);
        return true;
    }

    /**
     * Discards the currently started entry and deletes any data associated with it.
     */
    void discardEntry() {
        if (this.openEntry != null) {
            this.openEntry.discard();
        }
        this.openEntry = null;
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
        if (this.openEntry != null) {
            int entryLength = this.openEntry.getLength();
            assert entryLength >= 0 : "entryLength is negative.";
            this.openEntry.setLastRecordEntry(endOfRecord);
            this.entries.addLast(this.openEntry);
            this.length += entryLength + EntryHeader.HEADER_SIZE;
            this.openEntry = null;
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
        return this.openEntry.append(b);
    }

    /**
     * Appends the contents of the {@link BufferView.Reader} to the DataFrame.
     *
     * @param data The {@link BufferView.Reader} to append.
     * @param copy If true, a copy of the data the given {@link BufferView.Reader} will be added. If false, a reference
     *             to {@link BufferView.Reader#readBytes(int)} will be added.
     * @return The number of bytes written. If less than {@link BufferView.Reader#available()}, the frame is full and cannot
     * write anything anymore. The remaining bytes will need to be written to a new frame.
     * @throws IllegalStateException If the frame is sealed or no entry has been started.
     */
    int append(BufferView.Reader data, boolean copy) {
        ensureAppendConditions();
        return this.openEntry.append(data, copy);
    }

    /**
     * Seals the frame for writing. After this method returns, no more modifications are allowed on this DataFrame.
     * This method has no effect if the Frame is read-only if it is already sealed.
     *
     * @throws IllegalStateException If an open entry exists (entries must be closed prior to sealing).
     */
    void seal() {
        if (!this.sealed) {
            Preconditions.checkState(this.openEntry == null, "An open entry exists. Any open entries must be closed prior to sealing.");
            this.sealed = true;
        }
    }

    /**
     * Calculates the number of bytes available in the frame for writing.
     */
    private int getAvailableLength() {
        return this.maxSize - this.length;
    }

    private void ensureAppendConditions() {
        Preconditions.checkState(!this.sealed, "DataFrame is sealed.");
        Preconditions.checkState(this.openEntry != null, "No entry started.");
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
        FrameHeader header = new FrameHeader(source);
        if (length < FrameHeader.SERIALIZATION_LENGTH + header.getContentLength()) {
            throw new SerializationException(String.format("Given buffer has insufficient number of bytes for this DataFrame. Expected %d, actual %d.",
                    FrameHeader.SERIALIZATION_LENGTH + header.getContentLength(), length));
        }

        BoundedInputStream contents = new BoundedInputStream(source, header.getContentLength());
        return new DataFrameEntryIterator(contents, address, FrameHeader.SERIALIZATION_LENGTH);
    }

    //endregion

    //region EntryHeader

    @RequiredArgsConstructor
    @Getter
    private static class EntryHeader {
        static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES; // Length(int) + Flags(byte)
        static final int FLAGS_OFFSET = Integer.BYTES;
        static final byte FIRST_ENTRY_MASK = 1;
        static final byte LAST_ENTRY_MASK = 2;

        /**
         * The length of the Entry, in bytes.
         */
        private final int entryLength;

        /**
         * Whether this is the first entry for the record.
         */
        private final boolean firstRecordEntry;

        /**
         * Whether this is the last entry for the record.
         */
        private final boolean lastRecordEntry;

        EntryHeader(InputStream inputStream) throws IOException {
            this.entryLength = BitConverter.readInt(inputStream);
            byte flags = (byte) inputStream.read();
            if (flags < 0) {
                throw new EOFException();
            }
            this.firstRecordEntry = (flags & FIRST_ENTRY_MASK) == FIRST_ENTRY_MASK;
            this.lastRecordEntry = (flags & LAST_ENTRY_MASK) == LAST_ENTRY_MASK;
        }

        void serialize(ByteArraySegment data) {
            // Write length.
            BitConverter.writeInt(data, 0, this.entryLength);

            // Write flags.
            byte flags = this.firstRecordEntry ? FIRST_ENTRY_MASK : 0;
            flags |= this.lastRecordEntry ? LAST_ENTRY_MASK : 0;
            data.set(FLAGS_OFFSET, flags);
        }

        @Override
        public String toString() {
            return String.format("Length = %d, FirstEntry = %s, LastEntry = %s", getEntryLength(), isFirstRecordEntry(), isLastRecordEntry());
        }
    }

    //endregion

    //region FrameHeader

    /**
     * Header for the DataFrame.
     */
    @RequiredArgsConstructor
    @Getter
    private static class FrameHeader {
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
        private final int contentLength;

        FrameHeader(int contentLength) {
            this(CURRENT_VERSION, contentLength);
        }

        FrameHeader(InputStream source) throws IOException {
            Preconditions.checkNotNull(source, "source");
            this.version = (byte) source.read();
            if (version < 0) {
                throw new EOFException();
            }

            this.contentLength = BitConverter.readInt(source);
            byte flags = (byte) source.read();
            if (flags < 0) {
                throw new EOFException();
            }
            decodeFlags(flags, version);
        }

        byte encodeFlags() {
            // Placeholder method. Nothing to do yet.
            return 0;
        }

        void decodeFlags(byte flags, byte version) {
            // Placeholder method. Nothing to do yet.
        }

        void serialize(ByteArraySegment buffer) {
            int bufferOffset = 0;
            buffer.set(bufferOffset, getVersion());
            bufferOffset += Byte.BYTES;
            bufferOffset += BitConverter.writeInt(buffer, bufferOffset, getContentLength());
            buffer.set(bufferOffset, encodeFlags());
        }

        @Override
        public String toString() {
            return String.format("Version = %d, ContentLength = %d", getVersion(), getContentLength());
        }
    }

    //endregion

    //region WriteEntry

    @RequiredArgsConstructor
    private static class WriteEntry {
        private final int maxLength;
        private final CompositeByteArrayOutputStream sharedBuffer;
        private final LinkedList<Component> components = new LinkedList<>();
        @Getter
        private int length;
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

        private int getAvailableLength() {
            return this.maxLength - length;
        }

        Iterator<BufferView> getComponents() {
            return Iterators.transform(this.components.iterator(), Component::getData);
        }

        int append(byte b) {
            if (getAvailableLength() >= 1) {
                getByteSequence().data.write(b);
                this.length++;
                return 1;
            } else {
                // Owning DataFrame is full. We can't write anymore.
                return 0;
            }
        }

        @SneakyThrows(IOException.class)
        int append(BufferView.Reader data, boolean copy) {
            int actualLength = Math.min(data.available(), getAvailableLength());
            if (actualLength > 0) {
                BufferView b = data.readBytes(actualLength);
                if (copy) {
                    b.copyTo(getByteSequence().data);
                } else {
                    this.components.addLast(new BufferViewReference(b));
                }
                this.length += actualLength;
            }

            return actualLength;
        }

        void discard() {
            this.components.stream().filter(c -> c instanceof ByteSequence).forEach(c -> ((ByteSequence) c).data.reset());
            this.components.clear();
        }

        private ByteSequence getByteSequence() {
            Component c = this.components.peekLast();
            if (c instanceof ByteSequence) {
                return (ByteSequence) c;
            } else {
                ByteSequence s = new ByteSequence(this.sharedBuffer.slice());
                this.components.addLast(s);
                return s;
            }
        }

        private interface Component {
            BufferView getData();
        }

        @RequiredArgsConstructor
        private static class ByteSequence implements Component {
            private final CompositeByteArrayOutputStream.Slice data;

            @Override
            public BufferView getData() {
                return this.data.asBufferView();
            }
        }

        @RequiredArgsConstructor
        private static class BufferViewReference implements Component {
            @Getter
            private final BufferView data;
        }
    }

    //endregion

    //region ReadEntry

    /**
     * Represents an Entry in the DataFrame.
     */
    public static class ReadEntry {
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
        private ReadEntry(EntryHeader header, BoundedInputStream data, LogAddress frameAddress, boolean lastEntryInDataFrame, int frameOffset) {
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
    static class DataFrameEntryIterator implements CloseableIterator<ReadEntry, IOException> {
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
        public ReadEntry getNext() throws IOException {
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
            EntryHeader header = new EntryHeader(this.contents);

            // Integrity check. This means that we have a corrupt frame.
            if (this.contents.getRemaining() < header.getEntryLength()) {
                throw new SerializationException(String.format("Data Frame is corrupt. Found Entry Length %d which cannot fit in the Frame's remaining length of %d.",
                        header.getEntryLength(), this.contents.getRemaining()));
            }

            // Get the result contents && advance the positions.
            int frameOffset = this.bufferOffset + this.contents.getBound() - this.contents.getRemaining();
            BoundedInputStream resultContents = this.contents.subStream(header.getEntryLength());
            this.lastEntryContents = resultContents;
            return new ReadEntry(header, resultContents, this.frameAddress, reachedEnd(), frameOffset);
        }

        private boolean reachedEnd() {
            return this.contents.getRemaining() <= 0;
        }
    }

    //endregion
}
