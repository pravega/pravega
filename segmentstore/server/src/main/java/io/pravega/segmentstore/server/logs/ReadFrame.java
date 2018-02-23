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

import com.google.common.base.Preconditions;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.CloseableIterator;
import io.pravega.segmentstore.storage.LogAddress;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

public class ReadFrame extends DataFrame {
    @Getter
    private final int length;
    private final BoundedInputStream contents;

    ReadFrame(InputStream source, int length) throws IOException {
        this.length = length;

        // Check to see that we have enough bytes in the InputStream.
        ReadFrameHeader header = new ReadFrameHeader(source);
        if (length < ReadFrameHeader.SERIALIZATION_LENGTH + header.getContentLength()) {
            throw new SerializationException(String.format("Given buffer has insufficient number of bytes for this DataFrame. Expected %d, actual %d.",
                    ReadFrameHeader.SERIALIZATION_LENGTH + header.getContentLength(), length));
        }

        this.contents = new BoundedInputStream(source, header.getContentLength());
    }

    //region Reading

    /**
     * Gets an Iterator that returns all DataFrameEntries in this Data Frame
     */
    CloseableIterator<DataFrameEntry, IOException> getEntries() {
        return new DataFrameEntryIterator(this.contents, getAddress());
    }

    //endregion

    private static class ReadEntryHeader extends EntryHeader {
        @Getter
        private final int entryLength;
        @Getter
        private final boolean firstRecordEntry;
        @Getter
        private final boolean lastRecordEntry;

        ReadEntryHeader(InputStream inputStream) throws IOException {
            this.entryLength = BitConverter.readInt(inputStream);
            byte flags = (byte) inputStream.read();
            if (flags < 0) {
                throw new EOFException();
            }
            this.firstRecordEntry = (flags & FIRST_ENTRY_MASK) == FIRST_ENTRY_MASK;
            this.lastRecordEntry = (flags & LAST_ENTRY_MASK) == LAST_ENTRY_MASK;
        }
    }

    private static final class ReadFrameHeader extends FrameHeader {
        @Getter
        private final byte version;
        @Getter
        private final int contentLength;

        /**
         * Creates a new instance of the FrameHeader class for a read-only frame (deserialization constructor).
         *
         * @param source The source ByteArraySegment to read from.
         * @throws SerializationException If we are unable to deserialize the header.
         */
        ReadFrameHeader(InputStream source) throws IOException {
            if (source == null) {
                throw new SerializationException("Null or empty source buffer.");
            }

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


        private void decodeFlags(byte flags, byte version) {
            // Placeholder method. Nothing to do yet.
        }
    }

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
        private final InputStream data;

        @Getter
        private final int length;

        /**
         * Creates a new instance of the DataFrameEntry class.
         *
         * @param header               The Header for this entry.
         * @param data                 A ByteArraySegment representing the contents of this frame.
         * @param frameAddress         The Address of the containing DataFrame.
         * @param lastEntryInDataFrame Whether this is the last entry in the DataFrame.
         */
        private DataFrameEntry(EntryHeader header, BoundedInputStream data, LogAddress frameAddress, boolean lastEntryInDataFrame) {
            this.firstRecordEntry = header.isFirstRecordEntry();
            this.lastRecordEntry = header.isLastRecordEntry();
            this.lastEntryInDataFrame = lastEntryInDataFrame;
            this.frameAddress = frameAddress;
            this.data = data;
            this.length = data.bound;
        }

        @Override
        public String toString() {
            return String.format("Address = %s, Size = %d, First = %s, Last = %s, LastInDataFrame = %s", this.frameAddress,
                    getLength(), isFirstRecordEntry(), isLastRecordEntry(), isLastEntryInDataFrame());
        }
    }

    //region DataFrameEntryIterator

    /**
     * Represents an iterator over all entries within a DataFrame.
     */
    @RequiredArgsConstructor
    private static class DataFrameEntryIterator implements CloseableIterator<DataFrameEntry, IOException> {
        private final BoundedInputStream contents;
        private final LogAddress frameAddress;
        private BoundedInputStream lastEntryContents;

        //region AutoCloseable Implementation

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

        //endregion

        //region CloseableIterator Implementation

        @Override
        public DataFrameEntry getNext() throws IOException {
            closeLast();
            if (reachedEnd()) {
                return null;
            }

            // Integrity check. This means that we have a corrupt frame.
            if (this.contents.remaining < EntryHeader.HEADER_SIZE) {
                throw new SerializationException(String.format("Data Frame is corrupt. InputStream has insufficient bytes for a new Entry Header (%d).",
                        this.contents.remaining));
            }

            // Determine the length of the next record and advance the position by the appropriate amount of bytes.
            ReadEntryHeader header = new ReadEntryHeader(this.contents);

            // Integrity check. This means that we have a corrupt frame.
            if (this.contents.remaining < header.getEntryLength()) {
                throw new SerializationException(String.format("Data Frame is corrupt. Found Entry Length %d which cannot fit in the Frame's remaining length of %d.",
                        header.getEntryLength(), this.contents.remaining));
            }

            // Get the result contents && advance the positions.
            BoundedInputStream resultContents = this.contents.subStream(header.getEntryLength());
            this.lastEntryContents = resultContents;
            return new DataFrameEntry(header, resultContents, this.frameAddress, reachedEnd());
        }

        private boolean reachedEnd() {
            return this.contents.remaining <= 0;
        }

        //endregion
    }

    //endregion

    /**
     * InputStream wrapper that counts how many bytes were read and prevents over-reading.
     */
    private static class BoundedInputStream extends FilterInputStream {
        private final int bound;
        private int remaining;

        BoundedInputStream(InputStream inputStream, int bound) {
            super(inputStream);
            this.bound = bound;
            this.remaining = bound;
        }

        @Override
        public void close() throws IOException {
            // Skip over the remaining bytes. Do not close the underlying InputStream.
            if (this.remaining > 0) {
                int toSkip = this.remaining;
                long skipped = skip(toSkip);
                if (skipped != toSkip) {
                    throw new SerializationException(String.format("Read %d fewer byte(s) than expected only able to skip %d.", toSkip, skipped));
                }
            } else if (this.remaining < 0) {
                throw new SerializationException(String.format("Read more bytes than expected (%d).", -this.remaining));
            }
        }

        BoundedInputStream subStream(int length) {
            Preconditions.checkArgument(length <= this.remaining, "Too large.");
            this.remaining -= length;
            return new BoundedInputStream(this.in, length);
        }

        @Override
        public int read() throws IOException {
            // Do not allow reading more than we should.
            int r = this.remaining > 0 ? super.read() : -1;
            if (r >= 0) {
                this.remaining--;
            }

            return r;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int readLength = Math.min(length, this.remaining);
            int r = this.in.read(buffer, offset, readLength);
            if (r > 0) {
                this.remaining -= r;
            } else if (length > 0 && this.remaining <= 0) {
                // We have reached our bound.
                return -1;
            }
            return r;
        }

        @Override
        public long skip(long count) throws IOException {
            long r = this.in.skip(Math.min(count, this.remaining));
            this.remaining -= r;
            return r;
        }

        @Override
        public int available() throws IOException {
            return Math.min(this.in.available(), this.remaining);
        }
    }

    //endregion
}

