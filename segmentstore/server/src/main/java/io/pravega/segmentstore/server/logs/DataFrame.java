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

import io.pravega.segmentstore.storage.LogAddress;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.Setter;

/**
 * Helps serialize entries into fixed-size batches. Allows writing multiple records per frame, as well as splitting a record
 * across multiple frames.
 */
@NotThreadSafe
public abstract class DataFrame {
    /**
     * The Frame Address within its serialization chain.
     * This value is not serialized with the data frame, rather it is assigned by the DataFrameBuilder or DataFrameReader
     * when constructing frames.
     */
    @Getter
    @Setter
    private LogAddress address;

    /**
     * Gets a value indicating the length, in bytes, of the frame, including the header, contents and any other control
     * structures needed to serialize the frame.
     * When creating new frames (write mode), this value may be less than the 'maxLength' provided in the constructor.
     * When reading frames from a source (read mode), this value may be less than the size of the source.
     * This value is serialized with the frame.
     */
    abstract int getLength();

    //region EntryHeader

    /**
     * Header for a DataFrameEntry.
     */
    protected static abstract class EntryHeader {
        static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES; // Length(int) + Flags(byte)
        static final int FLAGS_OFFSET = Integer.BYTES;
        static final byte FIRST_ENTRY_MASK = 1;
        static final byte LAST_ENTRY_MASK = 2;

        /**
         * The length of the Entry, in bytes.
         */
        abstract int getEntryLength();

        /**
         * Whether this is the first entry for the record.
         */
        abstract boolean isFirstRecordEntry();

        /**
         * Whether this is the last entry for the record.
         */
        abstract boolean isLastRecordEntry();

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
    protected static abstract class FrameHeader {
        static final int SERIALIZATION_LENGTH = Byte.BYTES + Integer.BYTES + Byte.BYTES;

        /**
         * The serialization Version for the frame.
         */
        abstract byte getVersion();

        /**
         * The length of the Frame's payload (contents), in bytes.
         */
        abstract int getContentLength();

        @Override
        public String toString() {
            return String.format("Version = %d, ContentLength = %d", getVersion(), getContentLength());
        }
    }

    //endregion
}
