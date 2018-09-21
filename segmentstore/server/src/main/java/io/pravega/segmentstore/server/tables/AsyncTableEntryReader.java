/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

/**
 * Helps read Table Entry components asynchronously by processing successive instances of {@link ReadResultEntry}. Subclasses
 * can be used to read desired Table Entry data from a Segment using a {@link ReadResult} or any derivations of that action.
 *
 * NOTE: This class's derivations are meant to be used as {@link AsyncReadResultHandler}s by an {@link AsyncReadResultProcessor},
 * which processes it in a specific sequence using its own synchronization primitives. As such this class should be considered
 * thread-safe when used in such a manner, but no guarantees are made if it is used otherwise.
 *
 * @param <ResultT> The type of the result.
 */
abstract class AsyncTableEntryReader<ResultT> implements AsyncReadResultHandler {
    //region Members

    private final TimeoutTimer timer;
    private final EnhancedByteArrayOutputStream readData;
    @Getter
    private final CompletableFuture<ResultT> result;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncTableEntryReader class.
     *
     * @param timer Timer for the whole operation.
     */
    private AsyncTableEntryReader(@NonNull TimeoutTimer timer) {
        this.timer = timer;
        this.readData = new EnhancedByteArrayOutputStream();
        this.result = new CompletableFuture<>();
    }

    /**
     * Creates a new {@link AsyncTableEntryReader} that can be used to match a key to a sought one.
     *
     * @param soughtKey  An {@link ArrayView} representing the Key to match.
     * @param serializer The {@link EntrySerializer} to use for deserializing the Keys.
     * @param timer      Timer for the whole operation.
     * @return A new instance of the {@link AsyncTableEntryReader} class. The {@link #getResult()} will be completed with
     * an {@link EntrySerializer.Header} instance once the Key is matched.
     */
    static AsyncTableEntryReader<EntrySerializer.Header> matchKey(ArrayView soughtKey, EntrySerializer serializer, TimeoutTimer timer) {
        return new KeyMatcher(soughtKey, serializer, timer);
    }

    /**
     * Creates a new {@link AsyncTableEntryReader} that can be used to read a key.
     *
     * @param serializer The {@link EntrySerializer} to use for deserializing the Keys.
     * @param timer      Timer for the whole operation.
     * @return A new instance of the {@link AsyncTableEntryReader} class. The {@link #getResult()} will be completed with
     * an {@link ArrayView} instance once a key is read.
     */
    static AsyncTableEntryReader<ArrayView> readKey(EntrySerializer serializer, TimeoutTimer timer) {
        return new KeyReader(serializer, timer);
    }

    /**
     * Creates a new {@link AsyncTableEntryReader} that can be used to read the value of a Table Entry.
     *
     * @param valueLength The length of the value.
     * @param timer       Timer for the whole operation.
     * @return A new instance of the {@link AsyncTableEntryReader} class. The {@link #getResult()} will be completed with
     * an {@link ArrayView} instance once a value is read.
     */
    static AsyncTableEntryReader<ArrayView> readValue(int valueLength, TimeoutTimer timer) {
        return new ValueReader(valueLength, timer);
    }

    //endregion

    //region Internal Operations

    /**
     * When implemented in a derived class, this method will process all the read data so far.
     *
     * @param readData A {@link ByteArraySegment} representing the data read so far.
     * @return True if the desired result has been found (so no more reading will be done), or false if the desired result
     * could not be generated yet and more reading is required.
     * @throws SerializationException If unable to process a key due to a serialization issue.
     */
    protected abstract boolean processReadData(ByteArraySegment readData) throws SerializationException;

    /**
     * Completes the result with the given value.
     */
    protected void complete(ResultT result) {
        this.result.complete(result);
    }

    //endregion

    //region AsyncReadResultHandler implementation

    @Override
    public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
        // We only care about actual data, and the data must have been written. So Cache and Storage are the only entry
        // types we process.
        return entryType == ReadResultEntryType.Cache || entryType == ReadResultEntryType.Storage;
    }

    @Override
    public boolean processEntry(ReadResultEntry entry) {
        if (this.result.isDone()) {
            // We are done. Nothing else to do.
            return false;
        }

        try {
            Preconditions.checkArgument(entry.getContent().isDone(), "Entry Contents is not yet fetched.");
            ReadResultEntryContents contents = entry.getContent().join();

            // TODO: most of these transfers are from memory to memory. It's a pity that we need an extra buffer to do the copy.
            // TODO: https://github.com/pravega/pravega/issues/2924
            this.readData.write(StreamHelpers.readAll(contents.getData(), contents.getLength()));
            return !processReadData(this.readData.getData());
        } catch (Throwable ex) {
            processError(ex);
            return false;
        }
    }

    @Override
    public void processResultComplete() {
        if (!this.result.isDone()) {
            // We've reached the end of the read but couldn't find anything.
            processError(new SerializationException("Reached the end of the ReadResult but unable to read desired data."));
        }
    }

    @Override
    public void processError(Throwable cause) {
        this.result.completeExceptionally(cause);
    }

    @Override
    public Duration getRequestContentTimeout() {
        return this.timer.getRemaining();
    }

    //endregion

    //region HeaderReader

    /**
     * Base implementation for any AsyncTableEntryReader that must first read the Entry's Header.
     */
    static abstract class HeaderReader<T> extends AsyncTableEntryReader<T> {
        private final EntrySerializer serializer;
        @Getter(AccessLevel.PROTECTED)
        private EntrySerializer.Header header;

        private HeaderReader(@NonNull EntrySerializer serializer, TimeoutTimer timer) {
            super(timer);
            this.serializer = serializer;
        }

        /**
         * When implemented in a derived class, this will return the minimum number of bytes needed to read before attempting
         * to generate a result.
         */
        protected abstract int getMinReadLength();

        /**
         * When implemented in a derived class, this will accept the result generated and either invoke {@link #complete}
         * or {@link #processError}.
         *
         * @param readData A {@link ByteArraySegment} representing the generated result data.
         */
        protected abstract void acceptResult(ByteArraySegment readData);

        @Override
        protected boolean processReadData(ByteArraySegment readData) throws SerializationException {
            if (this.header == null && readData.getLength() >= EntrySerializer.HEADER_LENGTH) {
                // We now have enough to read the header.
                this.header = this.serializer.readHeader(readData);
            }

            if (this.header != null && readData.getLength() >= getMinReadLength()) {
                // We read enough information.
                acceptResult(readData);
                return true; // We are done.
            }

            return false; // Not done; must continue reading if possible.
        }
    }

    //endregion

    //region KeyMatcher

    /**
     * AsyncTableEntryReader implementation that matches a particular Key and returns its TableEntry's Header.
     */
    private static class KeyMatcher extends HeaderReader<EntrySerializer.Header> {
        private final ArrayView soughtKey;

        private KeyMatcher(@NonNull ArrayView soughtKey, EntrySerializer serializer, TimeoutTimer timer) {
            super(serializer, timer);
            this.soughtKey = soughtKey;
        }

        @Override
        protected int getMinReadLength() {
            return EntrySerializer.HEADER_LENGTH + this.soughtKey.getLength();
        }

        @Override
        protected void acceptResult(ByteArraySegment readData) {
            val header = getHeader();
            assert header != null : "acceptResult called with no header loaded.";
            if (header.getKeyLength() != this.soughtKey.getLength()) {
                // Key length mismatch.
                complete(null);
            }

            // Compare the sought key and the data we read, byte-by-byte.
            ByteArraySegment keyData = readData.subSegment(header.getKeyOffset(), header.getKeyLength());
            for (int i = 0; i < this.soughtKey.getLength(); i++) {
                if (this.soughtKey.get(i) != keyData.get(i)) {
                    // Key mismatch; no point in continuing.
                    complete(null);
                    return;
                }
            }

            complete(header);
        }

        @Override
        public void processResultComplete() {
            if (!getResult().isDone()) {
                // We've reached the end of the read but couldn't find anything.
                complete(null);
            }
        }
    }

    //endregion

    //region KeyReader

    /**
     * AsyncTableEntryReader implementation that reads a Key from a ReadResult.
     */
    private static class KeyReader extends HeaderReader<ArrayView> {
        KeyReader(EntrySerializer serializer, TimeoutTimer timer) {
            super(serializer, timer);
        }

        @Override
        protected int getMinReadLength() {
            // The minimum length varies based on whether we know the Header or not. If we don't know it, then we need to
            // possibly read more than needed to ensure that we don't terminate prematurely.
            val header = getHeader();
            return EntrySerializer.HEADER_LENGTH + (header == null ? EntrySerializer.MAX_KEY_LENGTH : header.getKeyLength());
        }

        @Override
        protected void acceptResult(ByteArraySegment readData) {
            val header = getHeader();
            assert header != null : "acceptResult called with no header loaded.";
            ArrayView data = readData.subSegment(header.getKeyOffset(), header.getKeyLength());
            complete(data);
        }
    }

    //endregion

    //region ValueReader

    /**
     * AsyncTableEntryReader implementation that reads a Value from a ReadResult.
     */
    private static class ValueReader extends AsyncTableEntryReader<ArrayView> {
        private final int valueLength;

        private ValueReader(int valueLength, TimeoutTimer timer) {
            super(timer);
            Preconditions.checkArgument(valueLength >= 0, "valueLength must be a positive integer.");
            this.valueLength = valueLength;
            if (valueLength == 0) {
                // Value is supposed to have a zero length, so we are already done.
                complete(new ByteArraySegment(new byte[0]));
            }
        }

        @Override
        protected boolean processReadData(ByteArraySegment readData) {
            if (readData.getLength() >= this.valueLength) {
                complete(readData.getLength() == this.valueLength ? readData : readData.subSegment(0, this.valueLength));
                return true; // We are done.
            }

            return false; // We are not done.
        }
    }

    //endregion
}