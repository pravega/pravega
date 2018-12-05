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
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
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
    private final EntrySerializer serializer;
    @Getter(AccessLevel.PROTECTED)
    private EntrySerializer.Header header;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncTableEntryReader class.
     *
     * @param timer Timer for the whole operation.
     */
    private AsyncTableEntryReader(@NonNull EntrySerializer serializer, @NonNull TimeoutTimer timer) {
        this.serializer = serializer;
        this.timer = timer;
        this.readData = new EnhancedByteArrayOutputStream();
        this.result = new CompletableFuture<>();
    }

    /**
     * Creates a new {@link AsyncTableEntryReader} that can be used to read a {@link TableEntry} with a an optional
     * matching key.
     *
     * @param soughtKey  (Optional) An {@link ArrayView} representing the Key to match. If provided, a {@link TableEntry}
     *                   will only be returned if its {@link TableEntry#getKey()} matches this value.
     * @param keyVersion The version of the {@link TableEntry} that is located at this position. This will be used for
     *                   constructing the result and has no bearing on the reading/matching logic.
     * @param serializer The {@link EntrySerializer} to use for deserializing the {@link TableEntry} instance.
     * @param timer      Timer for the whole operation.
     * @return A new instance of the {@link AsyncTableEntryReader} class. The {@link #getResult()} will be completed with
     * a {@link TableEntry} instance once the Key is matched.
     */
    static AsyncTableEntryReader<TableEntry> readEntry(ArrayView soughtKey, long keyVersion, EntrySerializer serializer, TimeoutTimer timer) {
        return new EntryReader(soughtKey, keyVersion, serializer, timer);
    }

    /**
     * Creates a new {@link AsyncTableEntryReader} that can be used to read a key.
     *
     * @param keyVersion The version of the {@link TableKey} that is located at this position. This will be used for
     *                   constructing the result and has no bearing on the reading/matching logic.
     * @param serializer The {@link EntrySerializer} to use for deserializing the Keys.
     * @param timer      Timer for the whole operation.
     * @return A new instance of the {@link AsyncTableEntryReader} class. The {@link #getResult()} will be completed with
     * an {@link TableKey} instance once a key is read.
     */
    static AsyncTableEntryReader<TableKey> readKey(long keyVersion, EntrySerializer serializer, TimeoutTimer timer) {
        return new KeyReader(keyVersion, serializer, timer);
    }

    //endregion

    //region Internal Operations

    /**
     * When implemented in a derived class, this will process the data read so far.
     *
     * @param readData A {@link ByteArraySegment} representing the generated result data so far
     * @return True if the data read so far are enough to generate a result (in which case the read processing will abort),
     * or false if more data are needed (in which case the processing will attempt to resume).
     */
    protected abstract boolean processReadData(ByteArraySegment readData);

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
        // We only care about data that has already been written, so this implies Cache and Storage.
        // Additionally, given that the Store acks an append prior to inserting it into the cache (but after the metadata
        // update), we may occasionally get Future reads. We should accept those too, as they should be completed shortly.
        return entryType == ReadResultEntryType.Cache || entryType == ReadResultEntryType.Storage || entryType == ReadResultEntryType.Future;
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
            if (this.header == null && this.readData.size() >= EntrySerializer.HEADER_LENGTH) {
                // We now have enough to read the header.
                this.header = this.serializer.readHeader(this.readData.getData());
            }

            if (this.header != null) {
                return !processReadData(this.readData.getData());
            }

            return true; // Not done yet.
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

    //region KeyReader

    /**
     * AsyncTableEntryReader implementation that reads a Key from a ReadResult.
     */
    private static class KeyReader extends AsyncTableEntryReader<TableKey> {
        private final long keyVersion;

        KeyReader(long keyVersion, EntrySerializer serializer, TimeoutTimer timer) {
            super(serializer, timer);
            this.keyVersion = keyVersion;
        }

        @Override
        protected boolean processReadData(ByteArraySegment readData) {
            val header = getHeader();
            assert header != null : "acceptResult called with no header loaded.";

            if (readData.getLength() >= EntrySerializer.HEADER_LENGTH + header.getKeyLength()) {
                // We read enough information.
                ArrayView keyData = readData.subSegment(header.getKeyOffset(), header.getKeyLength());
                if (header.isDeletion()) {
                    complete(TableKey.notExists(keyData));
                } else {
                    complete(TableKey.versioned(keyData, this.keyVersion));
                }

                return true; // We are done.
            }

            return false;
        }
    }

    //endregion

    //region EntryReader

    /**
     * AsyncTableEntryReader implementation that matches a particular Key and returns its TableEntry's Header.
     */
    private static class EntryReader extends AsyncTableEntryReader<TableEntry> {
        private final ArrayView soughtKey;
        private final long keyVersion;
        private boolean keyValidated;

        private EntryReader(ArrayView soughtKey, long keyVersion, EntrySerializer serializer, TimeoutTimer timer) {
            super(serializer, timer);
            this.soughtKey = soughtKey;
            this.keyVersion = keyVersion;
            this.keyValidated = soughtKey == null;
        }

        @Override
        @SuppressWarnings("ReturnCount")
        protected boolean processReadData(ByteArraySegment readData) {
            val header = getHeader();
            assert header != null : "acceptResult called with no header loaded.";

            // The key has been read.
            if (!this.keyValidated) {
                if (header.getKeyLength() != this.soughtKey.getLength()) {
                    // Key length mismatch. This is not the Table Entry we're looking for.
                    complete(null);
                    return false;
                }

                if (readData.getLength() < EntrySerializer.HEADER_LENGTH + this.soughtKey.getLength()) {
                    // The key hasn't been fully read. Need more info.
                    return false;
                }

                // Compare the sought key and the data we read, byte-by-byte.
                ByteArraySegment keyData = readData.subSegment(header.getKeyOffset(), header.getKeyLength());
                for (int i = 0; i < this.soughtKey.getLength(); i++) {
                    if (this.soughtKey.get(i) != keyData.get(i)) {
                        // Key mismatch; no point in continuing.
                        complete(null);
                        return true;
                    }
                }

                this.keyValidated = true;
            }

            if (header.isDeletion()) {
                // Deleted key. We cannot read more.
                complete(TableEntry.notExists(getKeyData(this.soughtKey, readData, header)));
                return true;
            }

            if (readData.getLength() < header.getTotalLength()) {
                // The value hasn't been read yet. Need more info.
                return false;
            }

            // Fetch the value and finish up.
            ArrayView valueData;
            if (header.getValueLength() == 0) {
                valueData = new ByteArraySegment(new byte[0]);
            } else {
                valueData = readData.subSegment(header.getValueOffset(), header.getValueLength());
            }

            complete(TableEntry.versioned(getKeyData(this.soughtKey, readData, header), valueData, this.keyVersion));
            return true; // Now we are truly done.
        }

        private ArrayView getKeyData(ArrayView soughtKey, ByteArraySegment readData, EntrySerializer.Header header) {
            if (soughtKey == null) {
                if (readData.getLength() >= header.getKeyOffset() + header.getKeyLength()) {
                    soughtKey = readData.subSegment(header.getKeyOffset(), header.getKeyLength());
                } else {
                    soughtKey = new ByteArraySegment(new byte[0]);
                }
            }

            return soughtKey;
        }
    }

    //endregion
}