/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.BufferViewBuilder;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
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
import lombok.RequiredArgsConstructor;
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
    static final int INITIAL_READ_LENGTH = EntrySerializer.HEADER_LENGTH + EntrySerializer.MAX_KEY_LENGTH;

    private final TimeoutTimer timer;
    private final BufferViewBuilder readData;
    @Getter
    private final CompletableFuture<ResultT> result;
    private final EntrySerializer serializer;
    @Getter(AccessLevel.PROTECTED)
    private EntrySerializer.Header header;
    private final long keyVersion;

    //endregion

    //region Constructor and Static Methods

    /**
     * Creates a new instance of the AsyncTableEntryReader class.
     *
     * @param keyVersion The version of the item that is located at this position.
     * @param serializer The {@link EntrySerializer} to use.
     * @param timer Timer for the whole operation.
     */
    private AsyncTableEntryReader(long keyVersion, @NonNull EntrySerializer serializer, @NonNull TimeoutTimer timer) {
        this.keyVersion = keyVersion;
        this.serializer = serializer;
        this.timer = timer;
        this.readData = BufferView.builder();
        this.result = new CompletableFuture<>();
    }

    /**
     * Creates a new {@link AsyncTableEntryReader} that can be used to read a {@link TableEntry} with a an optional
     * matching key.
     *
     * @param soughtKey  (Optional) A {@link BufferView} representing the Key to match. If provided, a {@link TableEntry}
     *                   will only be returned if its {@link TableEntry#getKey()} matches this value.
     * @param keyVersion The version of the {@link TableEntry} that is located at this position. This will be used for
     *                   constructing the result and has no bearing on the reading/matching logic.
     * @param serializer The {@link EntrySerializer} to use for deserializing the {@link TableEntry} instance.
     * @param timer      Timer for the whole operation.
     * @return A new instance of the {@link AsyncTableEntryReader} class. The {@link #getResult()} will be completed with
     * a {@link TableEntry} instance once the Key is matched.
     */
    static AsyncTableEntryReader<TableEntry> readEntry(BufferView soughtKey, long keyVersion, EntrySerializer serializer, TimeoutTimer timer) {
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

    /**
     * Reads a single {@link TableEntry} from the given InputStream. The {@link TableEntry} itself is not constructed,
     * rather all of its components are returned individually.
     *
     * @param input         An InputStream to read from.
     * @param segmentOffset The Segment Offset that the first byte of the InputStream maps to. This wll be used as a Version,
     *                      unless the deserialized segment's Header contains an explicit version.
     * @param serializer    The {@link EntrySerializer} to use for deserializing entries.
     * @return A {@link DeserializedEntry} that contains all the components of the {@link TableEntry}.
     * @throws SerializationException If an Exception occurred while deserializing the {@link DeserializedEntry}.
     */
    static DeserializedEntry readEntryComponents(BufferView.Reader input, long segmentOffset, EntrySerializer serializer) throws SerializationException {
        val h = serializer.readHeader(input);
        long version = getKeyVersion(h, segmentOffset);
        BufferView key = input.readSlice(h.getKeyLength());
        BufferView value = h.isDeletion() ? null :
                (h.getValueLength() == 0 ? BufferView.empty() : input.readSlice(h.getValueLength()));
        return new DeserializedEntry(h, version, key, value);
    }

    //endregion

    //region Internal Operations

    /**
     * When implemented in a derived class, this will process the data read so far.
     *
     * @param readData A {@link BufferView} representing the generated result data so far
     * @return True if the data read so far are enough to generate a result (in which case the read processing will abort),
     * or false if more data are needed (in which case the processing will attempt to resume).
     */
    protected abstract boolean processReadData(BufferView readData);

    /**
     * Completes the result with the given value.
     */
    protected void complete(ResultT result) {
        this.result.complete(result);
    }

    private static long getKeyVersion(EntrySerializer.Header header, long segmentOffset) {
        return header.getEntryVersion() == TableKey.NO_VERSION ? segmentOffset : header.getEntryVersion();
    }

    protected long getKeyVersion() {
        return getKeyVersion(this.header, this.keyVersion);
    }

    protected BufferView compactIfNeeded(BufferView source) {
        val l = source.getLength();
        if (l != 0 && l < source.getAllocatedLength() >> 1) {
            // We compact if we use less than half of the underlying buffer.
            source = new ByteArraySegment(source.getCopy());
        }

        return source;
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
            this.readData.add(entry.getContent().join());
            if (this.header == null && this.readData.getLength() >= EntrySerializer.HEADER_LENGTH) {
                // We now have enough to read the header.
                this.header = this.serializer.readHeader(this.readData.build().getBufferViewReader());
            }

            if (this.header != null) {
                return !processReadData(this.readData.build());
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

    @Override
    public int getMaxReadAtOnce() {
        if (this.result.isDone()) {
            return 0;
        }
        return this.header == null ? INITIAL_READ_LENGTH : this.header.getTotalLength() - this.readData.getLength();
    }

    //endregion

    //region KeyReader

    /**
     * AsyncTableEntryReader implementation that reads a Key from a ReadResult.
     */
    private static class KeyReader extends AsyncTableEntryReader<TableKey> {
        KeyReader(long keyVersion, EntrySerializer serializer, TimeoutTimer timer) {
            super(keyVersion, serializer, timer);
        }

        @Override
        protected boolean processReadData(BufferView readData) {
            val header = getHeader();
            assert header != null : "acceptResult called with no header loaded.";

            if (readData.getLength() >= EntrySerializer.HEADER_LENGTH + header.getKeyLength()) {
                // We read enough information.
                BufferView keyData = compactIfNeeded(readData.slice(header.getKeyOffset(), header.getKeyLength()));
                if (header.isDeletion()) {
                    complete(TableKey.notExists(keyData));
                } else {
                    complete(TableKey.versioned(keyData, getKeyVersion()));
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
        private final BufferView soughtKey;
        private boolean keyValidated;

        private EntryReader(BufferView soughtKey, long keyVersion, EntrySerializer serializer, TimeoutTimer timer) {
            super(keyVersion, serializer, timer);
            this.soughtKey = soughtKey;
            this.keyValidated = soughtKey == null;
        }

        @Override
        @SuppressWarnings("ReturnCount")
        protected boolean processReadData(BufferView readData) {
            val header = getHeader();
            assert header != null : "acceptResult called with no header loaded.";

            // The key has been read.
            if (this.soughtKey != null && header.getKeyLength() != this.soughtKey.getLength()) {
                // Key length mismatch. This is not the Table Entry we're looking for.
                complete(null);
                return false;
            }

            if (readData.getLength() < EntrySerializer.HEADER_LENGTH + header.getKeyLength()) {
                // The key hasn't been fully read. Need more info.
                return false;
            }

            if (!this.keyValidated) {
                // Compare the sought key and the data we read, byte-by-byte.
                BufferView keyData = readData.slice(header.getKeyOffset(), header.getKeyLength());
                if (!this.soughtKey.equals(keyData)) {
                    // Key mismatch.
                    complete(null);
                    return true;
                }

                this.keyValidated = true;
            }

            if (header.isDeletion()) {
                // Deleted key. We cannot read more.
                complete(TableEntry.notExists(getOrReadKey(readData, header)));
                return true;
            }

            if (readData.getLength() < header.getTotalLength()) {
                // The value hasn't been read yet. Need more info.
                return false;
            }

            // Fetch the value and finish up.
            BufferView valueData;
            if (header.getValueLength() == 0) {
                valueData = BufferView.empty();
            } else {
                valueData = compactIfNeeded(readData.slice(header.getValueOffset(), header.getValueLength()));
            }

            complete(TableEntry.versioned(readKey(readData, header), valueData, getKeyVersion()));
            return true; // Now we are truly done.
        }

        private BufferView readKey(BufferView readData, EntrySerializer.Header header) {
            return compactIfNeeded(readData.slice(header.getKeyOffset(), header.getKeyLength()));
        }

        private BufferView getOrReadKey(BufferView readData, EntrySerializer.Header header) {
            return this.soughtKey != null ? this.soughtKey : readKey(readData, header);
        }
    }

    //endregion

    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class DeserializedEntry {
        /**
         * The Entry's Header.
         */
        private final EntrySerializer.Header header;

        /**
         * The computed Entry's Version. If explicitly defined in the Header, this mirrors it, otherwise this is the
         * offset at which this Entry resides in the Segment.
         */
        private final long version;

        /**
         * Key Data.
         */
        private final BufferView key;

        /**
         * Value data. Null if a deletion.
         */
        private final BufferView value;
    }
}