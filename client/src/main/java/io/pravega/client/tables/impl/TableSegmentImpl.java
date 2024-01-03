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
package io.pravega.client.tables.impl;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.NoSuchKeyException;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.OrderedProcessor;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.Retry;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Implementation for {@link TableSegment}.
 */
class TableSegmentImpl implements TableSegment {
    // region Members

    private static final int MAX_GET_KEY_BATCH_SIZE = TableSegment.MAXIMUM_BATCH_LENGTH / (TableSegment.MAXIMUM_KEY_LENGTH + TableSegment.MAXIMUM_VALUE_LENGTH);
    private static final int MAX_GET_CONCURRENT_REQUESTS = 5;
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(TableSegmentImpl.class));
    private final String segmentName;
    @Getter
    private final long segmentId;
    private final Controller controller;
    private final ConnectionPool connectionPool;
    private final DelegationTokenProvider tokenProvider;
    /**
     * We only retry {@link TokenExpiredException} and {@link ConnectionFailedException}. Any other exceptions are not
     * retryable and should be bubbled up to the caller.
     * <p>
     * These exceptions are thrown by {@link RawClient} and therefore we do not need to handle the underlying
     * {@link WireCommand}s that generate them.
     */
    private final Retry.RetryAndThrowConditionally retry;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConnectionContext writeContext;
    private final ConnectionContext readContext;

    //endregion

    /**
     * Creates a new instance of the {@link TableSegmentImpl} class.
     *
     * @param segment           A {@link Segment} representing the Pravega Table Segment this instance will interact with.
     * @param controller        The {@link Controller} to use.
     * @param connectionPool The {@link ConnectionPool} to use.
     * @param clientConfig      The {@link KeyValueTableClientConfiguration} to use to configure this client.
     * @param tokenProvider     A Token provider.
     */
    TableSegmentImpl(@NonNull Segment segment, @NonNull Controller controller, @NonNull ConnectionPool connectionPool,
                     @NonNull KeyValueTableClientConfiguration clientConfig, DelegationTokenProvider tokenProvider) {
        this.segmentName = segment.getKVTScopedName();
        this.segmentId = segment.getSegmentId();
        this.controller = controller;
        this.connectionPool = connectionPool;
        this.tokenProvider = tokenProvider;
        this.retry = Retry
                .withExpBackoff(clientConfig.getInitialBackoffMillis(), clientConfig.getBackoffMultiple(), clientConfig.getRetryAttempts(), clientConfig.getMaxBackoffMillis())
                .retryWhen(TableSegmentImpl::isRetryableException);
        this.writeContext = new ConnectionContext();
        this.readContext = new ConnectionContext();
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            resetConnections();
            log.info("{}: Closed.", this.segmentName);
        }
    }

    //endregion

    //region TableSegment Implementation

    @Override
    public CompletableFuture<List<TableSegmentKeyVersion>> put(@NonNull Iterator<TableSegmentEntry> tableEntries) {
        val wireEntries = entriesToWireCommand(tableEntries);
        return this.writeContext.execute((state, requestId) -> {
            val request = new WireCommands.UpdateTableEntries(requestId, this.segmentName, state.getToken(), wireEntries, WireCommands.NULL_TABLE_SEGMENT_OFFSET);

            return sendRequest(request, state, WireCommands.TableEntriesUpdated.class)
                    .thenApply(this::fromWireCommand);
        });
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull Iterator<TableSegmentKey> tableKeys) {
        val wireKeys = keysToWireCommand(tableKeys);
        return this.writeContext.execute((state, requestId) -> {
            val request = new WireCommands.RemoveTableKeys(requestId, this.segmentName, state.getToken(), wireKeys, WireCommands.NULL_TABLE_SEGMENT_OFFSET);
            return Futures.toVoid(sendRequest(request, state, WireCommands.TableKeysRemoved.class));
        });
    }

    @Override
    public CompletableFuture<List<TableSegmentEntry>> get(@NonNull Iterator<ByteBuf> keys) {
        val wireKeys = rawKeysToWireCommand(keys);
        val resultBuilder = new GetResultBuilder(wireKeys);
        CompletableFuture<Void> result;
        if (wireKeys.size() <= MAX_GET_KEY_BATCH_SIZE) {
            // The entire request can be satisfied using a single call.
            result = fetchSlice(resultBuilder);
        } else {
            // The request has to be split into multiple calls and then combined.
            val processor = new OrderedProcessor<Void>(MAX_GET_CONCURRENT_REQUESTS, this.connectionPool.getInternalExecutor());
            val futures = new ArrayList<CompletableFuture<Void>>();
            int index = 0;
            while (index < wireKeys.size()) {
                final int sliceStart = index;
                final int sliceLength = Math.min(MAX_GET_KEY_BATCH_SIZE, wireKeys.size() - sliceStart);
                futures.add(processor.execute(() -> fetchSlice(resultBuilder.slice(sliceStart, sliceStart + sliceLength))));
                index += sliceLength;
            }

            result = Futures.allOf(futures);
            result.thenRun(processor::close);
        }

        return result.thenApply(v -> resultBuilder.get());
    }

    private CompletableFuture<Void> fetchSlice(GetResultBuilder resultBuilder) {
        return this.readContext.execute((state, requestId) -> {
            val request = new WireCommands.ReadTable(requestId, this.segmentName, state.getToken(), resultBuilder.getWireKeys());

            return sendRequest(request, state, WireCommands.TableRead.class)
                    .thenAccept(reply -> fromWireCommand(reply.getEntries(), resultBuilder::add));
        });
    }

    @Override
    public AsyncIterator<IteratorItem<TableSegmentKey>> keyIterator(@NonNull SegmentIteratorArgs args) {
        return new TableSegmentIterator<>(
                s -> fetchIteratorItems(s, WireCommands.ReadTableKeys::new, WireCommands.TableKeysRead.class, this::fromWireCommand),
                TableSegmentKey::getKey, args)
                .asSequential(this.connectionPool.getInternalExecutor());
    }

    @Override
    public AsyncIterator<IteratorItem<TableSegmentEntry>> entryIterator(@NonNull SegmentIteratorArgs args) {
        return new TableSegmentIterator<>(
                s -> fetchIteratorItems(s, WireCommands.ReadTableEntries::new, WireCommands.TableEntriesRead.class, reply -> fromWireCommand(reply.getEntries())),
                e -> e.getKey().getKey(), args)
                .asSequential(this.connectionPool.getInternalExecutor());
    }

    @Override
    public CompletableFuture<Long> getEntryCount() {
        return this.readContext.execute((state, requestId) -> {
            val request = new WireCommands.GetTableSegmentInfo(requestId, this.segmentName, state.getToken());

            return sendRequest(request, state, WireCommands.TableSegmentInfo.class)
                    .thenApply(WireCommands.TableSegmentInfo::getEntryCount);
        });
    }

    /**
     * Fetches a collection of items as part of an async iterator.
     *
     * @param args               A {@link SegmentIteratorArgs} that contains initial arguments to the iterator.
     * @param newIteratorRequest Creates a {@link WireCommand} for the iterator items.
     * @param replyClass         Expected {@link WireCommand} reply type.
     * @param getResult          Extracts the result from the reply.
     * @param <ItemT>            Type of the items returned.
     * @param <RequestT>         Wire Command Request Type.
     * @param <ReplyT>           Wire Command Reply Type.
     * @return A CompletableFuture that, when completed, will return the desired result. If the operation failed, this
     * Future will be failed with the appropriate exception.
     */
    private <ItemT, RequestT extends Request & WireCommand, ReplyT extends Reply & WireCommand> CompletableFuture<IteratorItem<ItemT>> fetchIteratorItems(
            SegmentIteratorArgs args, CreateIteratorRequest<RequestT> newIteratorRequest,
            Class<ReplyT> replyClass, Function<ReplyT, List<ItemT>> getResult) {
        return this.readContext.execute((state, requestId) -> {
            val request = newIteratorRequest.apply(requestId, this.segmentName, state.getToken(), args.getMaxItemsAtOnce(),
                    new WireCommands.TableIteratorArgs(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, args.getFromKey(), args.getToKey()));
            return sendRequest(request, state, replyClass)
                    .thenApply(reply -> {
                        val items = getResult.apply(reply);
                        if (items == null || items.isEmpty()) {
                            // We have reached the end.
                            log.debug(requestId, "{}: Reached the end of the {} iterator.", this.segmentName, replyClass.getSimpleName());
                            return null;
                        }
                        return new IteratorItem<>(items);
                    });
        });
    }

    @FunctionalInterface
    private interface CreateIteratorRequest<V extends Request & WireCommand> {
        V apply(long requestId, String segmentName, String delegationToken, int maxEntriesAtOnce, WireCommands.TableIteratorArgs args);
    }

    //endregion

    //region Helpers

    /**
     * Sends a request.
     *
     * @param request    The Request to send.
     * @param state      A {@link ConnectionState} to use for sending the request.
     * @param replyClass A {@link Class} representing the expected type of the reply.
     * @param <RequestT> Type of the request.
     * @param <ReplyT>   Type of the reply.
     * @return A CompletableFuture that, when completed, will contain the reply of the request. If the request failed,
     * this Future will be completed with the appropriate exception.
     */
    private <RequestT extends Request & WireCommand, ReplyT extends Reply & WireCommand> CompletableFuture<ReplyT> sendRequest(
            RequestT request, ConnectionState state, Class<ReplyT> replyClass) {
        return state.getConnection().sendRequest(request.getRequestId(), request)
                .thenApply(reply -> handleReply(request, reply, replyClass));
    }

    /**
     * Processes a {@link Reply} from a {@link RawClient#sendRequest}.
     *
     * @param request           The {@link Request} that was used to get the {@link Reply}.
     * @param reply             The {@link Reply}.
     * @param expectedReplyType A {@link Class} representing the expected type of the reply.
     * @param <RequestT>        Type of the request.
     * @param <ReplyT>          Type of the reply.
     * @return The Reply cast to {@link ReplyT}.
     * @throws ConnectionFailedException If the connection had to be closed.
     * @throws NoSuchKeyException        If the {@link Request} was a conditional update that failed due to the key not
     *                                   existing.
     * @throws BadKeyVersionException    If the {@link Request} was a conditional update that failed due to the provided
     *                                   key version not being correct.
     */
    @SneakyThrows({ConnectionFailedException.class, ConditionalTableUpdateException.class})
    @SuppressWarnings("unchecked")
    private <RequestT extends Request & WireCommand, ReplyT extends Reply & WireCommand> ReplyT handleReply(
            RequestT request, Reply reply, Class<ReplyT> expectedReplyType) {
        log.debug(request.getRequestId(), "{}: Received response. RequestType={} Reply={}.",
                this.segmentName, request.getType(), reply.getClass().getSimpleName());

        if (reply.getClass().equals(expectedReplyType)) {
            // Successful reply.
            return (ReplyT) reply;
        }

        if (reply instanceof WireCommands.TableKeyDoesNotExist) {
            // Conditional update/removal failed: attempted to modify or retrieve a key that does not exist.
            throw new NoSuchKeyException(this.segmentName);
        } else if (reply instanceof WireCommands.TableKeyBadVersion) {
            // Conditional update/removal failed: wrong key version provided.
            throw new BadKeyVersionException(this.segmentName);
        } else {
            // Something unexpected occurred. Reset the connection and throw appropriate exception.
            // WrongHost, ConnectionFailedException and AuthenticationException are already handled by RawClient.
            log.error(request.getRequestId(), "{}: Unexpected reply. Resetting connection. Request={}, Reply={}.", this.segmentName, request, reply);
            resetConnections();
            throw new ConnectionFailedException(String.format("Unexpected reply of %s when expecting %s.", reply, expectedReplyType));
        }
    }

    /**
     * Converts a Collection of Keys to a List of {@link WireCommands.TableKey} instances.
     *
     * @param keys The keys.
     * @return The result.
     */
    private List<WireCommands.TableKey> rawKeysToWireCommand(Iterator<ByteBuf> keys) {
        ArrayList<WireCommands.TableKey> result = new ArrayList<>();
        AtomicInteger serializationLength = new AtomicInteger();
        keys.forEachRemaining(key -> {
            val k = toWireCommand(TableSegmentKey.unversioned(key));
            serializationLength.addAndGet(k.size());
            result.add(k);
        });
        checkBatchSize(result.size(), serializationLength.get());
        return result;
    }

    /**
     * Converts an Iterator of {@link TableSegmentKey}s to a List of {@link WireCommands.TableKey}.
     *
     * @param keys The {@link TableSegmentKey}s.
     * @return The result.
     */
    private List<WireCommands.TableKey> keysToWireCommand(Iterator<TableSegmentKey> keys) {
        ArrayList<WireCommands.TableKey> result = new ArrayList<>();
        AtomicInteger serializationLength = new AtomicInteger();
        keys.forEachRemaining(k -> {
            val key = toWireCommand(k);
            serializationLength.addAndGet(key.size());
            result.add(key);
        });
        checkBatchSize(result.size(), serializationLength.get());
        return result;
    }

    /**
     * Converts an Iterator of {@link TableSegmentEntry} instances to a {@link WireCommands.TableEntries} instance.
     *
     * @param tableEntries The {@link TableSegmentEntry} instances.
     * @return The {@link WireCommands.TableEntries} instance.
     */
    private WireCommands.TableEntries entriesToWireCommand(Iterator<TableSegmentEntry> tableEntries) {
        ArrayList<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> result = new ArrayList<>();
        AtomicInteger serializationLength = new AtomicInteger();
        tableEntries.forEachRemaining(entry -> {
            val key = toWireCommand(entry.getKey());
            val value = toWireCommand(entry.getValue());
            serializationLength.addAndGet(key.size() + value.size());
            result.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
        });
        checkBatchSize(result.size(), serializationLength.get());
        return new WireCommands.TableEntries(result);
    }

    /**
     * Converts a single {@link TableSegmentKey} to a {@link WireCommands.TableKey}.
     *
     * @param k The {@link TableSegmentKey}.
     * @return The {@link WireCommands.TableKey}.
     */
    private WireCommands.TableKey toWireCommand(final TableSegmentKey k) {
        Preconditions.checkArgument(k.getKey().readableBytes() <= TableSegment.MAXIMUM_KEY_LENGTH,
                "Key Length too long. Must be less than %s; given %s.", TableSegment.MAXIMUM_KEY_LENGTH, k.getKey().readableBytes());
        if (k.getVersion() == null || k.getVersion().equals(TableSegmentKeyVersion.NO_VERSION)) {
            // Unconditional update.
            return new WireCommands.TableKey(k.getKey(), WireCommands.TableKey.NO_VERSION);
        } else {
            // Conditional update.
            return new WireCommands.TableKey(k.getKey(), k.getVersion().getSegmentVersion());
        }
    }

    /**
     * Converts the given value to a {@link WireCommands.TableValue}.
     *
     * @param value The value.
     * @return The {@link WireCommands.TableValue}.
     */
    private WireCommands.TableValue toWireCommand(ByteBuf value) {
        Preconditions.checkArgument(value.readableBytes() <= TableSegment.MAXIMUM_VALUE_LENGTH,
                "Value Length too long. Must be less than %s; given %s.", TableSegment.MAXIMUM_VALUE_LENGTH, value.readableBytes());
        return new WireCommands.TableValue(value);
    }

    /**
     * Deserializes a {@link WireCommands.TableKey} to a {@link TableSegmentKey}.
     *
     * @param k The {@link WireCommands.TableKey}.
     * @return The {@link TableSegmentKey}.
     */
    private TableSegmentKey fromWireCommand(WireCommands.TableKey k) {
        if (k.getKeyVersion() == WireCommands.TableKey.NOT_EXISTS) {
            return TableSegmentKey.notExists(k.getData());
        } else {
            return TableSegmentKey.versioned(k.getData(), k.getKeyVersion());
        }
    }

    /**
     * Deserializes the given {@link WireCommands.TableKey}-{@link WireCommands.TableValue} pair into a {@link TableSegmentEntry}.
     *
     * @param e The pair to deserialize.
     * @return A {@link TableSegmentEntry} or null if the {@link TableSegmentKey} does not exist.
     */
    private TableSegmentEntry fromWireCommand(Map.Entry<WireCommands.TableKey, WireCommands.TableValue> e) {
        val key = fromWireCommand(e.getKey());
        if (key.exists()) {
            return new TableSegmentEntry(key, e.getValue().getData());
        } else {
            // No entry found for this key.
            key.getKey().release();
            return null;
        }
    }

    /**
     * Deserializes the {@link TableSegmentKeyVersion}s from a {@link WireCommands.TableEntriesUpdated}.
     *
     * @param reply The {@link WireCommands.TableEntriesUpdated}.
     * @return A List of {@link TableSegmentKeyVersion}s.
     */
    private List<TableSegmentKeyVersion> fromWireCommand(WireCommands.TableEntriesUpdated reply) {
        return reply.getUpdatedVersions()
                .stream()
                .map(TableSegmentKeyVersion::from)
                .collect(Collectors.toList());
    }

    /**
     * Deserializes the {@link TableSegmentKey}s from a {@link WireCommands.TableKeysRead}.
     *
     * @param reply The {@link WireCommands.TableKeysRead}.
     * @return A List of {@link TableSegmentKey}s.
     */
    private List<TableSegmentKey> fromWireCommand(WireCommands.TableKeysRead reply) {
        return reply.getKeys()
                .stream()
                .map(this::fromWireCommand)
                .collect(Collectors.toList());
    }

    /**
     * Deserializes the {@link TableSegmentEntry} instances from a {@link WireCommands.TableEntries}.
     *
     * @param reply The {@link WireCommands.TableEntries}.
     * @return A List of {@link TableSegmentEntry} instances.
     */
    private List<TableSegmentEntry> fromWireCommand(WireCommands.TableEntries reply) {
        val result = new ArrayList<TableSegmentEntry>(reply.getEntries().size());
        fromWireCommand(reply, result::add);
        return result;
    }

    /**
     * Deserializes the {@link TableSegmentEntry} instances from a {@link WireCommands.TableEntries} and invokes the given
     * callback, in order, for each of them.
     *
     * @param reply    The {@link WireCommands.TableEntries}.
     * @param callback A {@link Consumer} that will be invoked for each deserialized {@link TableSegmentEntry}.
     */
    private void fromWireCommand(WireCommands.TableEntries reply, Consumer<TableSegmentEntry> callback) {
        for (val e : reply.getEntries()) {
            callback.accept(fromWireCommand(e));
        }
    }

    /**
     * Closes the current Connections, if any. Any subsequent requests using them will cause them to reinitialize.
     */
    private void resetConnections() {
        this.writeContext.reset();
        this.readContext.reset();
    }

    private static boolean isRetryableException(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        return ex instanceof TokenExpiredException || ex instanceof ConnectionFailedException;
    }

    private void checkBatchSize(int count, int serializationLength) {
        Preconditions.checkArgument(count <= TableSegment.MAXIMUM_BATCH_KEY_COUNT,
                "Too many items. Expected at most %s, actual %s.", TableSegment.MAXIMUM_BATCH_KEY_COUNT, count);
        Preconditions.checkArgument(serializationLength <= TableSegment.MAXIMUM_BATCH_LENGTH,
                "Batch serialization too big. Expected at most %s, actual %s.", TableSegment.MAXIMUM_BATCH_LENGTH, serializationLength);
    }

    //endregion

    //region GetResultBuilder

    /**
     * Helps build the result for get() calls.
     */
    @ThreadSafe
    private static class GetResultBuilder {
        @Getter
        private final List<WireCommands.TableKey> wireKeys;
        @GuardedBy("entries")
        private final TableSegmentEntry[] entries;
        private final int startIndex;
        private final int endIndex;
        @GuardedBy("entries")
        private int index;

        GetResultBuilder(List<WireCommands.TableKey> wireKeys) {
            this(wireKeys, new TableSegmentEntry[wireKeys.size()], 0, wireKeys.size());
        }

        private GetResultBuilder(List<WireCommands.TableKey> wireKeys, TableSegmentEntry[] entries, int startIndex, int endIndex) {
            Preconditions.checkArgument(startIndex >= 0 && startIndex < endIndex && endIndex <= entries.length);
            this.wireKeys = wireKeys;
            this.entries = entries;
            this.startIndex = startIndex;
            this.index = startIndex;
            this.endIndex = endIndex;
        }

        GetResultBuilder slice(int startIndex, int endIndex) {
            synchronized (this.entries) {
                Preconditions.checkArgument(this.startIndex == 0 && this.endIndex == this.entries.length);
                return new GetResultBuilder(this.wireKeys.subList(startIndex, endIndex), this.entries, startIndex, endIndex);
            }
        }

        void add(TableSegmentEntry e) {
            synchronized (this.entries) {
                Preconditions.checkElementIndex(this.index, this.endIndex);
                this.entries[this.index++] = e;
            }
        }

        List<TableSegmentEntry> get() {
            synchronized (this.entries) {
                return Arrays.asList(this.entries);
            }
        }
    }

    //endregion

    //region ConnectionState

    @Data
    private class ConnectionState implements AutoCloseable {
        private final RawClient connection;
        private final String token;

        /**
         * Generates a new request id.
         */
        long nextRequestId() {
            return this.connection.getFlow().getNextSequenceNumber();
        }

        @Override
        public void close() {
            try {
                this.connection.close();
            } catch (Exception ex) {
                log.warn("{}: Exception tearing down connection: ", TableSegmentImpl.this.segmentName, ex);
            }
        }
    }

    //endregion

    //region Context

    private class ConnectionContext {
        @GuardedBy("this")
        private CompletableFuture<ConnectionState> state;

        /**
         * Executes an action with retries (see {@link #retry} for retry details).
         *
         * @param action A {@link BiFunction} representing the action to execute. The first argument is the
         *               {@link ConnectionState} that should be used, and the second is the request id for this action.
         * @param <T>    Response type.
         * @return A CompletableFuture that, when completed, will contain the result of the action. If the operation failed,
         * the Future will be failed with the appropriate exception.
         */
        <T> CompletableFuture<T> execute(BiFunction<ConnectionState, Long, CompletableFuture<T>> action) {
            return TableSegmentImpl.this.retry.runAsync(
                    () -> getOrCreateState()
                            .thenCompose(state -> action.apply(state, state.nextRequestId())),
                    TableSegmentImpl.this.connectionPool.getInternalExecutor());
        }

        /**
         * Attempts to reuse an existing state or creates a new one if necessary.
         *
         * @return A CompletableFuture that will contain the {@link ConnectionState} to use. If another invocation of
         * this method has already initiated the (async) creation of a new {@link ConnectionState}, then this invocation
         * will return the same CompletableFuture as the other invocation, which will complete when the initialization
         * is done.
         */
        private CompletableFuture<ConnectionState> getOrCreateState() {
            Exceptions.checkNotClosed(TableSegmentImpl.this.closed.get(), this);
            CompletableFuture<ConnectionState> result;
            boolean needsInitialization = false;
            synchronized (this) {
                if (this.state == null
                        || this.state.isCompletedExceptionally()
                        || (this.state.isDone() && this.state.join().getConnection().isClosed())) {
                    this.state = new CompletableFuture<>();
                    needsInitialization = true;
                }
                result = this.state;
            }

            if (needsInitialization) {
                Futures.completeAfter(
                        () -> TableSegmentImpl.this.controller
                                .getEndpointForSegment(TableSegmentImpl.this.segmentName)
                                .thenCompose(uri -> TableSegmentImpl.this.tokenProvider.retrieveToken().thenApply(token -> {
                                    return new ConnectionState(new RawClient(TableSegmentImpl.this.controller,
                                        TableSegmentImpl.this.connectionPool, Segment.fromScopedName(TableSegmentImpl.this.segmentName)), token);
                                })),
                        result);
            }
            return result;
        }

        /**
         * Closes the connection. Any subsequent request will cause the {@link ConnectionState} to be reinitialized.
         */
        void reset() {
            CompletableFuture<ConnectionState> state;
            synchronized (this) {
                state = this.state;
                this.state = null;
            }

            if (state != null && !state.isCompletedExceptionally()) {
                state.thenAccept(ConnectionState::close);
            }
        }
    }

    //endregion
}
