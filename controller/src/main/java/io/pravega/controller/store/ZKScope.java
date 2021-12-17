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
package io.pravega.controller.store;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ZKScope implements Scope {
    public static final String STREAMS_IN_SCOPE = "_streamsinscope";
    private static final String SCOPE_PATH = "/store/%s";
    private static final String STREAMS_IN_SCOPE_ROOT_PATH = "/store/" + STREAMS_IN_SCOPE + "/%s";
    private static final String STREAMS_IN_SCOPE_ROOT_PATH_FORMAT = STREAMS_IN_SCOPE_ROOT_PATH + "/streams";
    private static final String COUNTER_PATH = STREAMS_IN_SCOPE_ROOT_PATH + "/counter";
    private static final Predicate<Throwable> DATA_NOT_FOUND_PREDICATE = e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException;
    private static final String KVTABLES_IN_SCOPE = "_kvtablesinscope";
    private static final String KVTABLES_IN_SCOPE_ROOT_PATH = "/store/" + KVTABLES_IN_SCOPE + "/%s";
    // This is a path like: /store/_kvtablesinscope/scope1/kvtable1
    private static final String KVTABLES_IN_SCOPE_ROOT_PATH_FORMAT = KVTABLES_IN_SCOPE_ROOT_PATH + "/%s";
    private static final String DELETE_SCOPE_TABLE_FORMAT = "/store/deletingTable";

    private final String scopePath;
    private final String counterPath;
    private final String streamsInScopePath;
    private final String scopeName;
    private final ZKStoreHelper store;

    public ZKScope(final String scopeName, ZKStoreHelper store) {
        this.scopeName = scopeName;
        this.store = store;
        this.scopePath = String.format(SCOPE_PATH, scopeName);
        this.counterPath = String.format(COUNTER_PATH, scopeName);
        this.streamsInScopePath = String.format(STREAMS_IN_SCOPE_ROOT_PATH_FORMAT, scopeName);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope(OperationContext context) {
        return store.addNode(scopePath);
    }

    @Override
    public CompletableFuture<Void> deleteScope(OperationContext context) {
        return store.deleteNode(scopePath)
                    .thenCompose(v -> Futures.exceptionallyExpecting(store.deleteTree(counterPath), DATA_NOT_FOUND_PREDICATE, null))
                    .thenCompose(v -> Futures.exceptionallyExpecting(store.deleteTree(streamsInScopePath), DATA_NOT_FOUND_PREDICATE, null));
    }

    @Override
    public CompletableFuture<Void> deleteScopeRecursive(OperationContext context) {
        return Futures.failedFuture(new NotImplementedException("DeleteScopeRecursive not implemented for ZK scope"));
    }

    /**
     * Streams are ordered under the scope in the order of stream position. 
     * The metadata store first calls `getNextStreamPosition` method to generate a new position. 
     * This position is assigned to the stream and a reference to this stream is stored under the scope at the said position. 
     * 
     * ZkScope takes this position number and breaks it into 3 parts of 2, 4, 4 digits respectively.
     * Logically following becomes the hierarchy where the stream reference is added under the scope.
     * `/01/2345/6789-streamname`
     * `01` is called msb.
     * `2345` is called middle.
     * `6789` is called lsb.
     *
     * Storing the stream references in such a hierarchy has two advantages -- 
     * 1. we divide streams in groups of 10k which can be retrieved via a single zk api call which gives us a logical pagination.
     * 2. the last 4 digits included with stream ref also gives us the "order" within the group of 10k leaf nodes. 
     *    This enables us to have a continuation token that is not just at group level but for streams within a group too.
     * @param name name of stream
     * @param streamPosition position of stream
     * @return CompletableFuture which when completed has the stream reference stored under the aforesaid hierarchy.  
     */
    public CompletableFuture<Void> addStreamToScope(String name, int streamPosition) {
        // break the path from stream position into three parts --> 2, 4, 4
        String path = getPathForStreamPosition(name, streamPosition);
        return Futures.toVoid(store.createZNodeIfNotExist(path));
    }

    public CompletableFuture<Void> removeStreamFromScope(String name, int streamPosition) {
        String path = getPathForStreamPosition(name, streamPosition);

        return Futures.toVoid(store.deletePath(path, true));
    }

    private String getPathForStreamPosition(String name, int streamPosition) {
        Preconditions.checkArgument(streamPosition >= 0);
        Token token = new Token(streamPosition);

        // root path represents most significant 2 digits of the position.
        String root = ZKPaths.makePath(streamsInScopePath, token.getMsb().toString());
        // container path represents most significant 3rd to 6th digits of the position.
        String container = ZKPaths.makePath(root, token.getMiddle().toString());
        // the path is composed of stream name followed by last 4 chars that represent least significant 4 digits of the position.
        return ZKPaths.makePath(container, String.format("%s%04d", name, token.getLsb()));
    }

    private String getStreamName(String x) {
        return x.substring(0, x.length() - 4);
    }

    private int getPosition(String x) {
        return Integer.parseInt(x.substring(x.length() - 4, x.length()));
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope(OperationContext context) {
        return store.getChildren(scopePath, false);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreams(int limit, String continuationToken, Executor executor,
                                                                     OperationContext context) {
        // Stream references are stored under a hierarchy of nodes as described in `addStreamsInScope` method. 
        // A continuation token is essentially a serialized integer that is broken into three parts - 
        // msb 2 bytes, middle 4 bytes and lsb 4 bytes. 
        // stream references are stored as /01/2345/stream-6789. 
        // So effectively all streams under the scope are ordered by the stream position. Stream position is strictly 
        // increasing and any new stream that is added to the scope will be done at a higher position. 
        // Streams can be deleted though. 
        // So now the continuation token basically signifies the position of last stream that was returned in previous iteration. 
        // And continuing from the continuation token, we simply retrieve next `limit` number of streams under the scope. 
        // Since the streams are stored under the aforesaid hierarchy, we start with all children of the `middle` part of 
        // continuation token and only include streams whose position is greater than token.
        // If we are not able to get `limit` number of streams from this, we go to the next higher available `middle` znode 
        // and fetch its children. If middle is exhausted, we increment the `msb` and repeat until we have found `limit` 
        // number of streams or reached the end. 
        List<String> toReturn = new LinkedList<>();
        AtomicInteger remaining = new AtomicInteger(limit);
        Token floor = Token.fromString(continuationToken);
        AtomicReference<Token> lastPos = new AtomicReference<>(floor);
        // compute on all available top level children (0-99) that are greater than floor.getLeft
        return computeOnChildren(streamsInScopePath, topChild -> {
            if (topChild >= floor.getMsb()) {
                String topPath = ZKPaths.makePath(streamsInScopePath, topChild.toString());
                // set middle floor = supplied floor OR 0 if top floor has been incremented
                int middleFloor = topChild.intValue() == floor.getMsb() ? floor.getMiddle() : 0;

                // compute on all available middle level children (0-9999) of current top level that are greater than middle floor
                CompletableFuture<Void> voidCompletableFuture = computeOnChildren(topPath, middleChild -> {
                    if (middleChild >= middleFloor) {
                        String middlePath = ZKPaths.makePath(topPath, middleChild.toString());
                        return store.getChildren(middlePath)
                                    .thenAccept(streams -> {
                                        // set bottom floor = -1 if we have incremented either top or middle floors
                                        int bottomFloor = topChild.intValue() == floor.getMsb() &&
                                                middleChild.intValue() == floor.getMiddle() ? floor.getLsb() : -1;

                                        Pair<List<String>, Integer> retVal = filterStreams(streams, bottomFloor, remaining.get());
                                        if (!retVal.getKey().isEmpty()) {
                                            toReturn.addAll(retVal.getKey());
                                            remaining.set(limit - toReturn.size());
                                            lastPos.set(new Token(topChild, middleChild, retVal.getValue()));
                                        }
                                    })
                                    .thenApply(v -> remaining.get() > 0);
                    } else {
                        return CompletableFuture.completedFuture(true);
                    }
                }, executor);
                return voidCompletableFuture.thenApply(v -> remaining.get() > 0);
            } else {
                return CompletableFuture.completedFuture(true);
            }
        }, executor).thenApply(v -> new ImmutablePair<>(toReturn, lastPos.get().toString()));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreamsForTag(String tag, String continuationToken, Executor executor, OperationContext context) {
        return Futures.failedFuture(new NotImplementedException("ListStreamsForTag not implemented for ZK scope"));
    }

    private Pair<List<String>, Integer> filterStreams(List<String> streams, int bottomFloor, int limit) {
        AtomicReference<Integer> last = new AtomicReference<>(bottomFloor);

        List<String> list = streams.stream()
                                   .filter(x -> bottomFloor < getPosition(x))
                                   .sorted(Comparator.comparingInt(this::getPosition))
                                   .limit(limit)
                                   .collect(Collectors.toList());
        List<String> toReturn = list.stream().map(x -> {
            int streamPosition = getPosition(x);
            if (streamPosition > last.get()) {
                last.set(streamPosition);
            }
            return getStreamName(x);
        }).collect(Collectors.toList());

        return new ImmutablePair<>(toReturn, last.get());
    }

    @Override
    public void refresh() {
    }

    /**
     * When a new stream is created under a scope, we first get a new counter value by creating a sequential znode under 
     * a counter node. this is a 10 digit integer which the store passes to the zkscope object as position.
     * @return A future which when completed has the next stream position represented by an integer. 
     */
    public CompletableFuture<Integer> getNextStreamPosition() {
        return store.createEphemeralSequentialZNode(counterPath)
                    .thenApply(counterStr -> Integer.parseInt(counterStr.replace(counterPath, "")));
    }

    private CompletableFuture<Void> computeOnChildren(String path, Function<Integer, CompletableFuture<Boolean>> function,
                                                      Executor executor) {
        return store.getChildren(path, false)
                    .thenCompose(children -> {
                        AtomicInteger index = new AtomicInteger(0);
                        AtomicBoolean continueLoop = new AtomicBoolean(!children.isEmpty());
                        List<Integer> list = children.stream().map(Integer::parseInt).sorted().collect(Collectors.toList());
                        return Futures.loop(continueLoop::get,
                                () -> function.apply(list.get(index.get())).thenAccept(canContinue -> {
                                    continueLoop.set(canContinue && index.incrementAndGet() < children.size());
                                }), executor);
                    });
    }

    @Data
    private static class Token {
        static final Token EMPTY = new Token(0, 0, -1);
        static final TokenSerializer SERIALIZER = new TokenSerializer();

        private final Integer msb;
        private final Integer middle;
        private final Integer lsb;

        @Builder
        Token(int msb, int middle, int lsb) {
            Preconditions.checkArgument(msb >= 0 && msb < 100);
            Preconditions.checkArgument(middle >= 0 && middle < 10000);
            Preconditions.checkArgument(lsb < 10000);

            this.msb = msb;
            this.middle = middle;
            this.lsb = lsb;
        }

        Token(int position) {
            Preconditions.checkArgument(position >= 0);
            String withPadding = String.format("%010d", position);
            this.msb = Integer.parseInt(withPadding.substring(0, 2));
            this.middle = Integer.parseInt(withPadding.substring(2, 6));
            this.lsb = Integer.parseInt(withPadding.substring(6, 10));
        }

        @SneakyThrows
        static Token fromString(String token) {
            if (Strings.isNullOrEmpty(token)) {
                return Token.EMPTY;
            }
            return SERIALIZER.deserialize(Base64.getDecoder().decode(token));
        }

        @Override
        @SneakyThrows
        public String toString() {
            return Base64.getEncoder().encodeToString(SERIALIZER.serialize(this).getCopy());
        }


        static class TokenBuilder implements ObjectBuilder<Token> {

        }

        static class TokenSerializer extends
                VersionedSerializer.WithBuilder<Token, Token.TokenBuilder> {
            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput revisionDataInput, Token.TokenBuilder builder) throws IOException {
                builder.msb(revisionDataInput.readCompactInt())
                       .middle(revisionDataInput.readCompactInt())
                       .lsb((int) revisionDataInput.readCompactSignedLong());
            }

            private void write00(Token token, RevisionDataOutput revisionDataOutput) throws IOException {
                revisionDataOutput.writeCompactInt(token.msb);
                revisionDataOutput.writeCompactInt(token.middle);
                revisionDataOutput.writeCompactSignedLong(token.lsb);
            }

            @Override
            protected Token.TokenBuilder newBuilder() {
                return Token.builder();
            }
        }
    }

    public CompletableFuture<Void> addKVTableToScope(String kvt, UUID id) {
        return Futures.toVoid(getKVTableInScopeZNodePath(this.scopeName, kvt)
                .thenCompose(path -> store.createZNodeIfNotExist(path)
                .thenCompose(x -> {
                    byte[] b = new byte[2 * Long.BYTES];
                    BitConverter.writeUUID(new ByteArraySegment(b), id);

                    return store.setData(path, b, new Version.IntVersion(0));
                })));
    }

    public CompletableFuture<Void> removeKVTableFromScope(String name) {
        return Futures.toVoid(getKVTableInScopeZNodePath(this.scopeName, name)
                .thenApply(path -> store.deletePath(path, true)));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>>  listKeyValueTables(final int limit, final String continuationToken,
                                                      final Executor executor, OperationContext context) {
        String scopePath = String.format(KVTABLES_IN_SCOPE_ROOT_PATH, scopeName);
        return store.getChildren(scopePath).thenApply( kvtables -> new ImmutablePair<>(kvtables, continuationToken));

    }

    @Override
    public CompletableFuture<UUID> getReaderGroupId(String rgName, OperationContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> isScopeSealed(String scopeName, OperationContext context) {
        return getScopeInDeletingTable(scopeName).thenCompose(store::checkExists);
    }

    @Override
    public CompletableFuture<UUID> getScopeId(String scopeName, OperationContext context) {
        return Futures.failedFuture(new NotImplementedException("GetScopeId not implemented for ZK scope"));
    }

    public CompletableFuture<Boolean> checkKeyValueTableExistsInScope(String kvt) {
        return getKVTableInScopeZNodePath(this.scopeName, kvt).thenCompose(path -> store.checkExists(path));
    }

    public static CompletableFuture<String> getKVTableInScopeZNodePath(String scopeName, String kvtName) {
        return CompletableFuture.completedFuture(String.format(KVTABLES_IN_SCOPE_ROOT_PATH_FORMAT, scopeName, kvtName));
    }

    public static CompletableFuture<String> getScopeInDeletingTable(String scopeName) {
        return CompletableFuture.completedFuture(String.format(DELETE_SCOPE_TABLE_FORMAT, scopeName));
    }
}
