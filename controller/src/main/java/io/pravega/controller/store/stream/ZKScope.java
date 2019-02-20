/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ZKScope implements Scope {

    private static final String SCOPE_PATH = "/store/%s";
    private static final String STREAMS_IN_SCOPE_ROOT_PATH = "/store/streamsinscope/%s";
    private static final String STREAMS_IN_SCOPE_ROOT_PATH_FORMAT = STREAMS_IN_SCOPE_ROOT_PATH + "/streams";
    private static final String COUNTER_PATH = STREAMS_IN_SCOPE_ROOT_PATH + "/counter";

    private final String scopePath;
    private final String counterPath;
    private final String streamsInScopePath;
    private final String scopeName;
    private final ZKStoreHelper store;

    ZKScope(final String scopeName, ZKStoreHelper store) {
        this.scopeName = scopeName;
        this.store = store;
        this.scopePath = String.format(SCOPE_PATH, scopeName);
        this.counterPath = String.format(COUNTER_PATH, scopeName);
        this.streamsInScopePath = String.format(STREAMS_IN_SCOPE_ROOT_PATH, scopeName);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        return store.addNode(scopePath);
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return store.deleteNode(scopePath)
                    .thenCompose(v -> store.deleteTree(streamsInScopePath));
    }

    CompletableFuture<Void> addStreamToScope(String name, int streamPosition) {
        // break the path from stream position into three parts --> 2, 4, 4
        String path = getPathForStreamPosition(name, streamPosition);
        return Futures.toVoid(store.createZNodeIfNotExist(path));
    }

    CompletableFuture<Void> removeStreamFromScope(String name, int streamPosition) {
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
    public CompletableFuture<List<String>> listStreamsInScope() {
        return store.getChildren(scopePath);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreamsInScope(int limit, String continuationToken, Executor executor) {
        List<String> toReturn = new LinkedList<>();
        AtomicInteger remaining = new AtomicInteger(limit);
        Token floor = Token.fromString(continuationToken);
        AtomicReference<Token> lastPos = new AtomicReference<>(floor);
        // compute on all available top level children (0-99) that are greater than floor.getLeft
        return computeOnChildren(streamsInScopePath, topChild -> {
            final int top = Integer.parseInt(topChild);
            if (top >= floor.getMsb()) {
                String topPath = ZKPaths.makePath(streamsInScopePath, topChild);
                // set middle floor = supplied floor OR 0 if top floor has been incremented
                int middleFloor = top == floor.getMsb() ? floor.getMiddle() : 0;

                // compute on all available middle level children (0-9999) of current top level that are greater than middle floor
                CompletableFuture<Void> voidCompletableFuture = computeOnChildren(topPath, middleChild -> {
                    final int middle = Integer.parseInt(middleChild);
                    if (middle >= middleFloor) {
                        String middlePah = ZKPaths.makePath(topPath, middleChild.toString());
                        return store.getChildren(middlePah)
                                    .thenAccept(streams -> {
                                        // set bottom floor = -1 if we have incremented either top or middle floors
                                        int bottomFloor = top == floor.getMsb() &&
                                                middle == floor.getMiddle() ? floor.getLsb() : -1;

                                        Pair<List<String>, Integer> retVal = filterStreams(streams, bottomFloor, remaining.get());
                                        if (!retVal.getKey().isEmpty()) {
                                            toReturn.addAll(retVal.getKey());
                                            remaining.set(limit - toReturn.size());
                                            lastPos.set(new Token(top, middle, retVal.getValue()));
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

    CompletableFuture<Integer> getNextStreamPosition() {
        return store.createEphemeralSequentialZNode(counterPath)
                    .thenApply(counterStr -> Integer.parseInt(counterStr.replace(counterPath, "")));
    }

    private CompletableFuture<Void> computeOnChildren(String path, Function<String, CompletableFuture<Boolean>> function,
                                                      Executor executor) {
        return store.getChildren(path)
                    .thenCompose(children -> {
                        AtomicInteger index = new AtomicInteger(0);
                        AtomicBoolean canContinue = new AtomicBoolean(true);
                        return Futures.loop(canContinue::get,
                                () -> function.apply(children.get(index.get())).thenAccept(canCont -> {
                                    canContinue.set(canCont && index.incrementAndGet() < children.size());
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
            return SERIALIZER.deserialize(token.getBytes());
        }

        @SneakyThrows
        public String toString() {
            return new String(SERIALIZER.serialize(this).getCopy());
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
                       .lsb(revisionDataInput.readCompactInt());
            }

            private void write00(Token token, RevisionDataOutput revisionDataOutput) throws IOException {
                revisionDataOutput.writeCompactInt(token.msb);
                revisionDataOutput.writeCompactInt(token.middle);
                revisionDataOutput.writeCompactInt(token.lsb);
            }

            @Override
            protected Token.TokenBuilder newBuilder() {
                return Token.builder();
            }
        }
    }
}
