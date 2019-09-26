/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security.auth;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.CancellableRequest;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.protocol.netty.PravegaNodeUri;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

class FakeController implements Controller {

    @Override
    public CompletableFuture<Boolean> createScope(String scopeName) {
        return null;
    }

    @Override
    public AsyncIterator<Stream> listStreams(String scopeName) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> createStream(String scope, String streamName, StreamConfiguration streamConfig) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> updateStream(String scope, String streamName, StreamConfiguration streamConfig) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> truncateStream(String scope, String streamName, StreamCut streamCut) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> sealStream(String scope, String streamName) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(String scope, String streamName) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> startScale(Stream stream, List<Long> sealedSegments, Map<Double, Double> newKeyRanges) {
        return null;
    }

    @Override
    public CancellableRequest<Boolean> scaleStream(Stream stream, List<Long> sealedSegments, Map<Double, Double> newKeyRanges, ScheduledExecutorService executorService) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> checkScaleStatus(Stream stream, int scaleEpoch) {
        return null;
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String scope, String streamName) {
        return null;
    }

    @Override
    public CompletableFuture<TxnSegments> createTransaction(Stream stream, long lease) {
        return null;
    }

    @Override
    public CompletableFuture<Transaction.PingStatus> pingTransaction(Stream stream, UUID txId, long lease) {
        return null;
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, UUID txId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        return null;
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txId) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(Stream stream, long timestamp) {
        return null;
    }

    @Override
    public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
        return null;
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSuccessors(StreamCut from) {
        return null;
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSegments(StreamCut fromStreamCut, StreamCut toStreamCut) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(Segment segment) {
        return null;
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName) {
        return null;
    }
}
