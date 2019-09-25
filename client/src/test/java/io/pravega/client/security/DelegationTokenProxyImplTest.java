/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security;

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
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DelegationTokenProxyImplTest {

    @Test
    public void testDefaultCtorReturnsEmptyToken() {
        assertEquals("", new DelegationTokenProxyImpl().retrieveToken());
    }

    @Test
    public void testRefreshReturnsAsIsWithEmptyToken() {
        assertEquals("", new DelegationTokenProxyImpl().refreshToken());
    }

    @Test
    public void testExpirationTimeIsNullIfDelegationTokenIsEmpty() {
        DelegationTokenProxyImpl proxy = new DelegationTokenProxyImpl("", new FakeController(),
                "testscope", "teststream");
        assertNull("Expiration time is not null", proxy.extractExpirationTime(""));
    }

    @Test
    public void testExpirationTimeIsNullIfExpInBodyIsNotSet() {

        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "1234567890",
        //        "name": "John Doe",
        //        "iat": 1516239022
        //     }
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", // header
                "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ", // body
                "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"); // signature
        DelegationTokenProxyImpl proxy = new DelegationTokenProxyImpl(token, new FakeController(),
                "testscope", "teststream");

        assertNull("Expiration time is not null", proxy.extractExpirationTime(token));
    }

    @Test
    public void testExpirationTimeIsNotNullIfExpInBodyIsSet() {

        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "jdoe",
        //        "aud": "segmentstore",
        //         "iat": 1569324678,
        //         "exp": 1569324683
        //     }
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzUxMiJ9", // header
                "eyJzdWIiOiJqZG9lIiwiYXVkIjoic2VnbWVudHN0b3JlIiwiaWF0IjoxNTY5MzI0Njc4LCJleHAiOjE1NjkzMjQ2ODN9", // body
                "EKvw5oVkIihOvSuKlxiX7q9_OAYz7m64wsFZjJTBkoqg4oidpFtdlsldXHToe30vrPnX45l8QAG4DoShSMdw"); // signature
        DelegationTokenProxyImpl proxy = new DelegationTokenProxyImpl(token, new FakeController(),
                "testscope", "teststream");

        assertNotNull("Expiration time is null", proxy.extractExpirationTime(token));
    }

    @Test
    public void testReturnsExistingTokenIfExpiryIsNotSet() {
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", // header
                "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ", // body
                "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"); // signature
        DelegationTokenProxyImpl proxy = new DelegationTokenProxyImpl(token, new FakeController(),
                "testscope", "teststream");
        assertEquals(token, proxy.retrieveToken());
    }

    /*@Test
    public void testReturnsExistingTokenIfNotNearingExpiry() {
        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "jdoe",
        //        "aud": "segmentstore",
        //         "iat": 1569324678,
        //         "exp": 2147483647
        //     }
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzUxMiJ9", // header
                "eyJzdWIiOiJqZG9lIiwiYXVkIjoic2VnbWVudHN0b3JlIiwiaWF0IjoxNTY5MzI0Njc4LCJleHAiOjIxNDc0ODM2NDd9", // body
                "7fcgsw5T2VThK48mLG_z1QCxiYCHlGdGao2LprF9cs4-5xd7mIRGuX6sQnYgwA1pB47X-5ShGeU3HKyELkrMiA"); // signature
    }*/
}

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
