/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
class SegmentMetadataClientImpl implements SegmentMetadataClient {
    private static final RetryWithBackoff RETRY_SCHEDULE = Retry.withExpBackoff(1, 10, 5);

    private final Segment segmentId;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    
    private final Object lock = new Object();
    @GuardedBy("lock")
    private CompletableFuture<ClientConnection> connection = null;
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<StreamSegmentInfo>> infoRequests = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<WireCommands.SegmentAttribute>> getAttributeRequests = new HashMap<>();
    @GuardedBy("lock")
    private final Map<Long, CompletableFuture<SegmentAttributeUpdated>> setAttributeRequests = new HashMap<>();
    private final Supplier<Long> requestIdGenerator = new AtomicLong()::incrementAndGet;
    private final ResponseProcessor responseProcessor = new ResponseProcessor();
    
    private final class ResponseProcessor extends FailingReplyProcessor {
        
        @Override
        public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
            log.trace("Received stream segment info {}", streamInfo);
            CompletableFuture<StreamSegmentInfo> future;
            synchronized (lock) {
                future = infoRequests.remove(streamInfo.getRequestId());
            }
            if (future != null) {
                future.complete(streamInfo);
            }
        }        

        @Override
        public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {
            log.trace("Received stream segment attribute {}", segmentAttribute);
            CompletableFuture<WireCommands.SegmentAttribute> future;
            synchronized (lock) {
                future = getAttributeRequests.remove(segmentAttribute.getRequestId());
            }
            if (future != null) {
                future.complete(segmentAttribute);
            }
        }
        
        @Override
        public void segmentAttributeUpdated(SegmentAttributeUpdated segmentAttributeUpdated) {
            log.trace("Received stream segment attribute update result {}", segmentAttributeUpdated);
            CompletableFuture<SegmentAttributeUpdated> future;
            synchronized (lock) {
                future = setAttributeRequests.remove(segmentAttributeUpdated.getRequestId());
            }
            if (future != null) {
                future.complete(segmentAttributeUpdated);
            }
        }
        
        @Override
        public void connectionDropped() {
            closeConnection(new ConnectionFailedException());
        }

        @Override
        public void wrongHost(WireCommands.WrongHost wrongHost) {
            closeConnection(new ConnectionFailedException(wrongHost.toString()));
        }

        @Override
        public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
            closeConnection(new InvalidStreamException(noSuchSegment.toString()));
        }

        @Override
        public void processingFailure(Exception error) {
            log.warn("Processing failure: ", error);
            closeConnection(error);
        }
    }

    private void closeConnection(Exception exceptionToInflightRequests) {
        log.info("Closing connection with exception: {}", exceptionToInflightRequests.getMessage());
        CompletableFuture<ClientConnection> c;
        synchronized (lock) {
            c = connection;
            connection = null;
        }
        if (c != null && FutureHelpers.isSuccessful(c)) {
            try {
                c.getNow(null).close();
            } catch (Exception e) {
                log.warn("Exception tearing down connection: ", e);
            }
        }
        failAllInflight(exceptionToInflightRequests);
    }
    
    private void failAllInflight(Exception e) {
        log.info("SegmentMetadata connection failed due to a {}.", e.getMessage());
        List<CompletableFuture<StreamSegmentInfo>> infoRequestsToFail;
        List<CompletableFuture<WireCommands.SegmentAttribute>> getAttributeRequestsToFail;
        List<CompletableFuture<SegmentAttributeUpdated>> setAttributeRequestsToFail;
        synchronized (lock) {
            infoRequestsToFail = new ArrayList<>(infoRequests.values());
            getAttributeRequestsToFail = new ArrayList<>(getAttributeRequests.values());
            setAttributeRequestsToFail = new ArrayList<>(setAttributeRequests.values());
            infoRequests.clear();
            getAttributeRequests.clear();
            setAttributeRequests.clear();
        }
        for (CompletableFuture<StreamSegmentInfo> infoRequest : infoRequestsToFail) {
            infoRequest.completeExceptionally(e);
        }
        for (CompletableFuture<WireCommands.SegmentAttribute> getAttributeRequest : getAttributeRequestsToFail) {
            getAttributeRequest.completeExceptionally(e);
        }
        for (CompletableFuture<SegmentAttributeUpdated> setAttributeRequest : setAttributeRequestsToFail) {
            setAttributeRequest.completeExceptionally(e);
        }
    }
    
    CompletableFuture<ClientConnection> getConnection() {
        synchronized (lock) {
            //Optimistic check
            if (connection != null) {
                return connection;
            }
        }
        return controller.getEndpointForSegment(segmentId.getScopedName()).thenCompose((PravegaNodeUri uri) -> {
            log.info("Connecting to {}", uri);
            synchronized (lock) {
                if (connection == null) {
                    connection = connectionFactory.establishConnection(uri, responseProcessor);
                }
                return connection; 
            } 
        });
    }
    
    private CompletableFuture<WireCommands.StreamSegmentInfo> getSegmentInfo() {
        CompletableFuture<WireCommands.StreamSegmentInfo> result = new CompletableFuture<>();
        long requestId = requestIdGenerator.get();
        synchronized (lock) {
            infoRequests.put(requestId, result);
        }
        FutureHelpers.exceptionListener(getConnection().thenAccept(c -> {
            try {
                log.debug("Getting segment info for segment: {}", segmentId);
                c.send(new WireCommands.GetStreamSegmentInfo(requestId, segmentId.getScopedName()));
            } catch (ConnectionFailedException e) {
                closeConnection(e);
            }
        }), e -> {
            result.completeExceptionally(new CompletionException(e));
        });
        return result;
    }
    
    private CompletableFuture<WireCommands.SegmentAttribute> getPropertyAsync(UUID attributeId) {
        CompletableFuture<WireCommands.SegmentAttribute> result = new CompletableFuture<>();
        long requestId = requestIdGenerator.get();
        synchronized (lock) {
            getAttributeRequests.put(requestId, result);
        }
        FutureHelpers.exceptionListener(getConnection().thenAccept(c -> {
            try {
                log.debug("Getting segment attribute: {}", attributeId);
                c.send(new WireCommands.GetSegmentAttribute(requestId, segmentId.getScopedName(), attributeId));
            } catch (ConnectionFailedException e) {
                closeConnection(e);
            }
        }), e -> {
            result.completeExceptionally(new CompletionException(e));
        });
        return result;
    }
    
    private CompletableFuture<WireCommands.SegmentAttributeUpdated> updatePropertyAsync(UUID attributeId, long expected, long value) {
        CompletableFuture<WireCommands.SegmentAttributeUpdated> result = new CompletableFuture<>();
        long requestId = requestIdGenerator.get();
        synchronized (lock) {
            setAttributeRequests.put(requestId, result);
        }
        FutureHelpers.exceptionListener(getConnection().thenAccept(c -> {
            try {
                log.trace("Updating segment attribute: {}", attributeId);
                c.send(new WireCommands.UpdateSegmentAttribute(requestId, segmentId.getScopedName(), attributeId, value, expected));
            } catch (ConnectionFailedException e) {
                closeConnection(e);
            }
        }), e -> {
            result.completeExceptionally(new CompletionException(e));
        });
        return result;
    }
    
    @Override
    public long fetchCurrentStreamLength() {
        return RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                             .throwingOn(InvalidStreamException.class)
                             .run(() -> {
                                 return FutureHelpers.getThrowingException(getSegmentInfo()).getSegmentLength();
                             });
    }

    
    @Override
    public long fetchProperty(SegmentAttribute attribute) {
        return RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                .throwingOn(InvalidStreamException.class)
                .run(() -> {
                    return FutureHelpers.getThrowingException(getPropertyAsync(attribute.getValue())).getValue();
                });
    }

    @Override
    public boolean compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue) {
        return RETRY_SCHEDULE.retryingOn(ConnectionFailedException.class)
                .throwingOn(InvalidStreamException.class)
                .run(() -> {
                    return FutureHelpers.getThrowingException(updatePropertyAsync(attribute.getValue(), expectedValue, newValue)).isSuccess();
                });
    }

}
