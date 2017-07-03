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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryWithBackoff;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.NotImplementedException;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

@Slf4j
@VisibleForTesting
@RequiredArgsConstructor
public class SegmentOutputStreamFactoryImpl implements SegmentOutputStreamFactory {

    private final Controller controller;
    private final ConnectionFactory cf;

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId,
                                                                Consumer<Segment> segmentSealedCallback,
                                                                EventWriterConfig config) {
        CompletableFuture<String> name = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                name.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                name.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionInfo(WireCommands.TransactionInfo info) {
               name.complete(info.getTransactionName());
            }

            @Override
            public void processingFailure(Exception error) {
                name.completeExceptionally(error);
            }
        };
        val connectionFuture = controller.getEndpointForSegment(segment.getScopedName())
                                         .thenCompose((PravegaNodeUri endpointForSegment) -> {
                                             return cf.establishConnection(endpointForSegment, replyProcessor);
                                         });
        connectionFuture.thenAccept((ClientConnection connection) -> {
            try {
                connection.send(new WireCommands.GetTransactionInfo(1, segment.getScopedName(), txId));
            } catch (ConnectionFailedException e) {
                throw new RuntimeException(e);
            }
        }).exceptionally(t -> {
            name.completeExceptionally(t);
            return null;
        });
        name.whenComplete((s, e) -> {
            getAndHandleExceptions(connectionFuture, RuntimeException::new).close();
        });
        return new SegmentOutputStreamImpl(getAndHandleExceptions(name, RuntimeException::new), controller, cf,
                UUID.randomUUID(), segmentSealedCallback, getRetryFromConfig(config));
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, Consumer<Segment> segmentSealedCallback, EventWriterConfig config) {
        SegmentOutputStreamImpl result = new SegmentOutputStreamImpl(segment.getScopedName(), controller, cf,
                UUID.randomUUID(), segmentSealedCallback, getRetryFromConfig(config));
        try {
            result.getConnection();
        } catch (RetriesExhaustedException | SegmentSealedException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return result;
    }
    
    private RetryWithBackoff getRetryFromConfig(EventWriterConfig config) {
        return Retry.withExpBackoff(config.getInitalBackoffMillis(), config.getBackoffMultiple(),
                                    config.getRetryAttempts(), config.getMaxBackoffMillis());
    }
}
