/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.emc.pravega.common.util.RetriesExhaustedException;
import org.apache.commons.lang.NotImplementedException;

import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands.GetTransactionInfo;
import com.emc.pravega.common.netty.WireCommands.TransactionInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.ConnectionClosedException;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.google.common.annotations.VisibleForTesting;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@VisibleForTesting
@RequiredArgsConstructor
public class SegmentOutputStreamFactoryImpl implements SegmentOutputStreamFactory {

    private final Controller controller;
    private final ConnectionFactory cf;

    @Override
    public SegmentOutputStream createOutputStreamForTransaction(Segment segment, UUID txId) {
        CompletableFuture<String> name = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                name.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                name.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void transactionInfo(TransactionInfo info) {
               name.complete(info.getTransactionName());
            }
        };
        controller.getEndpointForSegment(segment.getScopedName()).thenCompose((PravegaNodeUri endpointForSegment) -> {
            return cf.establishConnection(endpointForSegment, replyProcessor);
        }).thenAccept((ClientConnection connection) -> {
            try {
                connection.send(new GetTransactionInfo(segment.getScopedName(), txId));
            } catch (ConnectionFailedException e) {
                throw new RuntimeException(e);
            } 
        }).exceptionally((Throwable t) -> {
            name.completeExceptionally(t);
            return null;
        });
        return new SegmentOutputStreamImpl( getAndHandleExceptions(name, RuntimeException::new), controller, cf, UUID.randomUUID());
    }

    @Override
    public SegmentOutputStream createOutputStreamForSegment(Segment segment, SegmentOutputConfiguration config)
            throws SegmentSealedException {
        SegmentOutputStreamImpl result = new SegmentOutputStreamImpl(segment.getScopedName(), controller, cf, UUID.randomUUID());
        try {
            result.getConnection();
        } catch (RetriesExhaustedException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return result;
    }
}
