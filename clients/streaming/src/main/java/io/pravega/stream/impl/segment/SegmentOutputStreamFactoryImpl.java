/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl.segment;

import static io.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.stream.impl.ConnectionClosedException;
import io.pravega.stream.impl.netty.ClientConnection;
import org.apache.commons.lang.NotImplementedException;

import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.stream.Segment;
import io.pravega.stream.impl.Controller;
import io.pravega.stream.impl.netty.ConnectionFactory;
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
        controller.getEndpointForSegment(segment.getScopedName()).thenCompose((PravegaNodeUri endpointForSegment) -> {
            return cf.establishConnection(endpointForSegment, replyProcessor);
        }).thenAccept((ClientConnection connection) -> {
            try {
                connection.send(new WireCommands.GetTransactionInfo(1, segment.getScopedName(), txId));
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
    public SegmentOutputStream createOutputStreamForSegment(UUID writerId, Segment segment) {
        SegmentOutputStreamImpl result = new SegmentOutputStreamImpl(segment.getScopedName(), controller, cf, writerId);
        try {
            result.getConnection();
        } catch (RetriesExhaustedException | SegmentSealedException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return result;
    }
}
