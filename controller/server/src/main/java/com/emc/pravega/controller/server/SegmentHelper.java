/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommand;
import com.emc.pravega.common.netty.WireCommandType;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.ConnectionClosedException;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.NotImplementedException;


public class SegmentHelper {

    public static Controller.NodeUri getSegmentUri(final String scope,
                                        final String stream,
                                        final int segmentNumber,
                                        final HostControllerStore hostStore) {
        final Host host = hostStore.getHostForSegment(scope, stream, segmentNumber);
        return Controller.NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build();
    }

    public static CompletableFuture<Boolean> createSegment(final String scope,
                                                           final String stream,
                                                           final int segmentNumber,
                                                           final PravegaNodeUri uri,
                                                           final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new NotImplementedException());
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                result.complete(false);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                result.complete(true);
            }
        };

        return sendRequestOverNewConnection(
                new WireCommands.CreateSegment(Segment.getScopedName(scope, stream, segmentNumber)),
                replyProcessor,
                clientCF,
                uri)
                .thenCompose(x -> result);
    }

    /**
     * This method sends segment sealed message for the specified segment.
     * It owns up the responsibility of retrying the operation on failures until success.
     *
     * @param scope               stream scope
     * @param stream              stream name
     * @param segmentNumber       number of segment to be sealed
     * @param hostControllerStore host controller store
     * @param clientCF            connection factory
     * @return void
     */
    public static CompletableFuture<Boolean> sealSegment(final String scope,
                                                         final String stream,
                                                         final int segmentNumber,
                                                         final HostControllerStore hostControllerStore,
                                                         final ConnectionFactory clientCF) {
        final Controller.NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                result.complete(true);
            }
        };

        return sendRequestOverNewConnection(
                new WireCommands.SealSegment(Segment.getScopedName(scope, stream, segmentNumber)),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri))
                .thenCompose(x -> result);
    }

    public static CompletableFuture<UUID> createTransaction(final String scope,
                                                            final String stream,
                                                            final int segmentNumber,
                                                            final UUID txId,
                                                            final HostControllerStore hostControllerStore,
                                                            final ConnectionFactory clientCF) {
        final Controller.NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        final CompletableFuture<UUID> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.CREATE_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCreated(WireCommands.TransactionCreated transactionCreated) {
                result.complete(txId);
            }
        };

        return sendRequestOverNewConnection(
                new WireCommands.CreateTransaction(Segment.getScopedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri))
                .thenCompose(x -> result);
    }

    public static CompletableFuture<TxnStatus> commitTransaction(final String scope,
                                                                         final String stream,
                                                                         final int segmentNumber,
                                                                         final UUID txId,
                                                                         final HostControllerStore hostControllerStore,
                                                                         final ConnectionFactory clientCF) {
        final Controller.NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        final CompletableFuture<TxnStatus> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.COMMIT_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }

            @Override
            public void transactionAborted(WireCommands.TransactionAborted transactionAborted) {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.PreconditionFailed));
            }
        };

        return sendRequestOverNewConnection(
                new WireCommands.CommitTransaction(Segment.getScopedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri))
                .thenCompose(x -> result);
    }

    public static CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                                       final String stream,
                                                                       final int segmentNumber,
                                                                       final UUID txId,
                                                                       final HostControllerStore hostControllerStore,
                                                                       final ConnectionFactory clientCF) {
        final Controller.NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
        final CompletableFuture<TxnStatus> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.ABORT_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.PreconditionFailed));
            }

            @Override
            public void transactionAborted(WireCommands.TransactionAborted transactionDropped) {
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }
        };

        return sendRequestOverNewConnection(
                new WireCommands.AbortTransaction(Segment.getScopedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri))
                .thenCompose(x -> result);
    }

    private static CompletableFuture<Void> sendRequestOverNewConnection(final WireCommand request,
                                                                        final ReplyProcessor replyProcessor,
                                                                        final ConnectionFactory connectionFactory,
                                                                        final PravegaNodeUri uri) {
        return connectionFactory.establishConnection(uri, replyProcessor)
                .thenApply(connection -> {
                    try {
                        connection.send(request);
                    } catch (ConnectionFailedException cfe) {
                        throw new WireCommandFailedException(cfe, request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
                    }
                    return null;
                });
    }

}
