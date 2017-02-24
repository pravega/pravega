/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.rpc.v1;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.FailingReplyProcessor;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommand;
import com.emc.pravega.common.netty.WireCommandType;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.TxnStatus;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SegmentHelper {

    public NodeUri getSegmentUri(final String scope,
                                        final String stream,
                                        final int segmentNumber,
                                        final HostControllerStore hostStore) {
        final Host host = hostStore.getHostForSegment(scope, stream, segmentNumber);
        return new NodeUri(host.getIpAddr(), host.getPort());
    }

    public CompletableFuture<Boolean> createSegment(final String scope,
                                                           final String stream,
                                                           final int segmentNumber,
                                                           final ScalingPolicy policy,
                                                           final HostControllerStore hostControllerStore,
                                                           final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        final WireCommandType type = WireCommandType.CREATE_SEGMENT;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
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

        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        whenComplete(sendRequestOverNewConnection(
                new WireCommands.CreateSegment(Segment.getScopedName(scope, stream, segmentNumber), extracted.getLeft(), extracted.getRight()),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri)), result);

        return result;
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
    public CompletableFuture<Boolean> sealSegment(final String scope,
                                                         final String stream,
                                                         final int segmentNumber,
                                                         final HostControllerStore hostControllerStore,
                                                         final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

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

        whenComplete(sendRequestOverNewConnection(
                new WireCommands.SealSegment(Segment.getScopedName(scope, stream, segmentNumber)),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri)), result);
        return result;
    }

    public CompletableFuture<UUID> createTransaction(final String scope,
                                                            final String stream,
                                                            final int segmentNumber,
                                                            final UUID txId,
                                                            final HostControllerStore hostControllerStore,
                                                            final ConnectionFactory clientCF) {
        final NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

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

        whenComplete(sendRequestOverNewConnection(
                new WireCommands.CreateTransaction(Segment.getScopedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri)), result);
        return result;
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope,
                                                                 final String stream,
                                                                 final int segmentNumber,
                                                                 final UUID txId,
                                                                 final HostControllerStore hostControllerStore,
                                                                 final ConnectionFactory clientCF) {
        final NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

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
                result.complete(TxnStatus.SUCCESS);
            }

            @Override
            public void transactionAborted(WireCommands.TransactionAborted transactionAborted) {
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.PreconditionFailed));
            }
        };

        whenComplete(sendRequestOverNewConnection(
                new WireCommands.CommitTransaction(Segment.getScopedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri)), result);
        return result;
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                                final String stream,
                                                                final int segmentNumber,
                                                                final UUID txId,
                                                                final HostControllerStore hostControllerStore,
                                                                final ConnectionFactory clientCF) {
        final NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
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
                result.complete(TxnStatus.SUCCESS);
            }
        };

        whenComplete(sendRequestOverNewConnection(
                new WireCommands.AbortTransaction(Segment.getScopedName(scope, stream, segmentNumber), txId),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri)), result);
        return result;
    }

    public CompletableFuture<Void> updatePolicy(String scope, String stream, ScalingPolicy policy,
                                                       int segmentNumber, HostControllerStore hostControllerStore,
                                                       ConnectionFactoryImpl clientCF) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_POLICY;
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
            public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated policyUpdated) {
                result.complete(null);
            }
        };

        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        whenComplete(sendRequestOverNewConnection(
                new WireCommands.UpdateSegmentPolicy(Segment.getScopedName(scope, stream, segmentNumber), extracted.getLeft(), extracted.getRight()),
                replyProcessor,
                clientCF,
                ModelHelper.encode(uri)), result);
        return result;
    }

    private CompletableFuture<Void> sendRequestOverNewConnection(final WireCommand request,
                                                                        final ReplyProcessor replyProcessor,
                                                                        final ConnectionFactory connectionFactory,
                                                                        final PravegaNodeUri uri) {
        CompletableFuture<ClientConnection> connect = new CompletableFuture<>();
        CompletableFuture<Void> result = new CompletableFuture<>();

        connectionFactory.establishConnection(uri, replyProcessor).whenComplete((r, e) -> {
            if (e != null) {
                connect.completeExceptionally(new ConnectionFailedException(e));
            } else {
                connect.complete(r);
            }
        });

        connect.thenAccept(connection -> {
            try {
                connection.send(request);
            } catch (ConnectionFailedException cfe) {
                throw new WireCommandFailedException(cfe, request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }).whenComplete((r, e) -> {
            if (e != null) {
                Throwable cause = ExceptionHelpers.getRealException(e);
                if (cause instanceof WireCommandFailedException) {
                    result.completeExceptionally(cause);
                } else if (cause instanceof ConnectionFailedException) {
                    result.completeExceptionally(new WireCommandFailedException(cause, request.getType(), WireCommandFailedException.Reason.ConnectionFailed));
                } else {
                    result.completeExceptionally(new RuntimeException(cause));
                }
            } else {
                result.complete(r);
            }
        });

        return result;
    }

    private <T> void whenComplete(CompletableFuture<Void> future, CompletableFuture<T> result) {
        future.whenComplete((res, ex) -> {
            if (ex != null) {
                Throwable cause = ExceptionHelpers.getRealException(ex);
                if (cause instanceof WireCommandFailedException) {
                    result.completeExceptionally(ex);
                } else {
                    result.completeExceptionally(new RuntimeException(ex));
                }
            }
        });
    }

    private Pair<Byte, Integer> extractFromPolicy(ScalingPolicy policy) {
        final int desiredRate;
        final byte rateType;
        if (policy.getType().equals(ScalingPolicy.Type.FIXED_NUM_SEGMENTS)) {
            desiredRate = 0;
            rateType = WireCommands.CreateSegment.NO_SCALE;
        } else {
            desiredRate = Math.toIntExact(policy.getTargetRate());
            if (policy.getType().equals(ScalingPolicy.Type.BY_RATE_IN_KBYTES_PER_SEC)) {
                rateType = WireCommands.CreateSegment.IN_KBYTES_PER_SEC;
            } else {
                rateType = WireCommands.CreateSegment.IN_EVENTS_PER_SEC;
            }
        }

        return new ImmutablePair<>(rateType, desiredRate);
    }
}
