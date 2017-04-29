/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.cluster.Host;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.Segment;
import io.pravega.stream.impl.ModelHelper;
import io.pravega.stream.impl.netty.ClientConnection;
import io.pravega.stream.impl.netty.ConnectionFactory;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class SegmentHelper {
    
    private final Supplier<Long> idGenerator = new AtomicLong(0)::incrementAndGet;

    public Controller.NodeUri getSegmentUri(final String scope,
                                            final String stream,
                                            final int segmentNumber,
                                            final HostControllerStore hostStore) {
        final Host host = hostStore.getHostForSegment(scope, stream, segmentNumber);
        return Controller.NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build();
    }

    public CompletableFuture<Boolean> createSegment(final String scope,
                                                    final String stream,
                                                    final int segmentNumber,
                                                    final ScalingPolicy policy,
                                                    final HostControllerStore hostControllerStore,
                                                    final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

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
                result.complete(true);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }
        };

        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        WireCommands.CreateSegment request = new WireCommands.CreateSegment(idGenerator.get(), 
                Segment.getScopedName(scope, stream, segmentNumber), extracted.getLeft(), extracted.getRight());
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Boolean> deleteSegment(final String scope,
                                                    final String stream,
                                                    final int segmentNumber,
                                                    final HostControllerStore hostControllerStore,
                                                    final ConnectionFactory clientCF) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

        final WireCommandType type = WireCommandType.DELETE_SEGMENT;

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
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                result.complete(true);
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }
        };

        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(idGenerator.get(), 
                Segment.getScopedName(scope, stream, segmentNumber));
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
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
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
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

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }
        };

        WireCommands.SealSegment request = new WireCommands.SealSegment(idGenerator.get(), 
                Segment.getScopedName(scope, stream, segmentNumber));
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<UUID> createTransaction(final String scope,
                                                     final String stream,
                                                     final int segmentNumber,
                                                     final UUID txId,
                                                     final HostControllerStore hostControllerStore,
                                                     final ConnectionFactory clientCF) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

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

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }
        };

        WireCommands.CreateTransaction request = new WireCommands.CreateTransaction(idGenerator.get(), 
                Segment.getScopedName(scope, stream, segmentNumber), txId);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope,
                                                          final String stream,
                                                          final int segmentNumber,
                                                          final UUID txId,
                                                          final HostControllerStore hostControllerStore,
                                                          final ConnectionFactory clientCF) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

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

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }
        };

        WireCommands.CommitTransaction request = new WireCommands.CommitTransaction(idGenerator.get(), 
                Segment.getScopedName(scope, stream, segmentNumber), txId);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                         final String stream,
                                                         final int segmentNumber,
                                                         final UUID txId,
                                                         final HostControllerStore hostControllerStore,
                                                         final ConnectionFactory clientCF) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);
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

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }
        };

        WireCommands.AbortTransaction request = new WireCommands.AbortTransaction(idGenerator.get(), 
                Segment.getScopedName(scope, stream, segmentNumber), txId);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Void> updatePolicy(String scope, String stream, ScalingPolicy policy,
                                                int segmentNumber, HostControllerStore hostControllerStore,
                                                ConnectionFactory clientCF) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentNumber, hostControllerStore);

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

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }
        };

        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        WireCommands.UpdateSegmentPolicy request = new WireCommands.UpdateSegmentPolicy(idGenerator.get(), 
                Segment.getScopedName(scope, stream, segmentNumber), extracted.getLeft(), extracted.getRight());
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    private <ResultT> void sendRequestAsync(final WireCommand request, final ReplyProcessor replyProcessor,
                                            final CompletableFuture<ResultT> resultFuture,
                                            final ConnectionFactory connectionFactory, final PravegaNodeUri uri) {
        CompletableFuture<ClientConnection> connectionFuture = connectionFactory.establishConnection(uri, replyProcessor);
        connectionFuture.whenComplete((connection, e) -> {
            if (connection == null) {
                resultFuture.completeExceptionally(new WireCommandFailedException(new ConnectionFailedException(e),
                        request.getType(),
                        WireCommandFailedException.Reason.ConnectionFailed));
            } else {
                try {
                    connection.send(request);
                } catch (ConnectionFailedException cfe) {
                    throw new WireCommandFailedException(cfe,
                            request.getType(),
                            WireCommandFailedException.Reason.ConnectionFailed);
                } catch (Exception e2) {
                    throw new RuntimeException(e2);
                }
            }
        }).exceptionally(e -> {
            Throwable cause = ExceptionHelpers.getRealException(e);
            if (cause instanceof WireCommandFailedException) {
                resultFuture.completeExceptionally(cause);
            } else if (cause instanceof ConnectionFailedException) {
                resultFuture.completeExceptionally(new WireCommandFailedException(cause, request.getType(), WireCommandFailedException.Reason.ConnectionFailed));
            } else {
                resultFuture.completeExceptionally(new RuntimeException(cause));
            }
            return null;
        });
        resultFuture.whenComplete((result, e) -> {
            connectionFuture.thenAccept(c -> c.close());
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
