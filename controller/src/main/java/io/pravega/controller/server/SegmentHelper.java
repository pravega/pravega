/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import com.google.common.base.Preconditions;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.Exceptions;
import io.pravega.common.auth.AuthenticationException;
import io.pravega.common.cluster.Host;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedStreamSegmentName;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getTransactionNameFromId;

@Slf4j
public class SegmentHelper {

    private final Supplier<Long> idGenerator = new AtomicLong(0)::incrementAndGet;

    public Controller.NodeUri getSegmentUri(final String scope,
                                            final String stream,
                                            final long segmentId,
                                            final HostControllerStore hostStore) {
        final Host host = hostStore.getHostForSegment(scope, stream, segmentId);
        return Controller.NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build();
    }

    public CompletableFuture<Boolean> createSegment(final String scope,
                                                    final String stream,
                                                    final long segmentId,
                                                    final ScalingPolicy policy,
                                                    final HostControllerStore hostControllerStore,
                                                    final ConnectionFactory clientCF, String controllerToken) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {
                log.warn("CreateSegment {} Connection dropped", qualifiedStreamSegmentName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("CreateSegment {} wrong host", qualifiedStreamSegmentName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                log.info("CreateSegment {} segmentAlreadyExists", qualifiedStreamSegmentName);
                result.complete(true);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                log.info("CreateSegment {} SegmentCreated", qualifiedStreamSegmentName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("CreateSegment {} threw exception", qualifiedStreamSegmentName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        WireCommands.CreateSegment request = new WireCommands.CreateSegment(idGenerator.get(),
                qualifiedStreamSegmentName, extracted.getLeft(), extracted.getRight(), controllerToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Boolean> truncateSegment(final String scope,
                                                      final String stream,
                                                      final long segmentId,
                                                      final long offset,
                                                      final HostControllerStore hostControllerStore,
                                                      final ConnectionFactory clientCF, String delegationToken) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.TRUNCATE_SEGMENT;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("truncateSegment {} Connection dropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("truncateSegment {} Wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {
                log.info("truncateSegment {} SegmentTruncated", qualifiedName);
                result.complete(true);
            }
            
            @Override
            public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {
                log.info("truncateSegment {} SegmentIsTruncated", qualifiedName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("truncateSegment {} error", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.TruncateSegment request = new WireCommands.TruncateSegment(idGenerator.get(),
                qualifiedName, offset, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Boolean> deleteSegment(final String scope,
                                                    final String stream,
                                                    final long segmentId,
                                                    final HostControllerStore hostControllerStore,
                                                    final ConnectionFactory clientCF, String delegationToken) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("deleteSegment {} Connection dropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("deleteSegment {} wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.info("deleteSegment {} NoSuchSegment", qualifiedName);
                result.complete(true);
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                log.info("deleteSegment {} SegmentDeleted", qualifiedName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("deleteSegment {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(idGenerator.get(),
                qualifiedName, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends segment sealed message for the specified segment.
     *
     * @param scope               stream scope
     * @param stream              stream name
     * @param segmentId       number of segment to be sealed
     * @param hostControllerStore host controller store
     * @param clientCF            connection factory
     * @param delegationToken     the token to be presented to segmentstore.
     * @return void
     */
    public CompletableFuture<Boolean> sealSegment(final String scope,
                                                  final String stream,
                                                  final long segmentId,
                                                  final HostControllerStore hostControllerStore,
                                                  final ConnectionFactory clientCF, String delegationToken) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        return sealSegment(qualifiedName, uri, clientCF, delegationToken);
    }

    private CompletableFuture<Boolean> sealSegment(final String qualifiedName,
                                                   final Controller.NodeUri uri,
                                                   final ConnectionFactory clientCF,
                                                   final String delegationToken) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {
                log.warn("sealSegment {} connectionDropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("sealSegment {} wrongHost", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                log.info("sealSegment {} segmentSealed", qualifiedName);
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                log.info("sealSegment {} SegmentIsSealed", qualifiedName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("sealSegment {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.SealSegment request = new WireCommands.SealSegment(idGenerator.get(),
                qualifiedName, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<UUID> createTransaction(final String scope,
                                                     final String stream,
                                                     final long segmentId,
                                                     final UUID txId,
                                                     final HostControllerStore hostControllerStore,
                                                     final ConnectionFactory clientCF, String delegationToken) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);
        final String transactionName = getTransactionName(scope, stream, segmentId, txId);

        final CompletableFuture<UUID> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("createTransaction {} connectionDropped", transactionName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("createTransaction {} wrong host", transactionName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated transactionCreated) {
                log.debug("createTransaction {} TransactionCreated", transactionName);

                result.complete(txId);
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                log.debug("createTransaction {} TransactionCreated", transactionName);
                result.complete(txId);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("createTransaction {} failed", transactionName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.CreateSegment request = new WireCommands.CreateSegment(idGenerator.get(), transactionName,
                WireCommands.CreateSegment.NO_SCALE, 0, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    private String getTransactionName(String scope, String stream, long segmentId, UUID txId) {
        // Transaction segments are created against a logical primary such that all transaction segments become mergeable.
        // So we will erase secondary id while creating transaction's qualified name.
        long generalizedSegmentId = TableHelper.generalizedSegmentId(segmentId, txId);

        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, generalizedSegmentId);
        return getTransactionNameFromId(qualifiedName, txId);
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope,
                                                          final String stream,
                                                          final long targetSegmentId,
                                                          final long sourceSegmentId,
                                                          final UUID txId,
                                                          final HostControllerStore hostControllerStore,
                                                          final ConnectionFactory clientCF, String delegationToken) {
        Preconditions.checkArgument(getSegmentNumber(targetSegmentId) == getSegmentNumber(sourceSegmentId));
        final Controller.NodeUri uri = getSegmentUri(scope, stream, sourceSegmentId, hostControllerStore);
        final String qualifiedNameTarget = getQualifiedStreamSegmentName(scope, stream, targetSegmentId);
        final String transactionName = getTransactionName(scope, stream, sourceSegmentId, txId);
        final CompletableFuture<TxnStatus> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.MERGE_SEGMENTS;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("commitTransaction {} connection dropped", transactionName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("commitTransaction {} wrongHost", transactionName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
                log.debug("commitTransaction {} TransactionCommitted", transactionName);
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                if (noSuchSegment.getSegment().equals(transactionName)) {
                    log.info("commitTransaction {} NoSuchSegment", transactionName);
                    result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
                } else {
                    log.warn("commitTransaction {} Source Segment not found", noSuchSegment.getSegment());
                    result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build());
                }
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("commitTransaction {} failed", transactionName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed)
                );
            }
        };

        WireCommands.MergeSegments request = new WireCommands.MergeSegments(idGenerator.get(),
                qualifiedNameTarget, transactionName, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                         final String stream,
                                                         final long segmentId,
                                                         final UUID txId,
                                                         final HostControllerStore hostControllerStore,
                                                         final ConnectionFactory clientCF, String delegationToken) {
        final String transactionName = getTransactionName(scope, stream, segmentId, txId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);
        final CompletableFuture<TxnStatus> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("abortTransaction {} connectionDropped", transactionName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("abortTransaction {} wrongHost", transactionName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted transactionAborted) {
                log.debug("abortTransaction {} transactionAborted", transactionName);
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.info("abortTransaction {} NoSuchSegment", transactionName);
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }

            @Override
            public void processingFailure(Exception error) {
                log.info("abortTransaction {} failed", transactionName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(idGenerator.get(), transactionName, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Void> updatePolicy(String scope, String stream, ScalingPolicy policy,
                                                long segmentId, HostControllerStore hostControllerStore,
                                                ConnectionFactory clientCF, String delegationToken) {
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);

        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_POLICY;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("updatePolicy {} connectionDropped", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("updatePolicy {} wrongHost", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated policyUpdated) {
                log.info("updatePolicy {} SegmentPolicyUpdated", qualifiedName);
                result.complete(null);
            }

            @Override
            public void processingFailure(Exception error) {
                log.info("updatePolicy {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        WireCommands.UpdateSegmentPolicy request = new WireCommands.UpdateSegmentPolicy(idGenerator.get(),
                qualifiedName, extracted.getLeft(), extracted.getRight(), delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<WireCommands.StreamSegmentInfo> getSegmentInfo(String scope, String stream, long segmentId,
                                                                            HostControllerStore hostControllerStore, ConnectionFactory clientCF, String delegationToken) {
        final CompletableFuture<WireCommands.StreamSegmentInfo> result = new CompletableFuture<>();
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);

        final WireCommandType type = WireCommandType.GET_STREAM_SEGMENT_INFO;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("getSegmentInfo {} connectionDropped", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("getSegmentInfo {} WrongHost", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {
                log.info("getSegmentInfo {} got response", qualifiedName);
                result.complete(streamInfo);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("getSegmentInfo {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.GetStreamSegmentInfo request = new WireCommands.GetStreamSegmentInfo(idGenerator.get(),
                qualifiedName, delegationToken);
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
            Throwable cause = Exceptions.unwrap(e);
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
            connectionFuture.thenAccept(ClientConnection::close);
        });
    }

    private Pair<Byte, Integer> extractFromPolicy(ScalingPolicy policy) {
        final int desiredRate;
        final byte rateType;
        if (policy.getScaleType().equals(ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS)) {
            desiredRate = 0;
            rateType = WireCommands.CreateSegment.NO_SCALE;
        } else {
            desiredRate = Math.toIntExact(policy.getTargetRate());
            if (policy.getScaleType().equals(ScalingPolicy.ScaleType.BY_RATE_IN_KBYTES_PER_SEC)) {
                rateType = WireCommands.CreateSegment.IN_KBYTES_PER_SEC;
            } else {
                rateType = WireCommands.CreateSegment.IN_EVENTS_PER_SEC;
            }
        }

        return new ImmutablePair<>(rateType, desiredRate);
    }
}
