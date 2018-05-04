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

import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.Exceptions;
import io.pravega.common.auth.AuthenticationException;
import io.pravega.common.cluster.Host;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import static io.pravega.shared.segment.StreamSegmentNameUtils.getPrimaryId;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getSecondaryId;

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
        int primary = getPrimaryId(segmentId);
        int secondary = getSecondaryId(segmentId);

        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);

        final WireCommandType type = WireCommandType.CREATE_SEGMENT;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("CreateSegment {}/{}/{}.#{} Connection dropped", scope, stream, primary, secondary);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("CreateSegment {}/{}/{}.#{} wrong host", scope, stream, primary, secondary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                log.info("CreateSegment {}/{}/{}.#{} segmentAlreadyExists", scope, stream, primary, secondary);
                result.complete(true);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                log.info("CreateSegment {}/{}/{}.#{} SegmentCreated", scope, stream, primary, secondary);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("CreateSegment {}/{}/{}.#{} threw exception", scope, stream, primary, secondary, error);
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

        // TODO: After client is updated to handle new scheme (#2469) we should send scoped stream name that makes use of
        // actual segment id as opposed to only primary id
        WireCommands.CreateSegment request = new WireCommands.CreateSegment(idGenerator.get(),
                Segment.getScopedName(scope, stream, getPrimaryId(segmentId)), extracted.getLeft(), extracted.getRight(), controllerToken);
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
        int primary = getPrimaryId(segmentId);
        int secondary = getSecondaryId(segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);

        final WireCommandType type = WireCommandType.TRUNCATE_SEGMENT;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("truncateSegment {}/{}/{}/#{} Connection dropped", scope, stream, primary, secondary);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("truncateSegment {}/{}/{}/#{} Wrong host", scope, stream, primary, secondary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {
                log.info("truncateSegment {}/{}/{}/#{} SegmentTruncated", scope, stream, primary, secondary);
                result.complete(true);
            }
            
            @Override
            public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {
                log.info("truncateSegment {}/{}/{}/#{} SegmentIsTruncated", scope, stream, primary, secondary);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("truncateSegment {}/{}/{}/#{} error", scope, stream, primary, secondary, error);
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
                Segment.getScopedName(scope, stream, (int) segmentId), offset, delegationToken);
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
        int primary = getPrimaryId(segmentId);
        int secondary = getSecondaryId(segmentId);

        final WireCommandType type = WireCommandType.DELETE_SEGMENT;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("deleteSegment {}/{}/{}/#{} Connection dropped", scope, stream, primary, secondary);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("deleteSegment {}/{}/{}/#{} wrong host", scope, stream, primary, secondary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.info("deleteSegment {}/{}/{}/#{} NoSuchSegment", scope, stream, primary, secondary);
                result.complete(true);
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                log.info("deleteSegment {}/{}/{}/#{} SegmentDeleted", scope, stream, primary, secondary);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("deleteSegment {}/{}/{}/#{} failed", scope, stream, primary, secondary, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        // TODO: After client is updated to handle new scheme (#2469) we should send scoped stream name that makes use of
        // actual segment id as opposed to only primary id
        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(idGenerator.get(),
                Segment.getScopedName(scope, stream, getPrimaryId(segmentId)), delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends segment sealed message for the specified segment.
     * It owns up the responsibility of retrying the operation on failures until success.
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
        int primary = getPrimaryId(segmentId);
        int secondary = getSecondaryId(segmentId);
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {
                log.warn("sealSegment {}/{}/{}/#{} connectionDropped", scope, stream, primary, secondary);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("sealSegment {}/{}/{}/#{} wrongHost", scope, stream, primary, secondary);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                log.info("sealSegment {}/{}/{}/#{} segmentSealed", scope, stream, primary, secondary);
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                log.info("sealSegment {}/{}/{}/#{} SegmentIsSealed", scope, stream, primary, secondary);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("sealSegment {}/{}/{}/#{} failed", scope, stream, primary, secondary, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        // TODO: After client is updated to handle new scheme (#2469) we should send scoped stream name that makes use of
        // actual segment id as opposed to only primary id
        WireCommands.SealSegment request = new WireCommands.SealSegment(idGenerator.get(),
                Segment.getScopedName(scope, stream, getPrimaryId(segmentId)), delegationToken);
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
        int primary = getPrimaryId(segmentId);

        final CompletableFuture<UUID> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.CREATE_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("createTransaction {}/{}/{}/{} connectionDropped", scope, stream, primary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("createTransaction {}/{}/{} wrong host", scope, stream, primary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCreated(WireCommands.TransactionCreated transactionCreated) {
                log.debug("createTransaction {}/{}/{} TransactionCreated", scope, stream, primary);

                result.complete(txId);
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                log.debug("createTransaction {}/{}/{} TransactionCreated", scope, stream, primary);
                result.complete(txId);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("createTransaction {}/{}/{} failed", scope, stream, primary, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.CreateTransaction request = new WireCommands.CreateTransaction(idGenerator.get(),
                Segment.getScopedName(scope, stream, getPrimaryId(segmentId)), txId, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope,
                                                          final String stream,
                                                          final long segmentId,
                                                          final UUID txId,
                                                          final HostControllerStore hostControllerStore,
                                                          final ConnectionFactory clientCF, String delegationToken) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);
        int primary = getPrimaryId(segmentId);

        final CompletableFuture<TxnStatus> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.COMMIT_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("commitTransaction {}/{}/{} connection dropped", scope, stream, primary);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("commitTransaction {}/{}/{} wrongHost", scope, stream, primary);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
                log.debug("commitTransaction {}/{}/{} TransactionCommitted", scope, stream, primary);
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }

            @Override
            public void transactionAborted(WireCommands.TransactionAborted transactionAborted) {
                log.warn("commitTransaction {}/{}/{} Transaction aborted", scope, stream, primary);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.PreconditionFailed));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.info("commitTransaction {}/{}/{} NoSuchSegment", scope, stream, primary);
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("commitTransaction {}/{}/{} failed", scope, stream, primary, error);
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

        WireCommands.CommitTransaction request = new WireCommands.CommitTransaction(idGenerator.get(),
                Segment.getScopedName(scope, stream, getPrimaryId(segmentId)), txId, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                         final String stream,
                                                         final long segmentId,
                                                         final UUID txId,
                                                         final HostControllerStore hostControllerStore,
                                                         final ConnectionFactory clientCF, String delegationToken) {
        int primary = getPrimaryId(segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);
        final CompletableFuture<TxnStatus> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.ABORT_TRANSACTION;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("abortTransaction {}/{}/{} connectionDropped", scope, stream, primary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("abortTransaction {}/{}/{} wrongHost", scope, stream, primary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
                log.warn("abortTransaction {}/{}/{} TransactionCommitted", scope, stream, primary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.PreconditionFailed));
            }

            @Override
            public void transactionAborted(WireCommands.TransactionAborted transactionDropped) {
                log.debug("abortTransaction {}/{}/{} transactionAborted", scope, stream, primary);
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.info("abortTransaction {}/{}/{} NoSuchSegment", scope, stream, primary);
                result.complete(TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
            }

            @Override
            public void processingFailure(Exception error) {
                log.info("abortTransaction {}/{}/{} failed", scope, stream, primary, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.AbortTransaction request = new WireCommands.AbortTransaction(idGenerator.get(),
                Segment.getScopedName(scope, stream, getPrimaryId(segmentId)), txId, delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Void> updatePolicy(String scope, String stream, ScalingPolicy policy,
                                                long segmentId, HostControllerStore hostControllerStore,
                                                ConnectionFactory clientCF, String delegationToken) {
        final int primary = getPrimaryId(segmentId);
        final int secondary = getSecondaryId(segmentId);
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);

        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_POLICY;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("updatePolicy {}/{}/{}/#{} connectionDropped", scope, stream, primary, secondary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("updatePolicy {}/{}/{}/#{} wrongHost", scope, stream, primary, secondary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated policyUpdated) {
                log.info("updatePolicy {}/{}/{}/#{} SegmentPolicyUpdated", scope, stream, primary, secondary);
                result.complete(null);
            }

            @Override
            public void processingFailure(Exception error) {
                log.info("updatePolicy {}/{}/{}/#{} failed", scope, stream, primary, secondary, error);
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

        // TODO: After client is updated to handle new scheme (#2469) we should send scoped stream name that makes use of
        // actual segment id as opposed to only primary id
        WireCommands.UpdateSegmentPolicy request = new WireCommands.UpdateSegmentPolicy(idGenerator.get(),
                Segment.getScopedName(scope, stream, getPrimaryId(segmentId)), extracted.getLeft(), extracted.getRight(), delegationToken);
        sendRequestAsync(request, replyProcessor, result, clientCF, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<WireCommands.StreamSegmentInfo> getSegmentInfo(String scope, String stream, long segmentId,
                                                                            HostControllerStore hostControllerStore, ConnectionFactory clientCF, String delegationToken) {
        final int primary = getPrimaryId(segmentId);
        final int secondary = getSecondaryId(segmentId);
        final CompletableFuture<WireCommands.StreamSegmentInfo> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId, hostControllerStore);

        final WireCommandType type = WireCommandType.GET_STREAM_SEGMENT_INFO;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn("getSegmentInfo {}/{}/{}/#{} connectionDropped", scope, stream, primary, secondary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn("getSegmentInfo {}/{}/{}/#{} WrongHost", scope, stream, primary, secondary);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {
                log.info("getSegmentInfo {}/{}/{}/#{} got response", scope, stream, primary, secondary);
                result.complete(streamInfo);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error("getSegmentInfo {}/{}/{}/#{} failed", scope, stream, primary, secondary, error);
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
                Segment.getScopedName(scope, stream, getPrimaryId(segmentId)), delegationToken);
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
