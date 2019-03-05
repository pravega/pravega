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
import io.netty.buffer.ByteBuf;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.client.tables.impl.TableSegment;
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Host;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedStreamSegmentName;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getTransactionNameFromId;

public class SegmentHelper {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(SegmentHelper.class));

    private final Supplier<Long> idGenerator = new AtomicLong(0)::incrementAndGet;
    private final HostControllerStore hostStore;
    private final ConnectionFactory clientCF;
    private final AuthHelper authHelper;

    public SegmentHelper(HostControllerStore hostControllerStore, ConnectionFactory clientCF, AuthHelper authHelper) {
        this.hostStore = hostControllerStore;
        this.clientCF = clientCF;
        this.authHelper = authHelper;
    }

    public Controller.NodeUri getSegmentUri(final String scope,
                                            final String stream,
                                            final long segmentId) {
        final Host host = hostStore.getHostForSegment(scope, stream, segmentId);
        return Controller.NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build();
    }

    public CompletableFuture<Boolean> createSegment(final String scope,
                                                    final String stream,
                                                    final long segmentId,
                                                    final ScalingPolicy policy,
                                                    final String controllerToken,
                                                    final long clientRequestId) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {
                log.warn(requestId, "CreateSegment {} Connection dropped", qualifiedStreamSegmentName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "CreateSegment {} wrong host", qualifiedStreamSegmentName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                log.info(requestId, "CreateSegment {} segmentAlreadyExists", qualifiedStreamSegmentName);
                result.complete(true);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                log.info(requestId, "CreateSegment {} SegmentCreated", qualifiedStreamSegmentName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "CreateSegment {} threw exception", qualifiedStreamSegmentName, error);
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

        WireCommands.CreateSegment request = new WireCommands.CreateSegment(requestId, qualifiedStreamSegmentName,
                extracted.getLeft(), extracted.getRight(), controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Boolean> truncateSegment(final String scope,
                                                      final String stream,
                                                      final long segmentId,
                                                      final long offset,
                                                      final String controllerToken,
                                                      final long clientRequestId) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.TRUNCATE_SEGMENT;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "truncateSegment {} Connection dropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "truncateSegment {} Wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {
                log.info(requestId, "truncateSegment {} SegmentTruncated", qualifiedName);
                result.complete(true);
            }
            
            @Override
            public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {
                log.info(requestId, "truncateSegment {} SegmentIsTruncated", qualifiedName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "truncateSegment {} error", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.TruncateSegment request = new WireCommands.TruncateSegment(requestId, qualifiedName, offset, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Boolean> deleteSegment(final String scope,
                                                    final String stream,
                                                    final long segmentId,
                                                    final String controllerToken,
                                                    final long clientRequestId) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "deleteSegment {} Connection dropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "deleteSegment {} wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.info(requestId, "deleteSegment {} NoSuchSegment", qualifiedName);
                result.complete(true);
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                log.info(requestId, "deleteSegment {} SegmentDeleted", qualifiedName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "deleteSegment {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(requestId, qualifiedName, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends segment sealed message for the specified segment.
     *
     * @param scope               stream scope
     * @param stream              stream name
     * @param segmentId           number of segment to be sealed
     * @param clientRequestId     client-generated id for end-to-end tracing
     * @return void
     */
    public CompletableFuture<Boolean> sealSegment(final String scope,
                                                  final String stream,
                                                  final long segmentId,
                                                  final String controllerToken,
                                                  final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;
        return sealSegment(qualifiedName, uri, controllerToken, requestId);
    }

    private CompletableFuture<Boolean> sealSegment(final String qualifiedName,
                                                   final Controller.NodeUri uri,
                                                   final String controllerToken,
                                                   long requestId) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {
                log.warn(requestId, "sealSegment {} connectionDropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "sealSegment {} wrongHost", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
                log.info(requestId, "sealSegment {} segmentSealed", qualifiedName);
                result.complete(true);
            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
                log.info(requestId, "sealSegment {} SegmentIsSealed", qualifiedName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "sealSegment {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.SealSegment request = new WireCommands.SealSegment(requestId, qualifiedName, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<UUID> createTransaction(final String scope,
                                                     final String stream,
                                                     final long segmentId,
                                                     final UUID txId,
                                                     final String controllerToken) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
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
                WireCommands.CreateSegment.NO_SCALE, 0, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    private String getTransactionName(String scope, String stream, long segmentId, UUID txId) {
        // Transaction segments are created against a logical primary such that all transaction segments become mergeable.
        // So we will erase secondary id while creating transaction's qualified name.
        long generalizedSegmentId = RecordHelper.generalizedSegmentId(segmentId, txId);

        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, generalizedSegmentId);
        return getTransactionNameFromId(qualifiedName, txId);
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope,
                                                          final String stream,
                                                          final long targetSegmentId,
                                                          final long sourceSegmentId,
                                                          final UUID txId,
                                                          final String controllerToken) {
        Preconditions.checkArgument(getSegmentNumber(targetSegmentId) == getSegmentNumber(sourceSegmentId));
        final Controller.NodeUri uri = getSegmentUri(scope, stream, sourceSegmentId);
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
                qualifiedNameTarget, transactionName, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                         final String stream,
                                                         final long segmentId,
                                                         final UUID txId,
                                                         final String controllerToken) {
        final String transactionName = getTransactionName(scope, stream, segmentId, txId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
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

        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(idGenerator.get(), transactionName, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Void> updatePolicy(String scope, String stream, ScalingPolicy policy, long segmentId,
                                                final String controllerToken, long clientRequestId) {
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_POLICY;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "updatePolicy {} connectionDropped", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "updatePolicy {} wrongHost", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated policyUpdated) {
                log.info(requestId, "updatePolicy {} SegmentPolicyUpdated", qualifiedName);
                result.complete(null);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "updatePolicy {} failed", qualifiedName, error);
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

        WireCommands.UpdateSegmentPolicy request = new WireCommands.UpdateSegmentPolicy(requestId,
                qualifiedName, extracted.getLeft(), extracted.getRight(), controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<WireCommands.StreamSegmentInfo> getSegmentInfo(String scope, String stream, long segmentId,
                                                                            final String controllerToken) {
        final CompletableFuture<WireCommands.StreamSegmentInfo> result = new CompletableFuture<>();
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);

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
                qualifiedName, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends a WireCommand to create a table segment.
     *
     * @param scope               Stream scope.
     * @param stream              Stream name.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed normally, will indicate the table segment creation completed
     * successfully. If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Boolean> createTableSegment(final String scope,
                                                         final String stream,
                                                         final String controllerToken,
                                                         final long clientRequestId) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, 0L);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, 0L);
        final WireCommandType type = WireCommandType.CREATE_TABLE_SEGMENT;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {
                log.warn(requestId, "CreateTableSegment {} Connection dropped", qualifiedStreamSegmentName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "CreateTableSegment {} wrong host", qualifiedStreamSegmentName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                log.info(requestId, "CreateTableSegment {} segmentAlreadyExists", qualifiedStreamSegmentName);
                result.complete(true);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                log.info(requestId, "CreateTableSegment {} SegmentCreated", qualifiedStreamSegmentName);
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "CreateTableSegment {} threw exception", qualifiedStreamSegmentName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.CreateTableSegment request = new WireCommands.CreateTableSegment(requestId, qualifiedStreamSegmentName, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends a WireCommand to delete a table segment.
     *
     * @param scope               Stream scope.
     * @param stream              Stream name.
     * @param mustBeEmpty         Flag to check if the table segment should be empty before deletion.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed normally, will indicate the table segment deletion completed
     * successfully. If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Boolean> deleteTableSegment(final String scope,
                                                         final String stream,
                                                         final boolean mustBeEmpty,
                                                         final String controllerToken,
                                                         final long clientRequestId) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, 0L);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, 0L);
        final WireCommandType type = WireCommandType.DELETE_TABLE_SEGMENT;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "deleteTableSegment {} Connection dropped.", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "deleteTableSegment {} wrong host.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.info(requestId, "deleteTableSegment {} NoSuchSegment.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                log.info(requestId, "deleteTableSegment {} SegmentDeleted.", qualifiedName);
                result.complete(true);
            }

            @Override
            public void tableSegmentNotEmpty(WireCommands.TableSegmentNotEmpty tableSegmentNotEmpty) {
                log.warn(requestId, "deleteTableSegment {} TableSegmentNotEmpty.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableSegmentNotEmpty));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "deleteTableSegment {} failed.", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.DeleteTableSegment request = new WireCommands.DeleteTableSegment(requestId, qualifiedName, mustBeEmpty, controllerToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends a WireCommand to update table entries.
     *
     * @param scope               Stream scope.
     * @param stream              Stream name.
     * @param entries             List of {@link TableEntry}s to be updated.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed normally, will contain the current versions of each {@link TableEntry}
     * If the operation failed, the future will be failed with the causing exception. If the exception can be retried
     * then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<List<KeyVersion>> updateTableEntries(final String scope,
                                                                  final String stream,
                                                                  final List<TableEntry<byte[], byte[]>> entries,
                                                                  final String controllerToken,
                                                                  final long clientRequestId) {
        final CompletableFuture<List<KeyVersion>> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, 0L);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, 0L);
        final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "updateTableEntries {} Connection dropped", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "updateTableEntries {} wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "updateTableEntries {} NoSuchSegment", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {
                log.info(requestId, "updateTableEntries request for {} tableSegment completed.", qualifiedName);
                result.complete(tableEntriesUpdated.getUpdatedVersions().stream().map(KeyVersionImpl::new).collect(Collectors.toList()));
            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
                log.warn(requestId, "updateTableEntries request for {} tableSegment failed with TableKeyDoesNotExist.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist));
            }

            @Override
            public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
                log.warn(requestId, "updateTableEntries request for {} tableSegment failed with TableKeyBadVersion.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyBadVersion));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "updateTableEntries {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> wireCommandEntries = entries.stream().map(te -> {
            final WireCommands.TableKey key = convertToWireCommand(te.getKey());
            final WireCommands.TableValue value = new WireCommands.TableValue(wrappedBuffer(te.getValue()));
            return new AbstractMap.SimpleImmutableEntry<>(key, value);
        }).collect(Collectors.toList());

        WireCommands.UpdateTableEntries request = new WireCommands.UpdateTableEntries(requestId, qualifiedName, controllerToken,
                                                                                      new WireCommands.TableEntries(wireCommandEntries));
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends a WireCommand to remove table keys.
     *
     * @param scope               Stream scope.
     * @param stream              Stream name.
     * @param keys                List of {@link TableKey}s to be removed. Only if all the elements in the list has version as
     *                            {@link KeyVersion#NOT_EXISTS} then an unconditional update/removal is performed. Else an atomic conditional
     *                            update (removal) is performed.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that will complete normally when the provided keys are deleted.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be
     * retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Void> removeTableKeys(final String scope,
                                                   final String stream,
                                                   final List<TableKey<byte[]>> keys,
                                                   final String controllerToken,
                                                   final long clientRequestId) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, 0L);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, 0L);
        final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "removeTableKeys {} Connection dropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "removeTableKeys {} Wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "removeTableKeys {} NoSuchSegment", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {
                log.info(requestId, "removeTableKeys {} completed.", qualifiedName);
                result.complete(null);
            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
                log.info(requestId, "removeTableKeys request for {} tableSegment failed with TableKeyDoesNotExist.", qualifiedName);
                result.complete(null);
            }

            @Override
            public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
                log.warn(requestId, "removeTableKeys request for {} tableSegment failed with TableKeyBadVersion.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyBadVersion));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "removeTableKeys {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        List<WireCommands.TableKey> keyList = keys.stream().map(this::convertToWireCommand).collect(Collectors.toList());

        WireCommands.RemoveTableKeys request = new WireCommands.RemoveTableKeys(requestId, qualifiedName, controllerToken, keyList);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends a WireCommand to read table entries.
     *
     * @param scope               Stream scope.
     * @param stream              Stream name.
     * @param keys                List of {@link TableKey}s to be read. {@link TableKey#getVersion()} is not used
     *                            during this operation and the latest version is read.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed normally, will contain a list of {@link TableEntry} with
     * a value corresponding to the latest version. If the operation failed, the future will be failed with the
     * causing exception. If the exception can be retried then the future will be failed with
     * {@link WireCommandFailedException}.
     */
    public CompletableFuture<List<TableEntry<byte[], byte[]>>> readTable(final String scope,
                                                                         final String stream,
                                                                         final List<TableKey<byte[]>> keys,
                                                                         final String controllerToken,
                                                                         final long clientRequestId) {
        final CompletableFuture<List<TableEntry<byte[], byte[]>>> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getSegmentUri(scope, stream, 0L);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, 0L);
        final WireCommandType type = WireCommandType.READ_TABLE;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "readTable {} Connection dropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "readTable {} wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "readTable {} NoSuchSegment", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableRead(WireCommands.TableRead tableRead) {
                log.info(requestId, "readTable {} successful.", qualifiedName);
                AtomicBoolean allKeysFound = new AtomicBoolean(true);
                List<TableEntry<byte[], byte[]>> tableEntries = tableRead.getEntries().getEntries().stream()
                                                                         .map(e -> {
                                                                             WireCommands.TableKey k = e.getKey();
                                                                             TableKey<byte[]> tableKey =
                                                                                     new TableKeyImpl<>(getArray(k.getData()),
                                                                                             new KeyVersionImpl(k.getKeyVersion()));
                                                                             // Hack added to return KeyDoesNotExist if key version is Long.Min
                                                                             allKeysFound.compareAndSet(true, k.getKeyVersion() != WireCommands.TableKey.NO_VERSION);
                                                                             return new TableEntryImpl<>(tableKey, getArray(e.getValue().getData()));
                                                                         }).collect(Collectors.toList());
                if (allKeysFound.get()) {
                    result.complete(tableEntries);
                } else {
                    // Hack added to return KeyDoesNotExist if key version is Long.Min
                    result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist));
                }
            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
                log.warn(requestId, "readTable request for {} tableSegment failed with TableKeyDoesNotExist.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "readTable {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        // the version is always NO_VERSION as read returns the latest version of value.
        List<WireCommands.TableKey> keyList = keys.stream().map(k -> new WireCommands.TableKey(wrappedBuffer(k.getKey()),
                WireCommands.TableKey.NO_VERSION))
                                                  .collect(Collectors.toList());

        WireCommands.ReadTable request = new WireCommands.ReadTable(requestId, qualifiedName, controllerToken, keyList);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    /**
     * The method sends a WireCommand to iterate over table keys.
     * @param scope Stream scope.
     * @param stream Stream name.
     * @param suggestedKeyCount Suggested number of {@link TableKey}s to be returned by the SegmentStore.
     * @param state Last known state of the iterator.
     * @param clientRequestId Request id.
     * @return A CompletableFuture that will return the next set of {@link TableKey}s returned from the SegmentStore.
     */
    public CompletableFuture<TableSegment.IteratorItem<TableKey<byte[]>>> readTableKeys(final String scope,
                                                                                    final String stream,
                                                                                    final int suggestedKeyCount,
                                                                                    final IteratorState state,
                                                                                        final String controllerToken,
                                                                                    final long clientRequestId) {

        final Controller.NodeUri uri = getSegmentUri(scope, stream, 0L);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, 0L);
        final WireCommandType type = WireCommandType.READ_TABLE_KEYS;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;
        final IteratorState token = (state == null) ? IteratorState.EMPTY : state;

        final CompletableFuture<TableSegment.IteratorItem<TableKey<byte[]>>> result = new CompletableFuture<>();
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "readTableKeys {} Connection dropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "readTableKeys {} wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "readTableKeys {} NoSuchSegment", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {
                log.info(requestId, "readTableKeys {} successful.", qualifiedName);
                final IteratorState state = IteratorState.fromBytes(tableKeysRead.getContinuationToken());
                final List<TableKey<byte[]>> keys =
                        tableKeysRead.getKeys().stream().map(k -> new TableKeyImpl<>(getArray(k.getData()),
                                new KeyVersionImpl(k.getKeyVersion()))).collect(Collectors.toList());
                result.complete(new TableSegment.IteratorItem<>(state, keys));
            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
                log.warn(requestId, "readTableKeys request for {} tableSegment failed with TableKeyDoesNotExist.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "readTableKeys {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.ReadTableKeys cmd = new WireCommands.ReadTableKeys(requestId, qualifiedName, controllerToken, suggestedKeyCount,
                token.toBytes());
        sendRequestAsync(cmd, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }


    /**
     * The method sends a WireCommand to iterate over table entries.
     * @param scope Stream scope.
     * @param stream Stream name.
     * @param suggestedEntryCount Suggested number of {@link TableKey}s to be returned by the SegmentStore.
     * @param state Last known state of the iterator.
     * @param clientRequestId Request id.
     * @return A CompletableFuture that will return the next set of {@link TableKey}s returned from the SegmentStore.
     */
    public CompletableFuture<TableSegment.IteratorItem<TableEntry<byte[], byte[]>>> readTableEntries(final String scope,
                                                                                                     final String stream,
                                                                                                     final int suggestedEntryCount,
                                                                                                     final IteratorState state,
                                                                                                     final String controllerToken,
                                                                                                     final long clientRequestId) {

        final Controller.NodeUri uri = getSegmentUri(scope, stream, 0L);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, 0L);
        final WireCommandType type = WireCommandType.READ_TABLE_ENTRIES;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;
        final IteratorState token = (state == null) ? IteratorState.EMPTY : state;

        final CompletableFuture<TableSegment.IteratorItem<TableEntry<byte[], byte[]>>> result = new CompletableFuture<>();
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "readTableEntries {} Connection dropped", qualifiedName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "readTableEntries {} wrong host", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "readTableEntries {} NoSuchSegment", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {
                log.info(requestId, "readTableEntries {} successful.", qualifiedName);
                final IteratorState state = IteratorState.fromBytes(tableEntriesRead.getContinuationToken());
                final List<TableEntry<byte[], byte[]>> entries =
                        tableEntriesRead.getEntries().getEntries().stream()
                                        .map(e -> {
                                            WireCommands.TableKey k = e.getKey();
                                            TableKey<byte[]> tableKey = new TableKeyImpl<>(getArray(k.getData()),
                                                                                           new KeyVersionImpl(k.getKeyVersion()));
                                            return new TableEntryImpl<>(tableKey, getArray(e.getValue().getData()));
                                        }).collect(Collectors.toList());
                result.complete(new TableSegment.IteratorItem<>(state, entries));
            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
                log.warn(requestId, "readTableEntries request for {} tableSegment failed with TableKeyDoesNotExist.", qualifiedName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "readTableEntries {} failed", qualifiedName, error);
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.ReadTableEntries cmd = new WireCommands.ReadTableEntries(requestId, qualifiedName, controllerToken,
                suggestedEntryCount, token.toBytes());
        sendRequestAsync(cmd, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    private byte[] getArray(ByteBuf buf) {
        final byte[] bytes = new byte[buf.readableBytes()];
        final int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return bytes;
    }

    private WireCommands.TableKey convertToWireCommand(final TableKey<byte[]> k) {
        WireCommands.TableKey key;
        if (k.getVersion() == null) {
            // unconditional update.
            key = new WireCommands.TableKey(wrappedBuffer(k.getKey()), WireCommands.TableKey.NO_VERSION);
        } else {
            key = new WireCommands.TableKey(wrappedBuffer(k.getKey()), k.getVersion().getSegmentVersion());
        }
        return key;
    }

    private <ResultT> void sendRequestAsync(final WireCommand request, final ReplyProcessor replyProcessor,
                                            final CompletableFuture<ResultT> resultFuture,
                                            final PravegaNodeUri uri) {
        CompletableFuture<ClientConnection> connectionFuture = clientCF.establishConnection(uri, replyProcessor);
        connectionFuture.whenComplete((connection, e) -> {
            if (connection == null) {
                resultFuture.completeExceptionally(new WireCommandFailedException(new ConnectionFailedException(e),
                        request.getType(),
                        WireCommandFailedException.Reason.ConnectionFailed));
            } else {                
                connection.sendAsync(request, cfe -> {
                    if (cfe != null) {
                        Throwable cause = Exceptions.unwrap(cfe);
                        if (cause instanceof ConnectionFailedException) {
                            resultFuture.completeExceptionally(new WireCommandFailedException(cause, request.getType(), WireCommandFailedException.Reason.ConnectionFailed));
                        } else {
                            resultFuture.completeExceptionally(new RuntimeException(cause));
                        }                        
                    }
                });                
            }
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

    public String retrieveMasterToken() {
        return authHelper.retrieveMasterToken();
    }
}
