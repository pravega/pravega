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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.RawClient;
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
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;

import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.controller.server.SegmentStoreConnectionManager.ConnectionWrapper;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedStreamSegmentName;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getTransactionNameFromId;

public class SegmentHelper implements AutoCloseable {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(SegmentHelper.class));

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
            .put(WireCommands.CreateSegment.class, ImmutableSet.of(WireCommands.SegmentCreated.class,
                    WireCommands.SegmentAlreadyExists.class))
            .put(WireCommands.CreateTableSegment.class, ImmutableSet.of(WireCommands.SegmentCreated.class,
                    WireCommands.SegmentAlreadyExists.class))
            .put(WireCommands.DeleteSegment.class, ImmutableSet.of(WireCommands.SegmentDeleted.class,
                    WireCommands.NoSuchSegment.class))
            .put(WireCommands.DeleteTableSegment.class, ImmutableSet.of(WireCommands.SegmentDeleted.class,
                    WireCommands.NoSuchSegment.class))
            .put(WireCommands.SealSegment.class, ImmutableSet.of(WireCommands.SegmentSealed.class,
                    WireCommands.SegmentIsSealed.class))
            .put(WireCommands.TruncateSegment.class, ImmutableSet.of(WireCommands.SegmentTruncated.class,
                    WireCommands.SegmentIsTruncated.class))
            .build();

    private final Supplier<Long> idGenerator = new AtomicLong(0)::incrementAndGet;

    private final SegmentStoreConnectionManager connectionManager;
    private final HostControllerStore hostStore;
    private final ConnectionFactory connectionFactory;

    public SegmentHelper(final ConnectionFactory clientCF, HostControllerStore hostStore) {
        connectionManager = new SegmentStoreConnectionManager(clientCF);
        this.connectionFactory = clientCF;
        this.hostStore = hostStore;
    }

    public Controller.NodeUri getSegmentUri(final String scope,
                                            final String stream,
                                            final long segmentId) {
        final Host host = hostStore.getHostForSegment(scope, stream, segmentId);
        return Controller.NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build();
    }

    public Controller.NodeUri getTableUri(final String tableName) {
        final Host host = hostStore.getHostForTableSegment(tableName);
        return Controller.NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build();
    }

    public CompletableFuture<Boolean> createSegment(final String scope,
                                                    final String stream,
                                                    final long segmentId,
                                                    final ScalingPolicy policy,
                                                    String controllerToken,
                                                    final long clientRequestId) {
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionFactory);
        final long requestId = connection.getFlow().asLong();
        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        return connection.sendRequest(requestId, new WireCommands.CreateSegment(requestId, qualifiedStreamSegmentName,
            extracted.getLeft(), extracted.getRight(), controllerToken))
            .thenApply(r -> transformReply(r, connection, qualifiedStreamSegmentName, WireCommands.CreateSegment.class, type));
    }

    public CompletableFuture<Boolean> truncateSegment(final String scope,
                                                      final String stream,
                                                      final long segmentId,
                                                      final long offset,
                                                      String delegationToken,
                                                      final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.TRUNCATE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionFactory);
        final long requestId = connection.getFlow().asLong();

        return connection.sendRequest(requestId, new WireCommands.TruncateSegment(requestId, qualifiedStreamSegmentName, offset, delegationToken))
                .thenApply(r -> transformReply(r, connection, qualifiedStreamSegmentName, WireCommands.TruncateSegment.class, type));
    }

    public CompletableFuture<Boolean> deleteSegment(final String scope,
                                                    final String stream,
                                                    final long segmentId,
                                                    String delegationToken,
                                                    final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionFactory);
        final long requestId = connection.getFlow().asLong();

        return connection.sendRequest(requestId, new WireCommands.DeleteSegment(requestId, qualifiedStreamSegmentName, delegationToken))
                .thenApply(r -> transformReply(r, connection, qualifiedStreamSegmentName, WireCommands.DeleteSegment.class, type));
    }

    /**
     * This method sends segment sealed message for the specified segment.
     *
     * @param scope               stream scope
     * @param stream              stream name
     * @param segmentId           number of segment to be sealed
     * @param delegationToken     the token to be presented to segmentstore.
     * @param clientRequestId     client-generated id for end-to-end tracing
     * @return void
     */
    public CompletableFuture<Boolean> sealSegment(final String scope,
                                                  final String stream,
                                                  final long segmentId,
                                                  String delegationToken,
                                                  final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        return sealSegment(qualifiedName, uri, delegationToken, clientRequestId);
    }

    private CompletableFuture<Boolean> sealSegment(final String qualifiedName,
                                                   final Controller.NodeUri uri,
                                                   final String delegationToken,
                                                   long clientRequestId) {
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionFactory);
        final long requestId = connection.getFlow().asLong();

        return connection.sendRequest(requestId, new WireCommands.SealSegment(requestId, qualifiedName, delegationToken))
                .thenApply(r -> transformReply(r, connection, qualifiedName, WireCommands.SealSegment.class, type));
    }

    public CompletableFuture<UUID> createTransaction(final String scope,
                                                     final String stream,
                                                     final long segmentId,
                                                     final UUID txId,
                                                     String delegationToken) {
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
                handleError(error, result, type);
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
                                                          String delegationToken) {
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
                handleError(error, result, type);
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
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                         final String stream,
                                                         final long segmentId,
                                                         final UUID txId,
                                                         String delegationToken) {
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
                handleError(error, result, type);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(idGenerator.get(), transactionName, delegationToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<Void> updatePolicy(String scope, String stream, ScalingPolicy policy, long segmentId,
                                                String delegationToken, long clientRequestId) {
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
                handleError(error, result, type);
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
                qualifiedName, extracted.getLeft(), extracted.getRight(), delegationToken);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    public CompletableFuture<WireCommands.StreamSegmentInfo> getSegmentInfo(String scope, String stream, long segmentId,
                                                                            String delegationToken) {
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
                handleError(error, result, type);
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
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    /**
     * This method sends a WireCommand to create a table segment.
     *
     * @param tableName           Qualified table name.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed normally, will indicate the table segment creation completed
     * successfully. If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Boolean> createTableSegment(final String tableName,
                                                         String delegationToken,
                                                         final long clientRequestId) {

        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.CREATE_TABLE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionFactory);
        final long requestId = connection.getFlow().asLong();

        return connection.sendRequest(requestId, new WireCommands.CreateTableSegment(requestId, tableName, delegationToken))
                .thenApply(rpl -> transformReply(rpl, connection, tableName, WireCommands.CreateTableSegment.class, type));
    }

    /**
     * This method sends a WireCommand to delete a table segment.
     *
     * @param tableName           Qualified table name.
     * @param mustBeEmpty         Flag to check if the table segment should be empty before deletion.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed normally, will indicate the table segment deletion completed
     * successfully. If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Boolean> deleteTableSegment(final String tableName,
                                                         final boolean mustBeEmpty,
                                                         String delegationToken,
                                                         final long clientRequestId) {
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.DELETE_TABLE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionFactory);
        final long requestId = connection.getFlow().asLong();

        return connection.sendRequest(requestId, new WireCommands.DeleteTableSegment(requestId, tableName, mustBeEmpty, delegationToken))
                .thenApply(rpl -> transformReply(rpl, connection, tableName, WireCommands.DeleteTableSegment.class, type));
    }

    /**
     * This method sends a WireCommand to update table entries.
     *
     * @param tableName           Qualified table name.
     * @param entries             List of {@link TableEntry}s to be updated.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed normally, will contain the current versions of each {@link TableEntry}
     * If the operation failed, the future will be failed with the causing exception. If the exception can be retried
     * then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<List<KeyVersion>> updateTableEntries(final String tableName,
                                                                  final List<TableEntry<byte[], byte[]>> entries,
                                                                  String delegationToken,
                                                                  final long clientRequestId) {
        final CompletableFuture<List<KeyVersion>> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "updateTableEntries {} Connection dropped", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "updateTableEntries {} wrong host", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "updateTableEntries {} NoSuchSegment", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {
                log.info(requestId, "updateTableEntries request for {} tableSegment completed.", tableName);
                result.complete(tableEntriesUpdated.getUpdatedVersions().stream().map(KeyVersionImpl::new).collect(Collectors.toList()));
            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
                log.warn(requestId, "updateTableEntries request for {} tableSegment failed with TableKeyDoesNotExist.", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist));
            }

            @Override
            public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
                log.warn(requestId, "updateTableEntries request for {} tableSegment failed with TableKeyBadVersion.", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyBadVersion));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "updateTableEntries {} failed", tableName, error);
                handleError(error, result, type);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                                       type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        List<ByteBuf> buffersToRelease = new ArrayList<>();
        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> wireCommandEntries = entries.stream().map(te -> {
            final WireCommands.TableKey key = convertToWireCommand(te.getKey());
            ByteBuf valueBuffer = wrappedBuffer(te.getValue());
            buffersToRelease.add(key.getData());
            buffersToRelease.add(valueBuffer);
            final WireCommands.TableValue value = new WireCommands.TableValue(valueBuffer);
            return new AbstractMap.SimpleImmutableEntry<>(key, value);
        }).collect(Collectors.toList());

        WireCommands.UpdateTableEntries request = new WireCommands.UpdateTableEntries(requestId, tableName, delegationToken,
                                                                                      new WireCommands.TableEntries(wireCommandEntries));
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result
                .whenComplete((r, e) -> release(buffersToRelease));
    }

    /**
     * This method sends a WireCommand to remove table keys.
     *
     * @param tableName           Qualified table name.
     * @param keys                List of {@link TableKey}s to be removed. Only if all the elements in the list has version as
     *                            {@link KeyVersion#NO_VERSION} then an unconditional update/removal is performed. Else an atomic conditional
     *                            update (removal) is performed.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that will complete normally when the provided keys are deleted.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be
     * retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Void> removeTableKeys(final String tableName,
                                                   final List<TableKey<byte[]>> keys,
                                                   String delegationToken,
                                                   final long clientRequestId) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "removeTableKeys {} Connection dropped", tableName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "removeTableKeys {} Wrong host", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "removeTableKeys {} NoSuchSegment", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {
                log.info(requestId, "removeTableKeys {} completed.", tableName);
                result.complete(null);
            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
                log.info(requestId, "removeTableKeys request for {} tableSegment failed with TableKeyDoesNotExist.", tableName);
                result.complete(null);
            }

            @Override
            public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
                log.warn(requestId, "removeTableKeys request for {} tableSegment failed with TableKeyBadVersion.", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyBadVersion));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "removeTableKeys {} failed", tableName, error);
                handleError(error, result, type);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                                       type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        List<ByteBuf> buffersToRelease = new ArrayList<>(keys.size());
        List<WireCommands.TableKey> keyList = keys.stream().map(x -> {
            WireCommands.TableKey key = convertToWireCommand(x);
            buffersToRelease.add(key.getData());
            return key;
        }).collect(Collectors.toList());

        WireCommands.RemoveTableKeys request = new WireCommands.RemoveTableKeys(requestId, tableName, delegationToken, keyList);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result
                .whenComplete((r, e) -> release(buffersToRelease));
    }

    /**
     * This method sends a WireCommand to read table entries.
     *
     * @param tableName           Qualified table name.
     * @param keys                List of {@link TableKey}s to be read. {@link TableKey#getVersion()} is not used
     *                            during this operation and the latest version is read.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed normally, will contain a list of {@link TableEntry} with
     * a value corresponding to the latest version. The version will be set to {@link KeyVersion#NOT_EXISTS} if the
     * key does not exist. If the operation failed, the future will be failed with the causing exception.
     */
    public CompletableFuture<List<TableEntry<byte[], byte[]>>> readTable(final String tableName,
                                                                         final List<TableKey<byte[]>> keys,
                                                                         String delegationToken,
                                                                         final long clientRequestId) {
        final CompletableFuture<List<TableEntry<byte[], byte[]>>> result = new CompletableFuture<>();
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.READ_TABLE;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;

        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "readTable {} Connection dropped", tableName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "readTable {} wrong host", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "readTable {} NoSuchSegment", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableRead(WireCommands.TableRead tableRead) {
                log.debug(requestId, "readTable {} successful.", tableName);
                List<TableEntry<byte[], byte[]>> tableEntries = tableRead.getEntries().getEntries().stream()
                                                                         .map(e -> new TableEntryImpl<>(convertFromWireCommand(e.getKey()), getArray(e.getValue().getData())))
                                                                         .collect(Collectors.toList());
                result.complete(tableEntries);
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "readTable {} failed", tableName, error);
                handleError(error, result, type);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                                       type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        List<ByteBuf> buffersToRelease = new ArrayList<>();
        // the version is always NO_VERSION as read returns the latest version of value.
        List<WireCommands.TableKey> keyList = keys.stream().map(k -> {
            ByteBuf buffer = wrappedBuffer(k.getKey());
            buffersToRelease.add(buffer);
            return new WireCommands.TableKey(buffer, WireCommands.TableKey.NO_VERSION);
        }).collect(Collectors.toList());

        WireCommands.ReadTable request = new WireCommands.ReadTable(requestId, tableName, delegationToken, keyList);
        sendRequestAsync(request, replyProcessor, result, ModelHelper.encode(uri));
        return result
                .whenComplete((r, e) -> release(buffersToRelease));
    }

    /**
     * The method sends a WireCommand to iterate over table keys.
     * @param tableName Qualified table name.
     * @param suggestedKeyCount Suggested number of {@link TableKey}s to be returned by the SegmentStore.
     * @param state Last known state of the iterator.
     * @param delegationToken The token to be presented to the segmentstore.
     * @param clientRequestId Request id.
     * @return A CompletableFuture that will return the next set of {@link TableKey}s returned from the SegmentStore.
     */
    public CompletableFuture<TableSegment.IteratorItem<TableKey<byte[]>>> readTableKeys(final String tableName,
                                                                                    final int suggestedKeyCount,
                                                                                    final IteratorState state,
                                                                                    final String delegationToken,
                                                                                    final long clientRequestId) {

        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.READ_TABLE_KEYS;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;
        final IteratorState token = (state == null) ? IteratorState.EMPTY : state;

        final CompletableFuture<TableSegment.IteratorItem<TableKey<byte[]>>> result = new CompletableFuture<>();
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "readTableKeys {} Connection dropped", tableName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "readTableKeys {} wrong host", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "readTableKeys {} NoSuchSegment", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {
                log.info(requestId, "readTableKeys {} successful.", tableName);
                final IteratorState state = IteratorState.fromBytes(tableKeysRead.getContinuationToken());
                final List<TableKey<byte[]>> keys =
                        tableKeysRead.getKeys().stream().map(k -> new TableKeyImpl<>(getArray(k.getData()),
                                                                                     new KeyVersionImpl(k.getKeyVersion()))).collect(Collectors.toList());
                result.complete(new TableSegment.IteratorItem<>(state, keys));
            }

            @Override
            public void processingFailure(Exception error) {
                log.error(requestId, "readTableKeys {} failed", tableName, error);
                handleError(error, result, type);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                                       type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.ReadTableKeys cmd = new WireCommands.ReadTableKeys(requestId, tableName, delegationToken, suggestedKeyCount,
                                                                        token.toBytes());
        sendRequestAsync(cmd, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }


    /**
     * The method sends a WireCommand to iterate over table entries.
     * @param tableName Qualified table name.
     * @param suggestedEntryCount Suggested number of {@link TableKey}s to be returned by the SegmentStore.
     * @param state Last known state of the iterator.
     * @param delegationToken The token to be presented to the segmentstore.
     * @param clientRequestId Request id.
     * @return A CompletableFuture that will return the next set of {@link TableKey}s returned from the SegmentStore.
     */
    public CompletableFuture<TableSegment.IteratorItem<TableEntry<byte[], byte[]>>> readTableEntries(final String tableName,
                                                                               final int suggestedEntryCount,
                                                                               final IteratorState state,
                                                                               final String delegationToken,
                                                                               final long clientRequestId) {

        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.READ_TABLE_ENTRIES;
        final long requestId = (clientRequestId == RequestTag.NON_EXISTENT_ID) ? idGenerator.get() : clientRequestId;
        final IteratorState token = (state == null) ? IteratorState.EMPTY : state;

        final CompletableFuture<TableSegment.IteratorItem<TableEntry<byte[], byte[]>>> result = new CompletableFuture<>();
        final FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                log.warn(requestId, "readTableEntries {} Connection dropped", tableName);
                result.completeExceptionally(
                        new WireCommandFailedException(type, WireCommandFailedException.Reason.ConnectionDropped));
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                log.warn(requestId, "readTableEntries {} wrong host", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost));
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                log.warn(requestId, "readTableEntries {} NoSuchSegment", tableName);
                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist));
            }

            @Override
            public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {
                log.debug(requestId, "readTableEntries {} successful.", tableName);
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
            public void processingFailure(Exception error) {
                log.error(requestId, "readTableEntries {} failed", tableName, error);
                handleError(error, result, type);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(
                        new WireCommandFailedException(new AuthenticationException(authTokenCheckFailed.toString()),
                                                       type, WireCommandFailedException.Reason.AuthFailed));
            }
        };

        WireCommands.ReadTableEntries cmd = new WireCommands.ReadTableEntries(requestId, tableName, delegationToken,
                                                                        suggestedEntryCount, token.toBytes());
        sendRequestAsync(cmd, replyProcessor, result, ModelHelper.encode(uri));
        return result;
    }

    private byte[] getArray(ByteBuf buf) {
        final byte[] bytes = new byte[buf.readableBytes()];
        final int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        release(Collections.singleton(buf));
        return bytes;
    }
    
    private void release(Collection<ByteBuf> buffers) {
        buffers.forEach(ReferenceCountUtil::safeRelease);
    }

    private <T> void handleError(Exception error, CompletableFuture<T> result, WireCommandType type) {
        if (Exceptions.unwrap(error) instanceof ConnectionFailedException) {
            result.completeExceptionally(new WireCommandFailedException(error, type, WireCommandFailedException.Reason.ConnectionFailed));
        } else {
            result.completeExceptionally(error);
        }
    }

    private WireCommands.TableKey convertToWireCommand(final TableKey<byte[]> k) {
        WireCommands.TableKey key;
        if (k.getVersion() == null || k.getVersion() == KeyVersion.NO_VERSION) {
            // unconditional update.
            key = new WireCommands.TableKey(wrappedBuffer(k.getKey()), WireCommands.TableKey.NO_VERSION);
        } else {
            key = new WireCommands.TableKey(wrappedBuffer(k.getKey()), k.getVersion().getSegmentVersion());
        }
        return key;
    }

    private TableKey<byte[]> convertFromWireCommand(WireCommands.TableKey k) {
        final TableKey<byte[]> key;
        if (k.getKeyVersion() == WireCommands.TableKey.NOT_EXISTS) {
            key = new TableKeyImpl<>(getArray(k.getData()), KeyVersion.NOT_EXISTS);
        } else {
            key = new TableKeyImpl<>(getArray(k.getData()), new KeyVersionImpl(k.getKeyVersion()));
        }
        return key;
    }

    /**
     * This method takes a new connection from the pool and associates replyProcessor with that connection and sends 
     * the supplied request over that connection. 
     * It takes a resultFuture that is completed when the response from the store is processed successfully. 
     * If there is a failure in establishing connection or sending the request over the wire, 
     * the resultFuture is completedExceptionally explicitly by this method. Otherwise, it simply registers a callback
     * on result future's completion to return the connection back to the pool. 
     * @param request request to send.
     * @param replyProcessor reply processor to associate with the connection.
     * @param resultFuture A future that when completed signals completion of request processing, either via 
     *                     recieving a response from segment store or a failure (to send the request/receive a response).  
     * @param uri segment store uri where the request needs to be sent.
     */
    private void sendRequestAsync(final WireCommand request, final ReplyProcessor replyProcessor,
                                            final CompletableFuture<?> resultFuture,
                                            final PravegaNodeUri uri) {
        try {
            // get connection for the segment store node from the connectionManager. 
            // take a new connection from the connection manager
            CompletableFuture<ConnectionWrapper> connectionFuture = connectionManager.getConnection(uri, replyProcessor);
            connectionFuture.whenComplete((connection, e) -> connectionCompleteCallback(request, resultFuture, connection, e));
            resultFuture.whenComplete((result, e) -> requestCompleteCallback(connectionFuture, e));
        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        }
    }

    /**
     * Connection completion callback method. This is invoked when the future returned by the connection pool completes.
     * If it succeeded, we will have a connection object where send the request. 
     * If it failed, the resultFuture is failed with ConnectionFailedException. 
     * @param request request to send over to segment store.
     * @param resultFuture Future to complete in case of a connection failure. This future is completed in the reply 
     *                     processor in successful case.  
     * @param connection Connection object received upon successful completion of future from the request for new 
     *                   connection from the pool. 
     * @param e Exception, if any, thrown from attempting to get a new connection.  
     */
    private  void connectionCompleteCallback(WireCommand request, CompletableFuture<?> resultFuture,
                                             ConnectionWrapper connection, Throwable e) {
        if (connection == null || e != null) {
            ConnectionFailedException cause = e != null ? new ConnectionFailedException(e) : new ConnectionFailedException();
            resultFuture.completeExceptionally(new WireCommandFailedException(cause,
                    request.getType(),
                    WireCommandFailedException.Reason.ConnectionFailed));
        } else {                
            connection.sendAsync(request, resultFuture);
        }
    }

    /**
     * Request Complete callback is invoked when the request is complete, either by sending and receiving a response from 
     * segment store or by way of failure of connection. 
     * This is responsible for returning the connection back to the connection pool. 
     * @param connectionFuture conection future that when completed successfully holds the connection object taken from the pool. 
     * @param e Exception, if any, thrown from the request processing. 
     */
    private void requestCompleteCallback(CompletableFuture<ConnectionWrapper> connectionFuture, 
                                         Throwable e) {
        // when processing completes, return the connection back to connection manager asynchronously.
        // Note: If result future is complete, connectionFuture is definitely complete. if connectionFuture had failed,
        // we would not have received a connection object anyway. 
        if (e != null) {
            Throwable unwrap = Exceptions.unwrap(e);
            if (hasConnectionFailed(unwrap)) {
                connectionFuture.thenAccept(connectionObject -> {
                    connectionObject.failConnection();
                    connectionObject.close();
                });
            } else {
                connectionFuture.thenAccept(ConnectionWrapper::close);
            }
        } else {
            connectionFuture.thenAccept(ConnectionWrapper::close);
        }
    }

    private boolean hasConnectionFailed(Throwable unwrap) {
        return unwrap instanceof WireCommandFailedException &&
                (((WireCommandFailedException) unwrap).getReason().equals(WireCommandFailedException.Reason.ConnectionFailed) ||
                        (((WireCommandFailedException) unwrap).getReason().equals(WireCommandFailedException.Reason.ConnectionDropped))
                );
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

    private void closeConnection(Reply reply, RawClient client) {
        log.info("Closing connection as a result of receiving: {}", reply);
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Exception tearing down connection: ", e);
            }
        }
    }

    /**
     * This method handle reply returned from RawClient.sendRequest.
     *
     * @param reply               actual reply received
     * @param client              RawClient for sending request
     * @param qualifiedStreamSegmentName StreamSegmentName
     * @param requestType         request which reply need to be transformed
     * @return true if reply is in the expected reply set for the given requestType or throw exception.
     */
    @SneakyThrows(ConnectionFailedException.class)
    private Boolean transformReply(Reply reply,
                                   RawClient client,
                                   String qualifiedStreamSegmentName,
                                   Class<? extends Request> requestType,
                                   WireCommandType type) {
        closeConnection(reply, client);
        Set<Class<? extends Reply>> expectedReplies = this.EXPECTED_REPLIES.get(requestType);
        if (expectedReplies != null && expectedReplies.contains(reply.getClass())) {
            log.info(reply.getRequestId(), "{} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName());
            return true;
        } else if (reply instanceof WireCommands.NoSuchSegment) {
            throw new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist);
        } else if (reply instanceof WireCommands.WrongHost) {
            throw new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost);
        } else if (reply instanceof WireCommands.TableSegmentNotEmpty) {
            throw new WireCommandFailedException(type, WireCommandFailedException.Reason.TableSegmentNotEmpty);
        } else {
            throw new ConnectionFailedException("Unexpected reply of " + reply + " when expecting one of "
                + expectedReplies.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }
    }

    @Override
    public void close() {
        connectionManager.close();
    }
}
