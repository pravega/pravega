/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.Unpooled;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.controller.util.Config;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import static io.pravega.shared.NameUtils.getQualifiedStreamSegmentName;
import static io.pravega.shared.NameUtils.getSegmentNumber;
import static io.pravega.shared.NameUtils.getTransactionNameFromId;

/**
 * Used by the Controller for interacting with Segment Store. Think of this class as a 'SegmentStoreHelper'. 
 */
public class SegmentHelper implements AutoCloseable {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(SegmentHelper.class));

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_SUCCESS_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
            .put(WireCommands.CreateSegment.class, ImmutableSet.of(WireCommands.SegmentCreated.class,
                    WireCommands.SegmentAlreadyExists.class))
            .put(WireCommands.CreateTableSegment.class, ImmutableSet.of(WireCommands.SegmentCreated.class,
                    WireCommands.SegmentAlreadyExists.class))
            .put(WireCommands.DeleteSegment.class, ImmutableSet.of(WireCommands.SegmentDeleted.class,
                    WireCommands.NoSuchSegment.class))
            .put(WireCommands.DeleteTableSegment.class, ImmutableSet.of(WireCommands.SegmentDeleted.class,
                    WireCommands.NoSuchSegment.class))
            .put(WireCommands.UpdateSegmentPolicy.class, ImmutableSet.of(WireCommands.SegmentPolicyUpdated.class))
            .put(WireCommands.SealSegment.class, ImmutableSet.of(WireCommands.SegmentSealed.class,
                    WireCommands.SegmentIsSealed.class))
            .put(WireCommands.TruncateSegment.class, ImmutableSet.of(WireCommands.SegmentTruncated.class,
                    WireCommands.SegmentIsTruncated.class))
            .put(WireCommands.GetStreamSegmentInfo.class, ImmutableSet.of(WireCommands.StreamSegmentInfo.class,
                    WireCommands.SegmentIsTruncated.class))
            .put(WireCommands.MergeSegments.class, ImmutableSet.of(WireCommands.SegmentsMerged.class,
                    WireCommands.NoSuchSegment.class))
            .put(WireCommands.UpdateTableEntries.class, ImmutableSet.of(WireCommands.TableEntriesUpdated.class))
            .put(WireCommands.RemoveTableKeys.class, ImmutableSet.of(WireCommands.TableKeysRemoved.class,
                    WireCommands.TableKeyDoesNotExist.class))
            .put(WireCommands.ReadTable.class, ImmutableSet.of(WireCommands.TableRead.class))
            .put(WireCommands.ReadTableKeys.class, ImmutableSet.of(WireCommands.TableKeysRead.class))
            .put(WireCommands.ReadTableEntries.class, ImmutableSet.of(WireCommands.TableEntriesRead.class))
            .build();

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_FAILING_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
            .put(WireCommands.UpdateTableEntries.class, ImmutableSet.of(WireCommands.TableKeyDoesNotExist.class, 
                    WireCommands.TableKeyBadVersion.class, WireCommands.NoSuchSegment.class))
            .put(WireCommands.RemoveTableKeys.class, ImmutableSet.of(WireCommands.TableKeyBadVersion.class, WireCommands.NoSuchSegment.class))
            .put(WireCommands.DeleteTableSegment.class, ImmutableSet.of(WireCommands.TableSegmentNotEmpty.class))
            .put(WireCommands.ReadTable.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.ReadTableKeys.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.ReadTableEntries.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .build();

    private final HostControllerStore hostStore;
    private final ConnectionPool connectionPool;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<Duration> timeout;

    public SegmentHelper(final ConnectionPool connectionPool, HostControllerStore hostStore, ScheduledExecutorService executorService) {
        this.connectionPool = connectionPool;
        this.hostStore = hostStore;
        this.executorService = executorService;
        this.timeout = new AtomicReference<>(Duration.ofSeconds(Config.REQUEST_TIMEOUT_SECONDS_SEGMENT_STORE));
    }

    @VisibleForTesting
    void setTimeout(Duration duration) {
        timeout.set(duration);    
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

    public CompletableFuture<Void> createSegment(final String scope,
                                                    final String stream,
                                                    final long segmentId,
                                                    final ScalingPolicy policy,
                                                    String controllerToken,
                                                    final long clientRequestId) {
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();
        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        return sendRequest(connection, requestId, new WireCommands.CreateSegment(requestId, qualifiedStreamSegmentName,
            extracted.getLeft(), extracted.getRight(), controllerToken))
            .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedStreamSegmentName, WireCommands.CreateSegment.class, type));
    }

    public CompletableFuture<Void> truncateSegment(final String scope,
                                                      final String stream,
                                                      final long segmentId,
                                                      final long offset,
                                                      String delegationToken,
                                                      final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.TRUNCATE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        return sendRequest(connection, requestId, new WireCommands.TruncateSegment(requestId, qualifiedStreamSegmentName, offset, delegationToken))
                .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedStreamSegmentName, WireCommands.TruncateSegment.class, type));
    }

    public CompletableFuture<Void> deleteSegment(final String scope,
                                                    final String stream,
                                                    final long segmentId,
                                                    String delegationToken,
                                                    final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        return sendRequest(connection, requestId, new WireCommands.DeleteSegment(requestId, qualifiedStreamSegmentName, delegationToken))
                .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedStreamSegmentName, WireCommands.DeleteSegment.class, type));
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
    public CompletableFuture<Void> sealSegment(final String scope,
                                                  final String stream,
                                                  final long segmentId,
                                                  String delegationToken,
                                                  final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        return sendRequest(connection, requestId, new WireCommands.SealSegment(requestId, qualifiedName, delegationToken))
                .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedName, WireCommands.SealSegment.class, type));
    }

    public CompletableFuture<Void> createTransaction(final String scope,
                                                     final String stream,
                                                     final long segmentId,
                                                     final UUID txId,
                                                     String delegationToken) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String transactionName = getTransactionName(scope, stream, segmentId, txId);
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.CreateSegment request = new WireCommands.CreateSegment(requestId, transactionName,
                WireCommands.CreateSegment.NO_SCALE, 0, delegationToken);

        return sendRequest(connection, requestId, request)
                .thenAccept(r -> handleReply(requestId, r, connection, transactionName, WireCommands.CreateSegment.class, type));
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
        final WireCommandType type = WireCommandType.MERGE_SEGMENTS;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();
        
        WireCommands.MergeSegments request = new WireCommands.MergeSegments(requestId,
                qualifiedNameTarget, transactionName, delegationToken);

        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, transactionName, WireCommands.MergeSegments.class, type);
                    if (r instanceof WireCommands.NoSuchSegment) {
                        WireCommands.NoSuchSegment reply = (WireCommands.NoSuchSegment) r;
                        if (reply.getSegment().equals(transactionName)) {
                            return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                        } else {
                            log.error(requestId, "Commit Transaction: Source segment {} not found.", reply.getSegment());
                            return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                        }
                    } else {
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });

    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                         final String stream,
                                                         final long segmentId,
                                                         final UUID txId,
                                                         String delegationToken) {
        final String transactionName = getTransactionName(scope, stream, segmentId, txId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();
        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(requestId, transactionName, delegationToken);

        return sendRequest(connection, requestId, request)
                .thenAccept(r -> handleReply(requestId, r, connection, transactionName, WireCommands.DeleteSegment.class, type))
                .thenApply(v -> TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build());
    }

    public CompletableFuture<Void> updatePolicy(String scope, String stream, ScalingPolicy policy, long segmentId,
                                                String delegationToken, long clientRequestId) {
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_POLICY;
        
        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.UpdateSegmentPolicy request = new WireCommands.UpdateSegmentPolicy(requestId,
                qualifiedName, extracted.getLeft(), extracted.getRight(), delegationToken);

        return sendRequest(connection, requestId, request)
                .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedName, WireCommands.UpdateSegmentPolicy.class, type));
    }

    public CompletableFuture<WireCommands.StreamSegmentInfo> getSegmentInfo(String scope, String stream, long segmentId,
                                                                            String delegationToken) {
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);

        final WireCommandType type = WireCommandType.GET_STREAM_SEGMENT_INFO;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.GetStreamSegmentInfo request = new WireCommands.GetStreamSegmentInfo(requestId,
                qualifiedName, delegationToken);
        
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, qualifiedName, WireCommands.GetStreamSegmentInfo.class, type);
                    assert r instanceof WireCommands.StreamSegmentInfo;
                    return (WireCommands.StreamSegmentInfo) r;
                });
    }

    /**
     * This method sends a WireCommand to create a table segment.
     *
     * @param tableName           Qualified table name.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @param clientRequestId     Request id.
     * @param sortedTableSegment  Boolean flag indicating if the Table Segment should be created in sorted order.
     * @return A CompletableFuture that, when completed normally, will indicate the table segment creation completed
     * successfully. If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Void> createTableSegment(final String tableName,
                                                         String delegationToken,
                                                         final long clientRequestId, final boolean sortedTableSegment) {

        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.CREATE_TABLE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        // All Controller Metadata Segments are non-sorted.
        return sendRequest(connection, requestId, new WireCommands.CreateTableSegment(requestId, tableName, sortedTableSegment, delegationToken))
                .thenAccept(rpl -> handleReply(clientRequestId, rpl, connection, tableName, WireCommands.CreateTableSegment.class, type));
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
    public CompletableFuture<Void> deleteTableSegment(final String tableName,
                                                         final boolean mustBeEmpty,
                                                         String delegationToken,
                                                         final long clientRequestId) {
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.DELETE_TABLE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        return sendRequest(connection, requestId, new WireCommands.DeleteTableSegment(requestId, tableName, mustBeEmpty, delegationToken))
                .thenAccept(rpl -> handleReply(clientRequestId, rpl, connection, tableName, WireCommands.DeleteTableSegment.class, type));
    }

    /**
     * This method sends a WireCommand to update table entries.
     *
     * @param tableName       Qualified table name.
     * @param entries         List of {@link TableSegmentEntry} instances to be updated.
     * @param delegationToken The token to be presented to the Segment Store.
     * @param clientRequestId Request id.
     * @return A CompletableFuture that, when completed normally, will contain the current versions of each
     * {@link TableSegmentEntry}.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be retried
     * then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<List<TableSegmentKeyVersion>> updateTableEntries(final String tableName,
                                                                              final List<TableSegmentEntry> entries,
                                                                              String delegationToken,
                                                                              final long clientRequestId) {
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> wireCommandEntries = entries.stream().map(te -> {
            final WireCommands.TableKey key = convertToWireCommand(te.getKey());
            final WireCommands.TableValue value = new WireCommands.TableValue(te.getValue());
            return new AbstractMap.SimpleImmutableEntry<>(key, value);
        }).collect(Collectors.toList());

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();
        WireCommands.UpdateTableEntries request = new WireCommands.UpdateTableEntries(requestId, tableName, delegationToken,
                new WireCommands.TableEntries(wireCommandEntries), WireCommands.NULL_TABLE_SEGMENT_OFFSET);

        return sendRequest(connection, requestId, request)
                .thenApply(rpl -> {
                    handleReply(clientRequestId, rpl, connection, tableName, WireCommands.UpdateTableEntries.class, type);
                    return ((WireCommands.TableEntriesUpdated) rpl)
                            .getUpdatedVersions().stream()
                            .map(TableSegmentKeyVersion::from).collect(Collectors.toList());
                });
    }

    /**
     * This method sends a WireCommand to remove table keys.
     *
     * @param tableName       Qualified table name.
     * @param keys            List of {@link TableSegmentKey}s to be removed. Only if all the elements in the list has version
     *                        as {@link TableSegmentKeyVersion#NO_VERSION} then an unconditional update/removal is performed.
     *                        Else an atomic conditional update (removal) is performed.
     * @param delegationToken The token to be presented to the Segment Store.
     * @param clientRequestId Request id.
     * @return A CompletableFuture that will complete normally when the provided keys are deleted.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be
     * retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Void> removeTableKeys(final String tableName,
                                                   final List<TableSegmentKey> keys,
                                                   String delegationToken,
                                                   final long clientRequestId) {
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
        List<WireCommands.TableKey> keyList = keys.stream().map(x -> {
            WireCommands.TableKey key = convertToWireCommand(x);
            return key;
        }).collect(Collectors.toList());

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.RemoveTableKeys request = new WireCommands.RemoveTableKeys(
                requestId, tableName, delegationToken, keyList, WireCommands.NULL_TABLE_SEGMENT_OFFSET);

        return sendRequest(connection, requestId, request)
                .thenAccept(rpl -> handleReply(clientRequestId, rpl, connection, tableName, WireCommands.RemoveTableKeys.class, type));
    }

    /**
     * This method sends a WireCommand to read table entries.
     *
     * @param tableName       Qualified table name.
     * @param keys            List of {@link TableSegmentKey}s to be read. {@link TableSegmentKey#getVersion()} is
     *                        not used during this operation and the latest version is read.
     * @param delegationToken The token to be presented to the Segment Store.
     * @param clientRequestId Request id.
     * @return A CompletableFuture that, when completed normally, will contain a list of {@link TableSegmentEntry} with
     * a value corresponding to the latest version. The version will be set to {@link TableSegmentKeyVersion#NOT_EXISTS}
     * if the key does not exist. If the operation failed, the future will be failed with the causing exception.
     */
    public CompletableFuture<List<TableSegmentEntry>> readTable(final String tableName,
                                                                final List<TableSegmentKey> keys,
                                                                String delegationToken,
                                                                final long clientRequestId) {
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.READ_TABLE;
        // the version is always NO_VERSION as read returns the latest version of value.
        List<WireCommands.TableKey> keyList = keys.stream().map(k -> {
            return new WireCommands.TableKey(k.getKey(), k.getVersion().getSegmentVersion());
        }).collect(Collectors.toList());

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.ReadTable request = new WireCommands.ReadTable(requestId, tableName, delegationToken, keyList);
        return sendRequest(connection, requestId, request)
                .thenApply(rpl -> {
                    handleReply(clientRequestId, rpl, connection, tableName, WireCommands.ReadTable.class, type);
                    return ((WireCommands.TableRead) rpl)
                            .getEntries().getEntries().stream()
                            .map(this::convertFromWireCommand)
                            .collect(Collectors.toList());
                });
    }

    /**
     * The method sends a WireCommand to iterate over table keys.
     *
     * @param tableName         Qualified table name.
     * @param suggestedKeyCount Suggested number of {@link TableSegmentKey}s to be returned by the SegmentStore.
     * @param state             Last known state of the iterator.
     * @param delegationToken   The token to be presented to the Segment Store.
     * @param clientRequestId   Request id.

     * @return A CompletableFuture that will return the next set of {@link TableSegmentKey}s returned from the SegmentStore.
     */
    public CompletableFuture<IteratorItem<TableSegmentKey>> readTableKeys(final String tableName,
                                                                          final int suggestedKeyCount,
                                                                          final IteratorStateImpl state,
                                                                          final String delegationToken,
                                                                          final long clientRequestId) {
        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.READ_TABLE_KEYS;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        final IteratorStateImpl token = (state == null) ? IteratorStateImpl.EMPTY : state;

        WireCommands.ReadTableKeys request = new WireCommands.ReadTableKeys(requestId, tableName, delegationToken, suggestedKeyCount,
                token.getToken(), Unpooled.EMPTY_BUFFER);
        return sendRequest(connection, requestId, request)
                .thenApply(rpl -> {
                    handleReply(clientRequestId, rpl, connection, tableName, WireCommands.ReadTableKeys.class, type);
                    WireCommands.TableKeysRead tableKeysRead = (WireCommands.TableKeysRead) rpl;
                    final IteratorState newState = IteratorStateImpl.fromBytes(tableKeysRead.getContinuationToken());
                    final List<TableSegmentKey> keys =
                            tableKeysRead.getKeys().stream().map(k -> TableSegmentKey.versioned(k.getData(),
                                    k.getKeyVersion())).collect(Collectors.toList());
                    return new IteratorItem<>(newState, keys);
                });
    }

    /**
     * The method sends a WireCommand to iterate over table entries.
     *
     * @param tableName           Qualified table name.
     * @param suggestedEntryCount Suggested number of {@link TableSegmentEntry} instances to be returned by the Segment Store.
     * @param state               Last known state of the iterator.
     * @param delegationToken     The token to be presented to the Segment Store.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that will return the next set of {@link TableSegmentEntry} instances returned from the
     * SegmentStore.
     */
    public CompletableFuture<IteratorItem<TableSegmentEntry>> readTableEntries(final String tableName,
                                                                               final int suggestedEntryCount,
                                                                               final IteratorStateImpl state,
                                                                               final String delegationToken,
                                                                               final long clientRequestId) {

        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.READ_TABLE_ENTRIES;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        final IteratorStateImpl token = (state == null) ? IteratorStateImpl.EMPTY : state;

        WireCommands.ReadTableEntries request = new WireCommands.ReadTableEntries(requestId, tableName, delegationToken,
                suggestedEntryCount, token.getToken(), Unpooled.EMPTY_BUFFER);
        return sendRequest(connection, requestId, request)
                .thenApply(rpl -> {
                    handleReply(clientRequestId, rpl, connection, tableName, WireCommands.ReadTableEntries.class, type);
                    WireCommands.TableEntriesRead tableEntriesRead = (WireCommands.TableEntriesRead) rpl;
                    final IteratorState newState = IteratorStateImpl.fromBytes(tableEntriesRead.getContinuationToken());
                    final List<TableSegmentEntry> entries =
                            tableEntriesRead.getEntries().getEntries().stream()
                                            .map(e -> {
                                                WireCommands.TableKey k = e.getKey();
                                                return TableSegmentEntry.versioned(k.getData(),
                                                        e.getValue().getData(),
                                                        k.getKeyVersion());
                                            }).collect(Collectors.toList());
                    return new IteratorItem<>(newState, entries);
                });
    }

    private WireCommands.TableKey convertToWireCommand(final TableSegmentKey k) {
        WireCommands.TableKey key;
        if (k.getVersion() == null) {
            // unconditional update.
            key = new WireCommands.TableKey(k.getKey(), WireCommands.TableKey.NO_VERSION);
        } else {
            key = new WireCommands.TableKey(k.getKey(), k.getVersion().getSegmentVersion());
        }
        return key;
    }

    private TableSegmentEntry convertFromWireCommand(Map.Entry<WireCommands.TableKey, WireCommands.TableValue> e) {
        if (e.getKey().getKeyVersion() == WireCommands.TableKey.NOT_EXISTS) {
            return TableSegmentEntry.notExists(e.getKey().getData(), e.getValue().getData());
        } else {
            return TableSegmentEntry.versioned(e.getKey().getData(), e.getValue().getData(), e.getKey().getKeyVersion());
        }
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

    private <T extends Request & WireCommand> CompletableFuture<Reply> sendRequest(RawClient connection, long requestId, T request) {
        CompletableFuture<Reply> future = Futures.futureWithTimeout(() -> connection.sendRequest(requestId, request), timeout.get(), "request", executorService);
        return future.exceptionally(e -> {
            processAndRethrowException(requestId, request, e);
            return null;
        });
    }

    @VisibleForTesting
    <T extends Request & WireCommand> void processAndRethrowException(long requestId, T request, Throwable e) {
        Throwable unwrap = Exceptions.unwrap(e);
        if (unwrap instanceof ConnectionFailedException || unwrap instanceof ConnectionClosedException) {
            log.warn(requestId, "Connection dropped");
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
        } else if (unwrap instanceof AuthenticationException) {
            log.warn(requestId, "Authentication Exception");
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.AuthFailed);
        } else if (unwrap instanceof TokenExpiredException) {
            log.warn(requestId, "Token expired");
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.AuthFailed);
        } else if (unwrap instanceof TimeoutException) {
            log.warn(requestId, "Request timed out.");
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
        } else {
            log.error(requestId, "Request failed", e);
            throw new CompletionException(e);
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
    private void handleReply(long callerRequestId, 
                             Reply reply,
                             RawClient client,
                             String qualifiedStreamSegmentName,
                             Class<? extends Request> requestType,
                             WireCommandType type) {
        closeConnection(reply, client);
        Set<Class<? extends Reply>> expectedReplies = EXPECTED_SUCCESS_REPLIES.get(requestType);
        Set<Class<? extends Reply>> expectedFailingReplies = EXPECTED_FAILING_REPLIES.get(requestType);
        if (expectedReplies != null && expectedReplies.contains(reply.getClass())) {
            log.info(callerRequestId, "{} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
        } else if (expectedFailingReplies != null && expectedFailingReplies.contains(reply.getClass())) {
            log.info(callerRequestId, "{} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
            if (reply instanceof WireCommands.NoSuchSegment) {
                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.SegmentDoesNotExist);
            } else if (reply instanceof WireCommands.TableSegmentNotEmpty) {
                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.TableSegmentNotEmpty);
            } else if (reply instanceof WireCommands.TableKeyDoesNotExist) {
                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist);
            } else if (reply instanceof WireCommands.TableKeyBadVersion) {
                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyBadVersion);
            }
        } else if (reply instanceof WireCommands.AuthTokenCheckFailed) {
            log.warn(callerRequestId, "Auth Check Failed {} {} {} {} with error code {}.",
                    requestType.getSimpleName(), qualifiedStreamSegmentName, reply.getClass().getSimpleName(),
                    reply.getRequestId(), ((WireCommands.AuthTokenCheckFailed) reply).getErrorCode());
            throw new WireCommandFailedException(new AuthenticationException(reply.toString()),
                    type, WireCommandFailedException.Reason.AuthFailed);
        } else if (reply instanceof WireCommands.WrongHost) {
            log.warn(callerRequestId, "Wrong Host {} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
            throw new WireCommandFailedException(type, WireCommandFailedException.Reason.UnknownHost);
        } else {
            log.error(callerRequestId, "Unexpected reply {} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());

            throw new ConnectionFailedException("Unexpected reply of " + reply + " when expecting one of "
                + expectedReplies.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }
    }

    @Override
    public void close() {
        connectionPool.close();
    }
}
