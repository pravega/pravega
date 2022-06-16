/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import io.pravega.client.tables.impl.HashTableIteratorItem;
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

import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
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
            .put(WireCommands.MergeSegmentsBatch.class, ImmutableSet.of(WireCommands.SegmentsBatchMerged.class))
            .put(WireCommands.ReadSegment.class, ImmutableSet.of(WireCommands.SegmentRead.class))
            .put(WireCommands.GetSegmentAttribute.class, ImmutableSet.of(WireCommands.SegmentAttribute.class))
            .put(WireCommands.UpdateSegmentAttribute.class, ImmutableSet.of(WireCommands.SegmentAttributeUpdated.class))
            .put(WireCommands.UpdateTableEntries.class, ImmutableSet.of(WireCommands.TableEntriesUpdated.class))
            .put(WireCommands.RemoveTableKeys.class, ImmutableSet.of(WireCommands.TableKeysRemoved.class,
                    WireCommands.TableKeyDoesNotExist.class))
            .put(WireCommands.ReadTable.class, ImmutableSet.of(WireCommands.TableRead.class))
            .put(WireCommands.ReadTableKeys.class, ImmutableSet.of(WireCommands.TableKeysRead.class))
            .put(WireCommands.ReadTableEntries.class, ImmutableSet.of(WireCommands.TableEntriesRead.class))
            .put(WireCommands.GetTableSegmentInfo.class, ImmutableSet.of(WireCommands.TableSegmentInfo.class))
            .build();

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_FAILING_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
            .put(WireCommands.GetStreamSegmentInfo.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.ReadSegment.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.GetSegmentAttribute.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.UpdateSegmentAttribute.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.UpdateTableEntries.class, ImmutableSet.of(WireCommands.TableKeyDoesNotExist.class,
                    WireCommands.TableKeyBadVersion.class, WireCommands.NoSuchSegment.class))
            .put(WireCommands.RemoveTableKeys.class, ImmutableSet.of(WireCommands.TableKeyBadVersion.class, WireCommands.NoSuchSegment.class))
            .put(WireCommands.DeleteTableSegment.class, ImmutableSet.of(WireCommands.TableSegmentNotEmpty.class))
            .put(WireCommands.ReadTable.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.ReadTableKeys.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.ReadTableEntries.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.GetTableSegmentInfo.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .put(WireCommands.MergeSegmentsBatch.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
            .build();

    protected final ConnectionPool connectionPool;
    protected final ScheduledExecutorService executorService;
    protected final AtomicReference<Duration> timeout;
    private final HostControllerStore hostStore;

    public SegmentHelper(final ConnectionPool connectionPool, HostControllerStore hostStore,
                         ScheduledExecutorService executorService) {
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
                                                 final String controllerToken,
                                                 final long clientRequestId,
                                                 final long rolloverSizeBytes) {
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();
        Pair<Byte, Integer> extracted = extractFromPolicy(policy);

        return sendRequest(connection, clientRequestId, new WireCommands.CreateSegment(requestId, qualifiedStreamSegmentName,
            extracted.getLeft(), extracted.getRight(), controllerToken, rolloverSizeBytes))
            .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedStreamSegmentName,
                    WireCommands.CreateSegment.class, type));
    }

    public CompletableFuture<Void> truncateSegment(final String scope,
                                                   final String stream,
                                                   final long segmentId,
                                                   final long offset,
                                                   final String delegationToken,
                                                   final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.TRUNCATE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        return sendRequest(connection, clientRequestId, new WireCommands.TruncateSegment(requestId,
                qualifiedStreamSegmentName, offset, delegationToken))
                .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedStreamSegmentName,
                        WireCommands.TruncateSegment.class, type));
    }

    public CompletableFuture<Void> deleteSegment(final String scope,
                                                 final String stream,
                                                 final long segmentId,
                                                 final String delegationToken,
                                                 final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedStreamSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        return sendRequest(connection, clientRequestId, new WireCommands.DeleteSegment(requestId,
                qualifiedStreamSegmentName, delegationToken))
                .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedStreamSegmentName,
                        WireCommands.DeleteSegment.class, type));
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
                                               final String delegationToken,
                                               final long clientRequestId) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.SEAL_SEGMENT;
        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        return sendRequest(connection, clientRequestId, new WireCommands.SealSegment(requestId, qualifiedName, delegationToken))
                .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedName, WireCommands.SealSegment.class, type));
    }

    public CompletableFuture<Void> createTransaction(final String scope,
                                                     final String stream,
                                                     final long segmentId,
                                                     final UUID txId,
                                                     final String delegationToken,
                                                     final long clientRequestId,
                                                     final long rolloverSizeBytes) {
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final String transactionName = getTransactionName(scope, stream, segmentId, txId);
        final WireCommandType type = WireCommandType.CREATE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.CreateSegment request = new WireCommands.CreateSegment(requestId, transactionName,
                WireCommands.CreateSegment.NO_SCALE, 0, delegationToken, rolloverSizeBytes);

        return sendRequest(connection, clientRequestId, request)
                .thenAccept(r -> handleReply(clientRequestId, r, connection, transactionName, WireCommands.CreateSegment.class,
                        type));
    }

    private String getTransactionName(String scope, String stream, long segmentId, UUID txId) {
        // Transaction segments are created against a logical primary such that all transaction segments become mergeable.
        // So we will erase secondary id while creating transaction's qualified name.
        long generalizedSegmentId = RecordHelper.generalizedSegmentId(segmentId, txId);

        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, generalizedSegmentId);
        return getTransactionNameFromId(qualifiedName, txId);
    }

    public CompletableFuture<List<Long>> mergeTxnSegments(final String scope,
                                                          final String stream,
                                                          final long targetSegmentId,
                                                          final long sourceSegmentId,
                                                          final List<UUID> txId,
                                                          final String delegationToken,
                                                          final long clientRequestId) {
        Preconditions.checkArgument(getSegmentNumber(targetSegmentId) == getSegmentNumber(sourceSegmentId));
        final Controller.NodeUri uri = getSegmentUri(scope, stream, sourceSegmentId);
        final String qualifiedNameTarget = getQualifiedStreamSegmentName(scope, stream, targetSegmentId);
        final List<String> transactionNames = txId.stream().map(x -> getTransactionName(scope, stream, sourceSegmentId, x)).collect(Collectors.toList());
        final WireCommandType type = WireCommandType.MERGE_SEGMENTS_BATCH;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();
        
        WireCommands.MergeSegmentsBatch request = new WireCommands.MergeSegmentsBatch(requestId, qualifiedNameTarget, transactionNames, delegationToken);

        return sendRequest(connection, clientRequestId, request)
                .thenApply(r -> {
                        handleReply(clientRequestId, r, connection, qualifiedNameTarget, WireCommands.MergeSegmentsBatch.class, type);
                        return ((WireCommands.SegmentsBatchMerged) r).getNewTargetWriteOffset();
                });
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope,
                                                         final String stream,
                                                         final long segmentId,
                                                         final UUID txId,
                                                         final String delegationToken,
                                                         final long clientRequestId) {
        final String transactionName = getTransactionName(scope, stream, segmentId, txId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        final WireCommandType type = WireCommandType.DELETE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();
        WireCommands.DeleteSegment request = new WireCommands.DeleteSegment(requestId, transactionName, delegationToken);

        return sendRequest(connection, clientRequestId, request)
                .thenAccept(r -> handleReply(clientRequestId, r, connection, transactionName,
                        WireCommands.DeleteSegment.class, type))
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

        return sendRequest(connection, clientRequestId, request)
                .thenAccept(r -> handleReply(clientRequestId, r, connection, qualifiedName,
                        WireCommands.UpdateSegmentPolicy.class, type));
    }

    public CompletableFuture<WireCommands.StreamSegmentInfo> getSegmentInfo(String scope, String stream, long segmentId,
                                                                            String delegationToken, long clientRequestId) {
        final String qualifiedName = getQualifiedStreamSegmentName(scope, stream, segmentId);
        final Controller.NodeUri uri = getSegmentUri(scope, stream, segmentId);
        return getSegmentInfo(qualifiedName, ModelHelper.encode(uri), delegationToken, clientRequestId);
    }

    public CompletableFuture<WireCommands.StreamSegmentInfo> getSegmentInfo(String qualifiedName, PravegaNodeUri uri,
                                                                            String delegationToken, long clientRequestId) {
        final WireCommandType type = WireCommandType.GET_STREAM_SEGMENT_INFO;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.GetStreamSegmentInfo request = new WireCommands.GetStreamSegmentInfo(requestId,
                qualifiedName, delegationToken);
        
        return sendRequest(connection, clientRequestId, request)
                .thenApply(r -> {
                    handleReply(clientRequestId, r, connection, qualifiedName, WireCommands.GetStreamSegmentInfo.class,
                            type);
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
     * @param keyLength           Key Length. If 0, a Hash Table Segment (Variable Key Length) will be created, otherwise
     *                            a Fixed-Key-Length Table Segment will be created with this value for the key length.
     * @param rolloverSizeBytes   The rollover size of segment in LTS.
     * @return A CompletableFuture that, when completed normally, will indicate the table segment creation completed
     * successfully. If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Void> createTableSegment(final String tableName,
                                                      String delegationToken,
                                                      final long clientRequestId,
                                                      final boolean sortedTableSegment,
                                                      final int keyLength,
                                                      final long rolloverSizeBytes) {

        final Controller.NodeUri uri = getTableUri(tableName);
        return createTableSegment(tableName, delegationToken, clientRequestId, sortedTableSegment, keyLength, rolloverSizeBytes, uri);
    }

    public CompletableFuture<Void> createTableSegment(final String tableName,
                                                      String delegationToken,
                                                      final long clientRequestId,
                                                      final boolean sortedTableSegment,
                                                      final int keyLength,
                                                      final long rolloverSizeBytes,
                                                      final Controller.NodeUri uri) {

        final WireCommandType type = WireCommandType.CREATE_TABLE_SEGMENT;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        // All Controller Metadata Segments are non-sorted.
        return sendRequest(connection, clientRequestId, new WireCommands.CreateTableSegment(requestId, tableName, sortedTableSegment, keyLength, delegationToken, rolloverSizeBytes))
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

        return sendRequest(connection, clientRequestId, new WireCommands.DeleteTableSegment(requestId,
                tableName, mustBeEmpty, delegationToken))
                .thenAccept(rpl -> handleReply(clientRequestId, rpl, connection, tableName,
                        WireCommands.DeleteTableSegment.class, type));
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
        return updateTableEntries(tableName, ModelHelper.encode(getTableUri(tableName)), entries, delegationToken, clientRequestId);
    }

    public CompletableFuture<List<TableSegmentKeyVersion>> updateTableEntries(final String tableName,
                                                                              final PravegaNodeUri uri,
                                                                              final List<TableSegmentEntry> entries,
                                                                              String delegationToken,
                                                                              final long clientRequestId) {
        final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
        List<Map.Entry<WireCommands.TableKey, WireCommands.TableValue>> wireCommandEntries = entries.stream().map(te -> {
            final WireCommands.TableKey key = convertToWireCommand(te.getKey());
            final WireCommands.TableValue value = new WireCommands.TableValue(te.getValue());
            return new AbstractMap.SimpleImmutableEntry<>(key, value);
        }).collect(Collectors.toList());

        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();
        WireCommands.UpdateTableEntries request = new WireCommands.UpdateTableEntries(requestId, tableName, delegationToken,
                new WireCommands.TableEntries(wireCommandEntries), WireCommands.NULL_TABLE_SEGMENT_OFFSET);

        return sendRequest(connection, clientRequestId, request)
                .thenApply(rpl -> {
                    handleReply(clientRequestId, rpl, connection, tableName, WireCommands.UpdateTableEntries.class, type);
                    return ((WireCommands.TableEntriesUpdated) rpl)
                            .getUpdatedVersions().stream()
                            .map(TableSegmentKeyVersion::from).collect(Collectors.toList());
                });
    }

    /**
     * This method sends a WireCommand to get information about a Table Segment.
     *
     * @param tableName           Qualified table name.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed successfully, will return information about the Table Segment.
     * If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<WireCommands.TableSegmentInfo> getTableSegmentInfo(final String tableName,
                                                      String delegationToken,
                                                      final long clientRequestId) {

        final Controller.NodeUri uri = getTableUri(tableName);
        final WireCommandType type = WireCommandType.GET_TABLE_SEGMENT_INFO;

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        // All Controller Metadata Segments are non-sorted.
        return sendRequest(connection, clientRequestId, new WireCommands.GetTableSegmentInfo(requestId, tableName, delegationToken))
                .thenApply(r -> {
                    handleReply(clientRequestId, r, connection, tableName, WireCommands.GetTableSegmentInfo.class,
                            type);
                    assert r instanceof WireCommands.TableSegmentInfo;
                    return (WireCommands.TableSegmentInfo) r;
                });
    }

    /**
     * This method gets the entry count for a Table Segment.
     *
     * @param tableName           Qualified table name.
     * @param delegationToken     The token to be presented to the segmentstore.
     * @param clientRequestId     Request id.
     * @return A CompletableFuture that, when completed successfully, will return entry count of a Table Segment.
     * If the operation failed, the future will be failed with the causing exception. If the exception
     * can be retried then the future will be failed with {@link WireCommandFailedException}.
     */
    public CompletableFuture<Long> getTableSegmentEntryCount(final String tableName,
                                                                                String delegationToken,
                                                                                final long clientRequestId) {
        return getTableSegmentInfo(tableName, delegationToken, clientRequestId)
                .thenApply(WireCommands.TableSegmentInfo::getEntryCount);
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
        return removeTableKeys(tableName, uri, keys, delegationToken, clientRequestId);
    }

    public CompletableFuture<Void> removeTableKeys(final String tableName,
                                                   Controller.NodeUri uri,
                                                   final List<TableSegmentKey> keys,
                                                   String delegationToken,
                                                   final long clientRequestId) {
        final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
        List<WireCommands.TableKey> keyList = keys.stream().map(x -> {
            WireCommands.TableKey key = convertToWireCommand(x);
            return key;
        }).collect(Collectors.toList());

        RawClient connection = new RawClient(ModelHelper.encode(uri), connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.RemoveTableKeys request = new WireCommands.RemoveTableKeys(
                requestId, tableName, delegationToken, keyList, WireCommands.NULL_TABLE_SEGMENT_OFFSET);

        return sendRequest(connection, clientRequestId, request)
                .thenAccept(rpl -> handleReply(clientRequestId, rpl, connection, tableName,
                        WireCommands.RemoveTableKeys.class, type));
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
        return readTable(tableName, ModelHelper.encode(getTableUri(tableName)), keys, delegationToken, clientRequestId);
    }

    public CompletableFuture<List<TableSegmentEntry>> readTable(final String tableName, final PravegaNodeUri uri,
                                                                final List<TableSegmentKey> keys, String delegationToken,
                                                                final long clientRequestId) {
        final WireCommandType type = WireCommandType.READ_TABLE;
        // the version is always NO_VERSION as read returns the latest version of value.
        List<WireCommands.TableKey> keyList = keys.stream().map(k ->
                new WireCommands.TableKey(k.getKey(), k.getVersion().getSegmentVersion())).collect(Collectors.toList());

        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.ReadTable request = new WireCommands.ReadTable(requestId, tableName, delegationToken, keyList);
        return sendRequest(connection, clientRequestId, request)
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
    public CompletableFuture<HashTableIteratorItem<TableSegmentKey>> readTableKeys(final String tableName,
                                                                                   final int suggestedKeyCount,
                                                                                   final HashTableIteratorItem.State state,
                                                                                   final String delegationToken,
                                                                                   final long clientRequestId) {
        return readTableKeys(tableName, ModelHelper.encode(getTableUri(tableName)), suggestedKeyCount, state,
                delegationToken, clientRequestId);
    }

    public CompletableFuture<HashTableIteratorItem<TableSegmentKey>> readTableKeys(final String tableName,
                                                                                   final PravegaNodeUri uri,
                                                                                   final int suggestedKeyCount,
                                                                                   final HashTableIteratorItem.State state,
                                                                                   final String delegationToken,
                                                                                   final long clientRequestId) {
        final WireCommandType type = WireCommandType.READ_TABLE_KEYS;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        final HashTableIteratorItem.State token = (state == null) ? HashTableIteratorItem.State.EMPTY : state;

        WireCommands.TableIteratorArgs args = new WireCommands.TableIteratorArgs(token.getToken(), Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
        WireCommands.ReadTableKeys request = new WireCommands.ReadTableKeys(requestId, tableName, delegationToken,
                suggestedKeyCount, args);
        return sendRequest(connection, clientRequestId, request)
                .thenApply(rpl -> {
                    handleReply(clientRequestId, rpl, connection, tableName, WireCommands.ReadTableKeys.class, type);
                    WireCommands.TableKeysRead tableKeysRead = (WireCommands.TableKeysRead) rpl;
                    final HashTableIteratorItem.State newState = HashTableIteratorItem.State.fromBytes(tableKeysRead.getContinuationToken());
                    final List<TableSegmentKey> keys =
                            tableKeysRead.getKeys().stream().map(k -> TableSegmentKey.versioned(k.getData(),
                                    k.getKeyVersion())).collect(Collectors.toList());
                    return new HashTableIteratorItem<>(newState, keys);
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
    public CompletableFuture<HashTableIteratorItem<TableSegmentEntry>> readTableEntries(final String tableName,
                                                                               final int suggestedEntryCount,
                                                                               final HashTableIteratorItem.State state,
                                                                               final String delegationToken,
                                                                               final long clientRequestId) {
        return readTableEntries(tableName, ModelHelper.encode(getTableUri(tableName)), suggestedEntryCount, state,
                delegationToken, clientRequestId);
    }

    public CompletableFuture<HashTableIteratorItem<TableSegmentEntry>> readTableEntries(final String tableName,
                                                                               final PravegaNodeUri uri,
                                                                               final int suggestedEntryCount,
                                                                               final HashTableIteratorItem.State state,
                                                                               final String delegationToken,
                                                                               final long clientRequestId) {

        final WireCommandType type = WireCommandType.READ_TABLE_ENTRIES;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        final HashTableIteratorItem.State token = (state == null) ? HashTableIteratorItem.State.EMPTY : state;

        WireCommands.TableIteratorArgs args = new WireCommands.TableIteratorArgs(token.getToken(), Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
        WireCommands.ReadTableEntries request = new WireCommands.ReadTableEntries(requestId, tableName, delegationToken,
                suggestedEntryCount, args);
        return sendRequest(connection, clientRequestId, request)
                .thenApply(rpl -> {
                    handleReply(clientRequestId, rpl, connection, tableName, WireCommands.ReadTableEntries.class, type);
                    WireCommands.TableEntriesRead tableEntriesRead = (WireCommands.TableEntriesRead) rpl;
                    final HashTableIteratorItem.State newState = HashTableIteratorItem.State.fromBytes(tableEntriesRead.getContinuationToken());
                    final List<TableSegmentEntry> entries =
                            tableEntriesRead.getEntries().getEntries().stream()
                                            .map(e -> {
                                                WireCommands.TableKey k = e.getKey();
                                                return TableSegmentEntry.versioned(k.getData(),
                                                        e.getValue().getData(),
                                                        k.getKeyVersion());
                                            }).collect(Collectors.toList());
                    return new HashTableIteratorItem<>(newState, entries);
                });
    }

    public CompletableFuture<WireCommands.SegmentRead> readSegment(String qualifiedName, long offset, int length,
                                                                        PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.READ_SEGMENT;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.ReadSegment request = new WireCommands.ReadSegment(qualifiedName, offset, length, delegationToken,
                requestId);

        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, qualifiedName, WireCommands.ReadSegment.class, type);
                    assert r instanceof WireCommands.SegmentRead;
                    return (WireCommands.SegmentRead) r;
                });
    }

    public CompletableFuture<WireCommands.SegmentAttribute> getSegmentAttribute(String qualifiedName, UUID attributeId,
                                                                   PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.GET_SEGMENT_ATTRIBUTE;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.GetSegmentAttribute request = new WireCommands.GetSegmentAttribute(requestId, qualifiedName, attributeId,
                delegationToken);

        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, qualifiedName, WireCommands.GetSegmentAttribute.class, type);
                    assert r instanceof WireCommands.SegmentAttribute;
                    return (WireCommands.SegmentAttribute) r;
                });
    }

    public CompletableFuture<WireCommands.SegmentAttributeUpdated> updateSegmentAttribute(
            String qualifiedName, UUID attributeId, long newValue, long existingValue, PravegaNodeUri uri,
            String delegationToken) {
        final WireCommandType type = WireCommandType.UPDATE_SEGMENT_ATTRIBUTE;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.UpdateSegmentAttribute request = new WireCommands.UpdateSegmentAttribute(
                requestId, qualifiedName, attributeId,
                newValue, existingValue, delegationToken);

        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, qualifiedName, WireCommands.UpdateSegmentAttribute.class, type);
                    assert r instanceof WireCommands.SegmentAttributeUpdated;
                    return (WireCommands.SegmentAttributeUpdated) r;
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

    private void closeConnection(Reply reply, RawClient client, long callerRequestId) {
        log.debug(callerRequestId, "Closing connection as a result of receiving: flowId: {}: reply: {}",
                reply.getRequestId(), reply);
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn(callerRequestId, "Exception tearing down connection: ", e);
            }
        }
    }

    protected <T extends Request & WireCommand> CompletableFuture<Reply> sendRequest(RawClient connection, long clientRequestId,
                                                                                   T request) {
        log.trace(clientRequestId, "Sending request to segment store with: flowId: {}: request: {}",
                request.getRequestId(), request);

        CompletableFuture<Reply> future = Futures.futureWithTimeout(() -> connection.sendRequest(request.getRequestId(), request),
                timeout.get(), "request", executorService);
        return future.exceptionally(e -> {
            processAndRethrowException(clientRequestId, request, e);
            return null;
        });
    }

    @VisibleForTesting
    <T extends Request & WireCommand> void processAndRethrowException(long callerRequestId, T request, Throwable e) {
        Throwable unwrap = Exceptions.unwrap(e);
        WireCommandFailedException ex = null;
        if (unwrap instanceof ConnectionFailedException || unwrap instanceof ConnectionClosedException) {
            log.warn(callerRequestId, "Connection dropped {}", request.getRequestId());
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
        } else if (unwrap instanceof AuthenticationException) {
            log.warn(callerRequestId, "Authentication Exception {}", request.getRequestId());
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.AuthFailed);
        } else if (unwrap instanceof TokenExpiredException) {
            log.warn(callerRequestId, "Token expired {}", request.getRequestId());
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.AuthFailed);
        } else if (unwrap instanceof TimeoutException) {
            log.warn(callerRequestId, "Request timed out. {}", request.getRequestId());
            throw new WireCommandFailedException(request.getType(), WireCommandFailedException.Reason.ConnectionFailed);
        } else {
            log.error(callerRequestId, "Request failed {}", request.getRequestId(), e);
            throw new CompletionException(e);
        }
    }

    /**
     * This method handle reply returned from RawClient.sendRequest.
     *
     * @param callerRequestId     request id issues by the client
     * @param reply               actual reply received
     * @param client              RawClient for sending request
     * @param qualifiedStreamSegmentName StreamSegmentName
     * @param requestType         request which reply need to be transformed
     * @param type                WireCommand for this request
     */
    @SneakyThrows(ConnectionFailedException.class)
    private void handleReply(long callerRequestId,
                             Reply reply,
                             RawClient client,
                             String qualifiedStreamSegmentName,
                             Class<? extends Request> requestType,
                             WireCommandType type) {
        handleExpectedReplies(callerRequestId, reply, client, qualifiedStreamSegmentName, requestType, type, EXPECTED_SUCCESS_REPLIES, EXPECTED_FAILING_REPLIES);
    }

    /**
     * This method handles the reply returned from RawClient.sendRequest given the expected success and failure cases.
     *
     * @param callerRequestId     request id issues by the client
     * @param reply               actual reply received
     * @param client              RawClient for sending request
     * @param qualifiedStreamSegmentName StreamSegmentName
     * @param requestType         request which reply need to be transformed
     * @param type                WireCommand for this request
     * @param expectedSuccessReplies the expected replies for a successful case
     * @param expectedFailureReplies the expected replies for a failing case
     * @throws ConnectionFailedException in case the reply is unexpected
     */
    protected void handleExpectedReplies(long callerRequestId,
                                         Reply reply,
                                         RawClient client,
                                         String qualifiedStreamSegmentName,
                                         Class<? extends Request> requestType,
                                         WireCommandType type,
                                         Map<Class<? extends Request>, Set<Class<? extends Reply>>> expectedSuccessReplies,
                                         Map<Class<? extends Request>, Set<Class<? extends Reply>>> expectedFailureReplies) throws ConnectionFailedException {
        closeConnection(reply, client, callerRequestId);
        Set<Class<? extends Reply>> expectedReplies = expectedSuccessReplies.get(requestType);
        Set<Class<? extends Reply>> expectedFailingReplies = expectedFailureReplies.get(requestType);
        if (expectedReplies != null && expectedReplies.contains(reply.getClass())) {
            log.debug(callerRequestId, "{} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
                    reply.getClass().getSimpleName(), reply.getRequestId());
        } else if (expectedFailingReplies != null && expectedFailingReplies.contains(reply.getClass())) {
            log.debug(callerRequestId, "{} {} {} {}.", requestType.getSimpleName(), qualifiedStreamSegmentName,
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
            log.error(callerRequestId, "Unexpected reply {} {} {} {}.", requestType.getSimpleName(),
                    qualifiedStreamSegmentName, reply.getClass().getSimpleName(), reply.getRequestId());

            throw new ConnectionFailedException("Unexpected reply of " + reply + " when expecting one of "
                + expectedReplies.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }
    }

    @Override
    public void close() {
        connectionPool.close();
    }
}
