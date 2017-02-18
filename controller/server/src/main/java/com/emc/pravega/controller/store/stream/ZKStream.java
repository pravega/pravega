/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.Cache;
import com.emc.pravega.controller.store.stream.tables.CompletedTxRecord;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.Data;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Utilities;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata store.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 * <p>
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
@Slf4j
class ZKStream extends PersistentStreamBase<Integer> {

    private static final String STREAM_PATH = "/streams/%s";
    private static final String CREATION_TIME_PATH = STREAM_PATH + "/creationTime";
    private static final String CONFIGURATION_PATH = STREAM_PATH + "/configuration";
    private static final String STATE_PATH = STREAM_PATH + "/state";
    private static final String SEGMENT_PATH = STREAM_PATH + "/segment";
    private static final String HISTORY_PATH = STREAM_PATH + "/history";
    private static final String INDEX_PATH = STREAM_PATH + "/index";

    private static final String TRANSACTION_ROOT_PATH = "/transactions";
    private static final String ACTIVE_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/activeTx";
    private static final String ACTIVE_TX_PATH = ACTIVE_TX_ROOT_PATH + "/%s";
    private static final String COMPLETED_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/completedTx";
    private static final String COMPLETED_TX_PATH = COMPLETED_TX_ROOT_PATH + "/%s";
    private static final String MARKER_PATH = STREAM_PATH + "/markers";
    private static final String BLOCKER_PATH = STREAM_PATH + "/blocker";

    private static final long BLOCK_VALIDITY_PERIOD = Duration.ofSeconds(10).toMillis();
    private static Executor executor;

    private static CuratorFramework client;

    private final String creationPath;
    private final String configurationPath;
    private final String statePath;
    private final String segmentPath;
    private final String segmentChunkPathTemplate;
    private final String historyPath;
    private final String indexPath;
    private final String activeTxPath;
    private final String completedTxPath;
    private final String markerPath;
    private final String blockerPath;

    private final Cache<Integer> cache;

    ZKStream(final String scope, final String name) {
        super(scope, name);

        creationPath = String.format(CREATION_TIME_PATH, name);
        configurationPath = String.format(CONFIGURATION_PATH, name);
        statePath = String.format(STATE_PATH, name);
        segmentPath = String.format(SEGMENT_PATH, name);
        segmentChunkPathTemplate = segmentPath + "/%s";
        historyPath = String.format(HISTORY_PATH, name);
        indexPath = String.format(INDEX_PATH, name);
        activeTxPath = String.format(ACTIVE_TX_PATH, name);
        completedTxPath = String.format(COMPLETED_TX_PATH, name);
        markerPath = String.format(MARKER_PATH, name);
        blockerPath = String.format(BLOCKER_PATH, name);
        cache = new Cache<>(ZKStream::getData);
    }

    // region overrides

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        return getChildren(activeTxPath).thenApply(list -> {
            return list == null ? 0 : list.size();
        });
    }

    @Override
    public void refresh() {
        cache.invalidateAll();
    }

    @Override
    public CompletableFuture<Void> checkStreamExists(final Create create) throws StreamAlreadyExistsException {
        return checkExists(creationPath)
                .thenCompose(x -> {
                    if (x) {
                        return cache.getCachedData(creationPath)
                                .thenApply(creationTime -> Utilities.toLong(creationTime.getData()) != create.getEventTime());
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
                })
                .thenAccept(x -> {
                    if (x) {
                        throw new StreamAlreadyExistsException(getName());
                    }
                });
    }

    @Override
    CompletableFuture<Void> storeCreationTime(final Create create) {
        return createZNodeIfNotExist(creationPath, Utilities.toByteArray(create.getEventTime()))
                .thenAccept(x -> cache.invalidateCache(creationPath));
    }

    @Override
    public CompletableFuture<Void> createConfiguration(final Create create) {
        return createZNodeIfNotExist(configurationPath, SerializationUtils.serialize(create.getConfiguration()))
                .thenApply(x -> cache.invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<Void> createState(final State state) {
        return createZNodeIfNotExist(statePath, SerializationUtils.serialize(state))
                .thenApply(x -> cache.invalidateCache(statePath));
    }

    @Override
    public CompletableFuture<Void> createSegmentTable(final Create create) {
        return createZNodeIfNotExist(segmentPath).thenApply(x -> cache.invalidateCache(segmentPath));
    }

    @Override
    CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final Data<Integer> data) {
        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkNumber);
        return createZNodeIfNotExist(segmentChunkPath, data.getData())
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<Void> createIndexTable(final Create create) {
        final byte[] indexTable = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
        return createZNodeIfNotExist(indexPath, indexTable)
                .thenApply(x -> cache.invalidateCache(indexPath));
    }

    @Override
    public CompletableFuture<Void> createHistoryTable(final Create create) {
        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
        final byte[] historyTable = TableHelper.updateHistoryTable(new byte[0],
                create.getEventTime(),
                IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

        return createZNodeIfNotExist(historyPath, historyTable)
                .thenApply(x -> cache.invalidateCache(historyPath));
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final Data<Integer> updated) {
        return setData(historyPath, updated)
                .thenApply(x -> cache.invalidateCache(historyPath));
    }

    @Override
    public CompletableFuture<Void> createSegmentFile(final Create create) {
        final int numSegments = create.getConfiguration().getScalingPolicy().getMinNumSegments();
        final int chunkFileName = 0;
        final double keyRangeChunk = 1.0 / numSegments;

        final int startingSegmentNumber = 0;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());
        final int toCreate = newRanges.size();

        final byte[] segmentTable = TableHelper.updateSegmentTable(startingSegmentNumber,
                new byte[0],
                toCreate,
                newRanges,
                create.getEventTime()
        );

        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkFileName);
        return createZNodeIfNotExist(segmentChunkPath, segmentTable)
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<Void> createMarkerData(int segmentNumber, long timestamp) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));

        return createZNodeIfNotExist(path, Utilities.toByteArray(timestamp))
                .thenAccept(x -> cache.invalidateCache(markerPath));
    }

    @Override
    CompletableFuture<Void> updateMarkerData(int segmentNumber, Data<Integer> data) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));

        return setData(path, data)
                .thenAccept(x -> cache.invalidateCache(path));
    }

    @Override
    CompletableFuture<Optional<Data<Integer>>> getMarkerData(int segmentNumber) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));
        return cache.getCachedData(path)
                .handle((res, ex) -> {
                    if (ex != null) {
                        Throwable cause = ExceptionHelpers.getRealException(ex);
                        if (cause instanceof DataNotFoundException) {
                            return Optional.empty();
                        }
                        RetryableException.throwRetryableOrElseRuntime(ex);
                        return Optional.empty();
                    } else {
                        return Optional.of(res);
                    }
                });
    }

    @Override
    CompletableFuture<Void> removeMarkerData(int segmentNumber) {
        final String path = ZKPaths.makePath(markerPath, String.format("%d", segmentNumber));

        return deletePath(path, false)
                .thenAccept(x -> cache.invalidateCache(path));
    }

    @Override
    CompletableFuture<Void> setBlockFlag() {
        return checkExists(blockerPath)
                .thenCompose(x -> {
                    if (x) {
                        return cache.getCachedData(blockerPath);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).thenCompose(x -> {
                    if (x == null) {
                        return createZNodeIfNotExist(blockerPath, Utilities.toByteArray(System.currentTimeMillis()));
                    } else if (System.currentTimeMillis() - Utilities.toLong(x.getData()) > BLOCK_VALIDITY_PERIOD) {
                        return setData(blockerPath, new Data<>(Utilities.toByteArray(System.currentTimeMillis()), x.getVersion()));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenAccept(x -> cache.invalidateCache(blockerPath));
    }

    @Override
    CompletableFuture<Void> unsetBlockFlag() {
        return deletePath(blockerPath, false)
                .thenAccept(x -> cache.invalidateCache(blockerPath));
    }

    @Override
    CompletableFuture<Boolean> isBlocked() {

        return checkExists(blockerPath)
                .thenCompose(x -> {
                    if (x) {
                        return cache.getCachedData(blockerPath);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }).thenApply(x ->
                        !(x == null || System.currentTimeMillis() - Utilities.toLong(x.getData()) > BLOCK_VALIDITY_PERIOD));
    }

    @Override
    public CompletableFuture<Map<String, Data<Integer>>> getCurrentTxns() {
        return getChildren(activeTxPath)
                .thenCompose(txIds -> FutureHelpers.allOfWithResults(txIds.stream().collect(
                        Collectors.toMap(txId -> txId, txId -> cache.getCachedData(ZKPaths.makePath(activeTxPath, txId)))
                )));
    }

    @Override
    CompletableFuture<Void> createNewTransaction(final UUID txId, final long timestamp) {
        final String activePath = getActiveTxPath(txId.toString());
        return createZNodeIfNotExist(activePath,
                new ActiveTxRecord(timestamp, TxnStatus.OPEN).toByteArray())
                .thenApply(x -> cache.invalidateCache(activePath));
    }

    @Override
    CompletableFuture<Data<Integer>> getActiveTx(final UUID txId) {
        final String activeTxPath = getActiveTxPath(txId.toString());

        return cache.getCachedData(activeTxPath);
    }

    @Override
    CompletableFuture<Void> sealActiveTx(final UUID txId) {
        final String activePath = getActiveTxPath(txId.toString());

        return getActiveTx(txId)
                .thenCompose(x -> {
                    ActiveTxRecord previous = ActiveTxRecord.parse(x.getData());
                    ActiveTxRecord updated = new ActiveTxRecord(previous.getTxCreationTimestamp(), TxnStatus.SEALED);
                    return setData(activePath, new Data<>(updated.toByteArray(), x.getVersion()));
                })
                .thenApply(x -> cache.invalidateCache(activePath));
    }

    @Override
    CompletableFuture<Data<Integer>> getCompletedTx(final UUID txId) {
        return cache.getCachedData(getCompletedTxPath(txId.toString()));
    }

    @Override
    CompletableFuture<Void> removeActiveTxEntry(final UUID txId) {
        final String activePath = getActiveTxPath(txId.toString());
        return checkExists(activePath)
                .thenCompose(x -> {
                    if (x) {
                        return deletePath(activePath, true)
                                .thenAccept(y -> cache.invalidateCache(activePath));
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    CompletableFuture<Void> createCompletedTxEntry(final UUID txId, final TxnStatus complete, final long timestamp) {
        final String completedTxPath = getCompletedTxPath(txId.toString());
        return createZNodeIfNotExist(completedTxPath,
                new CompletedTxRecord(timestamp, complete).toByteArray())
                .thenAccept(x -> cache.invalidateCache(completedTxPath));
    }

    @Override
    public CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration) {
        return setData(configurationPath, new Data<>(SerializationUtils.serialize(configuration), null))
                .thenAccept(x -> cache.invalidateCache(configurationPath));
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfigurationData() {
        return cache.getCachedData(configurationPath)
                .thenApply(x -> (StreamConfiguration) SerializationUtils.deserialize(x.getData()));
    }

    @Override
    CompletableFuture<Void> setStateData(final State state) {
        return setData(statePath, new Data<>(SerializationUtils.serialize(state), null))
                .thenApply(x -> cache.invalidateCache(statePath));
    }

    @Override
    CompletableFuture<State> getStateData() {
        return cache.getCachedData(statePath)
                .thenApply(x -> (State) SerializationUtils.deserialize(x.getData()));
    }

    @Override
    public CompletableFuture<Segment> getSegmentRow(final int number) {
        // compute the file name based on segment number
        final int chunkNumber = number / SegmentRecord.SEGMENT_CHUNK_SIZE;

        return getSegmentTableChunk(chunkNumber)
                .thenApply(x -> TableHelper.getSegment(number, x.getData()));
    }

    @Override
    public CompletableFuture<Data<Integer>> getSegmentTableChunk(final int chunkNumber) {
        return cache.getCachedData(String.format(segmentChunkPathTemplate, chunkNumber));
    }

    @Override
    CompletableFuture<Void> setSegmentTableChunk(final int chunkNumber, final Data<Integer> data) {
        final String segmentChunkPath = String.format(segmentChunkPathTemplate, chunkNumber);
        return setData(segmentChunkPath, data)
                .thenApply(x -> cache.invalidateCache(segmentChunkPath));
    }

    @Override
    public CompletableFuture<List<String>> getSegmentChunks() {
        return getChildren(segmentPath);
    }

    @Override
    public CompletableFuture<Data<Integer>> getHistoryTable() {
        return cache.getCachedData(historyPath);
    }

    @Override
    public CompletableFuture<Data<Integer>> getIndexTable() {
        return cache.getCachedData(indexPath);
    }

    @Override
    CompletableFuture<Void> updateIndexTable(final Data<Integer> updated) {
        return setData(indexPath, updated).thenApply(x -> cache.invalidateCache(indexPath));
    }

    // endregion

    // region private helpers
    private String getActiveTxPath(final String txId) {
        return ZKPaths.makePath(activeTxPath, txId);
    }

    private String getCompletedTxPath(final String txId) {
        return ZKPaths.makePath(completedTxPath, txId);
    }

    public static void initialize(final CuratorFramework cf, final Executor executor) {
        ZKStream.executor = executor;
        ZKStream.client = cf;
    }

    // region curator client store access

    private static CompletableFuture<Void> deletePath(final String path, final boolean deleteEmptyContainer) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final CompletableFuture<Void> deleteNode = new CompletableFuture<>();

        try {
            client.delete().inBackground(
                    callback(event -> deleteNode.complete(null),
                            e -> {
                                if (e instanceof DataNotFoundException) { // deleted already
                                    deleteNode.complete(null);
                                } else {
                                    deleteNode.completeExceptionally(e);
                                }
                            }), executor).forPath(path);
        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
            deleteNode.completeExceptionally(new StoreConnectionException(e));
        } catch (Exception e) {
            deleteNode.completeExceptionally(new RuntimeException(e));
        }

        deleteNode.whenComplete((res, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
            } else if (deleteEmptyContainer) {
                final String container = ZKPaths.getPathAndNode(path).getPath();
                try {
                    client.delete().inBackground(
                            callback(event -> result.complete(null),
                                    e -> {
                                        if (e instanceof DataNotFoundException) { // deleted already
                                            result.complete(null);
                                        } else if (e instanceof DataExistsException) { // non empty dir
                                            result.complete(null);
                                        } else {
                                            result.completeExceptionally(e);
                                        }
                                    }), executor).forPath(container);
                } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
                    result.completeExceptionally(new StoreConnectionException(e));
                } catch (Exception e) {
                    result.completeExceptionally(new RuntimeException(e));
                }
            } else {
                result.complete(null);
            }
        });

        return result;
    }

    private static CompletableFuture<Data<Integer>> getData(final String path) throws DataNotFoundException, RetryableException {
        final CompletableFuture<Data<Integer>> result = new CompletableFuture<>();

        checkExists(path)
                .whenComplete((exists, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else if (exists) {
                        try {
                            client.getData().inBackground(
                                    callback(event -> result.complete(new Data<>(event.getData(), event.getStat().getVersion())),
                                            result::completeExceptionally), executor).forPath(path);
                        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
                            result.completeExceptionally(new StoreConnectionException(e));
                        } catch (Exception e) {
                            result.completeExceptionally(new RuntimeException(e));
                        }
                    } else {
                        result.completeExceptionally(new DataNotFoundException(path));
                    }
                });

        return result;
    }

    private static CompletableFuture<List<String>> getChildren(final String path) {
        final CompletableFuture<List<String>> result = new CompletableFuture<>();

        try {
            client.getChildren().inBackground(
                    callback(event -> result.complete(event.getChildren()),
                            e -> {
                                if (e instanceof DataNotFoundException) {
                                    result.complete(Collections.emptyList());
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }), executor).forPath(path);
        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
            result.completeExceptionally(new StoreConnectionException(e));
        } catch (Exception e) {
            result.completeExceptionally(new RuntimeException(e));
        }

        return result;
    }

    private static CompletableFuture<Void> setData(final String path, final Data<Integer> data) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        checkExists(path)
                .whenComplete((exists, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    } else if (exists) {
                        try {
                            if (data.getVersion() == null) {
                                client.setData().inBackground(
                                        callback(event -> result.complete(null), result::completeExceptionally), executor)
                                        .forPath(path, data.getData());
                            } else {
                                client.setData().withVersion(data.getVersion()).inBackground(
                                        callback(event -> result.complete(null), result::completeExceptionally), executor)
                                        .forPath(path, data.getData());
                            }
                        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
                            result.completeExceptionally(new StoreConnectionException(e));
                        } catch (Exception e) {
                            result.completeExceptionally(new RuntimeException(e));
                        }
                    } else {
                        log.error("Failed to write data. path {}", path);
                        result.completeExceptionally(new DataNotFoundException(path));
                    }
                });

        return result;
    }

    private static CompletableFuture<Void> createZNodeIfNotExist(final String path, final byte[] data) {
        final CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            client.create().creatingParentsIfNeeded().inBackground(
                    callback(x -> result.complete(null),
                            e -> {
                                if (e instanceof DataExistsException) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }), executor).forPath(path, data);
        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
            result.completeExceptionally(new StoreConnectionException(e));
        } catch (Exception e) {
            result.completeExceptionally(new RuntimeException(e));
        }

        return result;
    }

    private static CompletableFuture<Void> createZNodeIfNotExist(final String path) {
        final CompletableFuture<Void> result = new CompletableFuture<>();

        try {
            client.create().creatingParentsIfNeeded().inBackground(
                    callback(x -> result.complete(null),
                            e -> {
                                if (e instanceof DataExistsException) {
                                    result.complete(null);
                                } else {
                                    result.completeExceptionally(e);
                                }
                            }), executor).forPath(path);
        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
            result.completeExceptionally(new StoreConnectionException(e));
        } catch (Exception e) {
            result.completeExceptionally(new RuntimeException(e));
        }

        return result;
    }

    private static CompletableFuture<Boolean> checkExists(final String path) {
        final CompletableFuture<Boolean> result = new CompletableFuture<>();

        try {
            client.checkExists().inBackground(
                    callback(x -> result.complete(x.getStat() != null),
                            ex -> {
                                if (ex instanceof DataNotFoundException) {
                                    result.complete(false);
                                } else {
                                    result.completeExceptionally(ex);
                                }
                            }), executor).forPath(path);

        } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException | KeeperException.OperationTimeoutException e) {
            result.completeExceptionally(new StoreConnectionException(e));
        } catch (Exception e) {
            result.completeExceptionally(new RuntimeException(e));
        }

        return result;
    }

    private static BackgroundCallback callback(Consumer<CuratorEvent> result, Consumer<Throwable> exception) {
        return (client, event) -> {
            if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                result.accept(event);
            } else if (event.getResultCode() == KeeperException.Code.CONNECTIONLOSS.intValue() ||
                    event.getResultCode() == KeeperException.Code.SESSIONEXPIRED.intValue() ||
                    event.getResultCode() == KeeperException.Code.SESSIONMOVED.intValue() ||
                    event.getResultCode() == KeeperException.Code.OPERATIONTIMEOUT.intValue()) {
                exception.accept(new StoreConnectionException("" + event.getResultCode()));
            } else if (event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue()) {
                exception.accept(new DataExistsException(event.getPath()));
            } else if (event.getResultCode() == KeeperException.Code.BADVERSION.intValue()) {
                exception.accept(new WriteConflictException(event.getPath()));
            } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                exception.accept(new DataNotFoundException(event.getPath()));
            } else if (event.getResultCode() == KeeperException.Code.NOTEMPTY.intValue()) {
                exception.accept(new DataExistsException(event.getPath()));
            } else {
                exception.accept(new RuntimeException("Curator background task errorCode:" + event.getResultCode()));
            }
        };
    }

    // endregion
}
