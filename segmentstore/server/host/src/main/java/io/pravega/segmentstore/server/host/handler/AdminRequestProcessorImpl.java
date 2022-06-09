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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.common.LoggerHelpers;
import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.shared.protocol.netty.AdminRequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Request processor for admin commands issues from Admin CLI.
 *
 * This class is a gateway to allow administrators performing arbitrary operations against Segments for debug and/or
 * recovery purposes. Note that enabling this gateway will allow an administrator to perform destructive operations
 * on data, so it should be used carefully. By default, the Pravega request processor is disabled.
 */
@Slf4j
public class AdminRequestProcessorImpl extends PravegaRequestProcessor implements AdminRequestProcessor {

    //region Constructor

    /**
     * Creates a new instance of the AdminRequestProcessor class with no Metrics StatsRecorder.
     *
     * @param segmentStore The StreamSegmentStore to attach to (and issue requests to).
     * @param tableStore The TableStore to attach to (and issue requests to).
     * @param connection   The ServerConnection to attach to (and send responses to).
     */
    public AdminRequestProcessorImpl(@NonNull StreamSegmentStore segmentStore, @NonNull TableStore tableStore,
                                     @NonNull ServerConnection connection) {
        this(segmentStore, tableStore, new TrackedConnection(connection, new ConnectionTracker()), new PassingTokenVerifier());
    }

    /**
     * Creates a new instance of the AdminRequestProcessor class with no Metrics StatsRecorder.
     *
     * @param segmentStore The StreamSegmentStore to attach to (and issue requests to).
     * @param tableStore The TableStore to attach to (and issue requests to).
     * @param connection   The ServerConnection to attach to (and send responses to).
     * @param tokenVerifier  Verifier class that verifies delegation token.
     */
    public AdminRequestProcessorImpl(@NonNull StreamSegmentStore segmentStore, @NonNull TableStore tableStore,
                                     @NonNull TrackedConnection connection, @NonNull DelegationTokenVerifier tokenVerifier) {
        this(segmentStore, tableStore, connection, SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(),
                tokenVerifier, true);
    }

    /**
     * Creates a new instance of the AdminRequestProcessor class.
     *
     * @param segmentStore  The StreamSegmentStore to attach to (and issue requests to).
     * @param tableStore    The TableStore to attach to (and issue requests to).
     * @param connection    The ServerConnection to attach to (and send responses to).
     * @param statsRecorder A StatsRecorder for Metrics for Stream Segments.
     * @param tableStatsRecorder A TableSegmentStatsRecorder for Metrics for Table Segments.
     * @param tokenVerifier  Verifier class that verifies delegation token.
     * @param replyWithStackTraceOnError Whether client replies upon failed requests contain server-side stack traces or not.
     */
    public AdminRequestProcessorImpl(@NonNull StreamSegmentStore segmentStore, @NonNull TableStore tableStore, @NonNull TrackedConnection connection,
                                     @NonNull SegmentStatsRecorder statsRecorder, @NonNull TableSegmentStatsRecorder tableStatsRecorder,
                                     @NonNull DelegationTokenVerifier tokenVerifier, boolean replyWithStackTraceOnError) {
        super(segmentStore, tableStore, connection, statsRecorder, tableStatsRecorder, tokenVerifier, replyWithStackTraceOnError);
    }

    //endregion

    //region RequestProcessor Implementation

    @Override
    public void hello(WireCommands.Hello hello) {
        log.info("Received hello from connection: {}", getConnection());
        getConnection().send(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
        if (hello.getLowVersion() > WireCommands.WIRE_VERSION || hello.getHighVersion() < WireCommands.OLDEST_COMPATIBLE_VERSION) {
            log.warn("Incompatible wire protocol versions {} from connection {}", hello, getConnection());
            getConnection().close();
        }
    }

    @Override
    public void keepAlive(WireCommands.KeepAlive keepAlive) {
        log.info("Received a keepAlive from connection: {}", getConnection());
        getConnection().send(keepAlive);
    }

    @Override
    public void flushToStorage(WireCommands.FlushToStorage flushToStorage) {
        final String operation = "flushToStorage";
        final int containerId = flushToStorage.getContainerId();

        if (!verifyToken(null, flushToStorage.getRequestId(), flushToStorage.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, flushToStorage);
        getSegmentStore().flushToStorage(containerId, TIMEOUT)
                .thenAccept(v -> {
                    LoggerHelpers.traceLeave(log, operation, trace);
                    getConnection().send(new WireCommands.StorageFlushed(flushToStorage.getRequestId()));
                })
                .exceptionally(ex -> handleException(flushToStorage.getRequestId(), null, operation, ex));
    }

    @Override
    public void listStorageChunks(WireCommands.ListStorageChunks listStorageChunks) {
        final String operation = "ListStorageChunks";
        final String segment = listStorageChunks.getSegment();

        if (!verifyToken(segment, listStorageChunks.getRequestId(), listStorageChunks.getDelegationToken(), operation)) {
            return;
        }

        List<WireCommands.ChunkInfo> result = new ArrayList<>();
        long trace = LoggerHelpers.traceEnter(log, operation, listStorageChunks);
        getSegmentStore().getExtendedChunkInfo(segment, TIMEOUT)
                .thenAccept(chunks -> {
                    LoggerHelpers.traceLeave(log, operation, trace);
                    for (ExtendedChunkInfo chunk : chunks) {
                        result.add(convertToChunkInfo(chunk));
                    }
                    getConnection().send(new WireCommands.StorageChunksListed(listStorageChunks.getRequestId(), result));
                })
                .exceptionally(ex -> handleException(listStorageChunks.getRequestId(), segment, operation, ex));
    }

    @Override
    public void checkChunkSanity(WireCommands.CheckChunkSanity checkChunkSanity) {
        final String operation = "CheckChunkSanity";
        final String chunkName = checkChunkSanity.getChunkName();
        final int containerId = checkChunkSanity.getContainerId();
        final int dataSize = checkChunkSanity.getDataSize();

        if (!verifyToken(chunkName, checkChunkSanity.getRequestId(), checkChunkSanity.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, checkChunkSanity);
        getSegmentStore().checkChunkStorageSanity(containerId, chunkName, dataSize, TIMEOUT)
                .thenAccept(v -> {
                    LoggerHelpers.traceLeave(log, operation, trace);
                    getConnection().send(new WireCommands.ChunkSanityChecked(checkChunkSanity.getRequestId()));
                }).exceptionally(ex -> handleException(checkChunkSanity.getRequestId(), chunkName, operation, ex));
    }

    @Override
    public void evictMetaDataCache(WireCommands.EvictMetaDataCache evictMetaDataCache) {
        final String operation = "EvictMetaDataCache";
        final int containerId = evictMetaDataCache.getContainerId();

        if (!verifyToken(null, evictMetaDataCache.getRequestId(), evictMetaDataCache.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, evictMetaDataCache);
        getSegmentStore().evictMetaDataCache(containerId, TIMEOUT)
                .thenAccept(v -> {
                    LoggerHelpers.traceLeave(log, operation, trace);
                    getConnection().send(new WireCommands.MetaDataCacheEvicted(evictMetaDataCache.getRequestId()));
                }).exceptionally(ex -> handleException(evictMetaDataCache.getRequestId(), null, operation, ex));
    }

    @Override
    public void evictReadIndexCache(WireCommands.EvictReadIndexCache evictReadIndexCache) {
        final String operation = "EvictReadIndexCache";
        final int containerId = evictReadIndexCache.getContainerId();

        if (!verifyToken(null, evictReadIndexCache.getRequestId(), evictReadIndexCache.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, evictReadIndexCache);
        getSegmentStore().evictReadIndexCache(containerId, TIMEOUT)
                .thenAccept(v -> {
                    LoggerHelpers.traceLeave(log, operation, trace);
                    getConnection().send(new WireCommands.ReadIndexCacheEvicted(evictReadIndexCache.getRequestId()));
                }).exceptionally(ex -> handleException(evictReadIndexCache.getRequestId(), null, operation, ex));
    }

    @Override
    public void evictReadIndexCacheForSegment(WireCommands.EvictReadIndexCacheForSegment evictReadIndexCacheForSegment) {
        final String operation = "EvictReadIndexCacheForSegment";
        final int containerId = evictReadIndexCacheForSegment.getContainerId();
        final String segmentName = evictReadIndexCacheForSegment.getSegmentName();

        if (!verifyToken(segmentName, evictReadIndexCacheForSegment.getRequestId(), evictReadIndexCacheForSegment.getDelegationToken(), operation)) {
            return;
        }

        long trace = LoggerHelpers.traceEnter(log, operation, evictReadIndexCacheForSegment);
        getSegmentStore().evictReadIndexCacheForSegment(containerId, segmentName, TIMEOUT)
                .thenAccept(v -> {
                    LoggerHelpers.traceLeave(log, operation, trace);
                    getConnection().send(new WireCommands.ReadIndexCacheEvictedForSegment(evictReadIndexCacheForSegment.getRequestId()));
                }).exceptionally(ex -> handleException(evictReadIndexCacheForSegment.getRequestId(), segmentName, operation, ex));
    }

    //endregion

    private WireCommands.ChunkInfo convertToChunkInfo(ExtendedChunkInfo extendedChunkInfo) {
        return new WireCommands.ChunkInfo(extendedChunkInfo.getLengthInMetadata(),
                extendedChunkInfo.getLengthInStorage(), extendedChunkInfo.getStartOffset(),
                extendedChunkInfo.getChunkName(), extendedChunkInfo.isExistsInStorage());
    }
}
