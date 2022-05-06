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
package io.pravega.cli.admin.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.SneakyThrows;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Used by the Admin CLI for interacting with the admin-gateway on the Segment Store.
 */
public class AdminSegmentHelper extends SegmentHelper implements AutoCloseable {

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_SUCCESS_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
                    .put(WireCommands.FlushToStorage.class, ImmutableSet.of(WireCommands.StorageFlushed.class))
                    .put(WireCommands.GetTableSegmentInfo.class, ImmutableSet.of(WireCommands.TableSegmentInfo.class))
                    .put(WireCommands.ListStorageChunks.class, ImmutableSet.of(WireCommands.StorageChunksListed.class))
                    .put(WireCommands.CheckChunkSanity.class, ImmutableSet.of(WireCommands.ChunkSanityChecked.class))
                    .build();

    private static final Map<Class<? extends Request>, Set<Class<? extends Reply>>> EXPECTED_FAILING_REPLIES =
            ImmutableMap.<Class<? extends Request>, Set<Class<? extends Reply>>>builder()
                    .put(WireCommands.GetTableSegmentInfo.class, ImmutableSet.of(WireCommands.NoSuchSegment.class))
                    .build();

    public AdminSegmentHelper(final ConnectionPool connectionPool, HostControllerStore hostStore,
                              ScheduledExecutorService executorService) {
        super(connectionPool, hostStore, executorService);
    }

    /**
     * This method sends a WireCommand to flush the container corresponding to the given containerId to storage.
     *
     * @param containerId     The Id of the container that needs to be persisted to storage.
     * @param uri             The uri of the Segment Store instance.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that will complete normally when the provided keys are deleted.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be
     * retried then the future will be failed.
     */
    public CompletableFuture<WireCommands.StorageFlushed> flushToStorage(int containerId, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.FLUSH_TO_STORAGE;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.FlushToStorage request = new WireCommands.FlushToStorage(containerId, delegationToken, requestId);
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                   handleReply(requestId, r, connection, null, WireCommands.FlushToStorage.class, type);
                   assert r instanceof WireCommands.StorageFlushed;
                   return (WireCommands.StorageFlushed) r;
                });
    }

    /**
     * This method sends a WireCommand to check the sanity of the container corresponding to the given containerId.
     *
     * @param containerId The Id of the container for which the sanity needs to be checked.
     * @param chunkName Name of the chunk.
     * @param dataSize DataSize of the bytes to read.
     * @param uri The uri of the Segment Store instance.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that will complete normally by checking the sanity of the chunk.
     * If the operation failed, the future will be failed with the causing exception. If the exception can be
     * retried then the future will be failed.
     */
    public CompletableFuture<WireCommands.ChunkSanityChecked> checkChunkSanity(int containerId, String chunkName, int dataSize, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.CHECK_CHUNK_SANITY;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.CheckChunkSanity request = new WireCommands.CheckChunkSanity(containerId, chunkName, dataSize, delegationToken, requestId);
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, chunkName, WireCommands.CheckChunkSanity.class, type);
                    assert r instanceof WireCommands.ChunkSanityChecked;
                    return (WireCommands.ChunkSanityChecked) r;
                });
    }

    /**
     * Evicts all eligible entries from buffer cache and all entries from guava cache.
     *
     * @param containerId The Id of the container for which the meta data cache is evicted.
     * @param uri The uri of the Segment Store instance.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that will complete normally when all the eligible entries from buffer cache and guava cache are evicted.
     */
    public CompletableFuture<WireCommands.MetaDataCacheEvicted> evictMetaDataCache(int containerId, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.EVICT_METADATA_CACHE;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.EvictMetaDataCache request = new WireCommands.EvictMetaDataCache(containerId, delegationToken, requestId);
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, null, WireCommands.EvictMetaDataCache.class, type);
                    assert r instanceof WireCommands.MetaDataCacheEvicted;
                    return (WireCommands.MetaDataCacheEvicted) r;
                });
    }

    /**
     * Evicts entire read index cache.
     * @param containerId The Id of the container for which the read index cache is evicted.
     * @param uri The uri of the Segment Store instance.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that will complete normally when entire read index cache is evicted.
     */
    public CompletableFuture<WireCommands.ReadIndexCacheEvicted> evictReadIndexCache(int containerId, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.EVICT_READINDEX_CACHE;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.EvictReadIndexCache request = new WireCommands.EvictReadIndexCache(containerId, delegationToken, requestId);
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, null, WireCommands.EvictReadIndexCache.class, type);
                    assert r instanceof WireCommands.ReadIndexCacheEvicted;
                    return (WireCommands.ReadIndexCacheEvicted) r;
                });
    }

    /**
     * Evicts read index cache for given segment.
     * @param containerId The Id of the container for which the read index cache for given segment is evicted.
     * @param segmentName Name of the segment for which the read index cache is evicted.
     * @param uri The uri of the Segment Store instance.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that will complete normally when entire read index cache for given segment is evicted.
     */
    public CompletableFuture<WireCommands.ReadIndexCacheEvictedForSegment> evictReadIndexCacheForSegment(int containerId, String segmentName, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.EVICT_READINDEX_CACHE_SEGMENT;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.EvictReadIndexCacheForSegment request = new WireCommands.EvictReadIndexCacheForSegment(containerId, segmentName, delegationToken, requestId);
        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, null, WireCommands.EvictReadIndexCacheForSegment.class, type);
                    assert r instanceof WireCommands.ReadIndexCacheEvictedForSegment;
                    return (WireCommands.ReadIndexCacheEvictedForSegment) r;
                });
    }


    /**
     * This method sends a WireCommand to get table segment info for the given table segment name.
     *
     * @param qualifiedName   StreamSegmentName
     * @param uri             The uri of the Segment Store instance.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that will return the table segment info as a WireCommand.
     */
    public CompletableFuture<WireCommands.TableSegmentInfo> getTableSegmentInfo(String qualifiedName, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.GET_TABLE_SEGMENT_INFO;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.GetTableSegmentInfo request = new WireCommands.GetTableSegmentInfo(requestId,
                qualifiedName, delegationToken);

        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, qualifiedName, WireCommands.GetTableSegmentInfo.class, type);
                    assert r instanceof WireCommands.TableSegmentInfo;
                    return (WireCommands.TableSegmentInfo) r;
                });
    }

    /**
     * This methods sends a WireCommand to get the list of storage chunks under the given segment name.
     *
     * @param qualifiedName   StreamSegmentName
     * @param uri             The uri of the Segment Store instance.
     * @param delegationToken The token to be presented to the Segment Store.
     * @return A CompletableFuture that return the list of storage chunks as a WireCommand.
     */
    public CompletableFuture<WireCommands.StorageChunksListed> listStorageChunks(String qualifiedName, PravegaNodeUri uri, String delegationToken) {
        final WireCommandType type = WireCommandType.LIST_STORAGE_CHUNKS;
        RawClient connection = new RawClient(uri, connectionPool);
        final long requestId = connection.getFlow().asLong();

        WireCommands.ListStorageChunks request = new WireCommands.ListStorageChunks(qualifiedName, delegationToken, requestId);

        return sendRequest(connection, requestId, request)
                .thenApply(r -> {
                    handleReply(requestId, r, connection, qualifiedName, WireCommands.ListStorageChunks.class, type);
                    assert r instanceof WireCommands.StorageChunksListed;
                    return (WireCommands.StorageChunksListed) r;
                });
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
}
