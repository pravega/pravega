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
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.ZKScope;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.util.Config;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * ZK stream metadata store.
 */
public class ZKStreamMetadataStore extends AbstractStreamMetadataStore implements AutoCloseable {
    /**
     * This constant defines the size of the block of counter values that will be used by this controller instance.
     * The controller will try to get current counter value from zookeeper. It then tries to update the value in store
     * by incrementing it by COUNTER_RANGE. If it is able to update the new value successfully, then this controller
     * can safely use the block `previous-value-in-store + 1` to `previous-value-in-store + COUNTER_RANGE` No other controller
     * will use this range for transaction id generation as it will be unique assigned to current controller.
     * If controller crashes, all unused values go to waste. In worst case we may lose COUNTER_RANGE worth of values everytime
     * a controller crashes.
     * Since we use a 96 bit number for our counter, so
     */
    @VisibleForTesting
    static final String SCOPE_ROOT_PATH = "/store";
    static final String SCOPE_DELETE_PATH = "_system/deletingScopes";
    static final String DELETED_STREAMS_PATH = "/lastActiveStreamSegment/%s";
    private static final String TRANSACTION_ROOT_PATH = "/transactions";
    private static final String COMPLETED_TXN_GC_NAME = "completedTxnGC";
    static final String ACTIVE_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/activeTx";
    static final String COMPLETED_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/completedTx";
    static final String COMPLETED_TX_BATCH_ROOT_PATH = COMPLETED_TX_ROOT_PATH + "/batches";
    static final String COMPLETED_TX_BATCH_PATH = COMPLETED_TX_BATCH_ROOT_PATH + "/%d";

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ZKStreamMetadataStore.class));

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private ZKStoreHelper storeHelper;
    private final ZkOrderedStore orderer;

    private final ZKGarbageCollector completedTxnGC;
    private final ZkInt96Counter counter;
    private final Executor executor;

    @VisibleForTesting
    ZKStreamMetadataStore(CuratorFramework client, Executor executor) {
        this(client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS));
    }

    @VisibleForTesting
    ZKStreamMetadataStore(CuratorFramework client, Executor executor, Duration gcPeriod) {
        super(new ZKHostIndex(client, "/hostTxnIndex", executor), new ZKHostIndex(client, "/hostRequestIndex", executor));
        this.storeHelper = new ZKStoreHelper(client, executor);
        this.orderer = new ZkOrderedStore("txnCommitOrderer", storeHelper, executor);
        this.completedTxnGC = new ZKGarbageCollector(COMPLETED_TXN_GC_NAME, storeHelper, this::gcCompletedTxn, gcPeriod);
        this.completedTxnGC.startAsync();
        this.completedTxnGC.awaitRunning();
        this.counter = new ZkInt96Counter(storeHelper);
        this.executor = executor;
    }

    private CompletableFuture<Void> gcCompletedTxn() {
        return storeHelper.getChildren(COMPLETED_TX_BATCH_ROOT_PATH)
                .thenApply(children -> {
                            // retain latest two and delete remainder.
                            List<Long> list = children.stream().map(Long::parseLong).sorted().collect(Collectors.toList());
                            if (list.size() > 2) {
                                return list.subList(0, list.size() - 2);
                            } else {
                                return new ArrayList<Long>();
                            }
                        }
                )
                .thenCompose(toDeleteList -> {
                    log.debug("deleting batches {} on new scheme", toDeleteList);

                    // delete all those marked for toDelete.
                    return Futures.allOf(toDeleteList.stream()
                            .map(toDelete -> storeHelper.deleteTree(String.format(COMPLETED_TX_BATCH_PATH, toDelete)))
                            .collect(Collectors.toList()));
                });
    }

    @Override
    ZKStream newStream(final String scopeName, final String name) {
        return new ZKStream(scopeName, name, storeHelper, completedTxnGC::getLatestBatch, executor, orderer);
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return counter.getNextCounter();
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scope, OperationContext context, Executor executor) {
        String scopePath = ZKPaths.makePath(SCOPE_ROOT_PATH, scope);
        return storeHelper.checkExists(scopePath);
    }

    @Override
    public CompletableFuture<Boolean> isScopeSealed(String scope, OperationContext context, Executor executor) {
        String scopePath = ZKPaths.makePath(SCOPE_DELETE_PATH, scope);
        return storeHelper.checkExists(scopePath);
    }

    @Override
    Version getEmptyVersion() {
        return Version.IntVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
    }

    @Override
    ReaderGroup newReaderGroup(String scope, String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    ZKScope newScope(final String scopeName) {
        return new ZKScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName, OperationContext context, Executor executor) {
        return storeHelper.checkExists(String.format("/store/%s", scopeName))
                .thenApply(scopeExists -> {
                    if (scopeExists) {
                        return scopeName;
                    } else {
                        throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName);
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> listScopes(Executor executor, long requestId) {
        return storeHelper.getChildren(SCOPE_ROOT_PATH)
                .thenApply(children -> children.stream().filter(x -> !x.equals(ZKScope.STREAMS_IN_SCOPE))
                                               .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listScopes(String continuationToken, int limit,
                                                                    Executor executor, long requestId) {
        // Pagination not supported for zk based store. 
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> addStreamTagsToIndex(String scope, String name, StreamConfiguration config, OperationContext context, Executor executor) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeTagsFromIndex(String scope, String name, Set<String> tagsRemoved, OperationContext context, Executor executor) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> addReaderGroupToScope(String scopeName, String rgName, UUID readerGroupId,
                                                         OperationContext context, Executor executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> checkReaderGroupExists(String scope, String rgName, OperationContext context,
                                                             Executor executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<UUID> getReaderGroupId(String scopeName, String rgName, OperationContext context, Executor executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> sealScope(String scope, OperationContext context, ScheduledExecutorService executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(String scope, String name, StreamConfiguration configuration,
                                                                long createTimestamp, OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        ZKScope zkScope = (ZKScope) getScope(scope, context);
        ZKStream zkStream = (ZKStream) getStream(scope, name, context);

        return super.createStream(scope, name, configuration, createTimestamp, context, executor)
                        .thenCompose(status -> zkScope.getNextStreamPosition()
                                    .thenCompose(zkStream::createStreamPositionNodeIfAbsent)
                                    .thenCompose(v -> zkStream.getStreamPosition())
                                    .thenCompose(id -> zkScope.addStreamToScope(name, id))
                                                  .thenApply(x -> status));

    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName, final OperationContext context,
                                                        Executor executor) {
        ZKStream stream = (ZKStream) getStream(scopeName, streamName, context);
        return storeHelper.checkExists(stream.getStreamPath());
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName,
                                                                      OperationContext context, Executor executor) {
        long requestId = getOperationContext(context).getRequestId();
        return storeHelper.getData(String.format(DELETED_STREAMS_PATH, getScopedStreamName(scopeName, streamName)), x -> BitConverter.readInt(x, 0))
                          .handleAsync((data, ex) -> {
                              if (ex == null) {
                                  return data.getObject() + 1;
                              } else if (ex instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.warn(requestId, "Problem found while getting a safe starting segment number for {}.",
                                          getScopedStreamName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          });
    }

    @Override
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, final int lastActiveSegment,
                                                    OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        final String deletePath = String.format(DELETED_STREAMS_PATH, getScopedStreamName(scope, stream));
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return storeHelper.getData(deletePath, x -> BitConverter.readInt(x, 0))
                          .exceptionally(e -> {
                              if (e instanceof StoreException.DataNotFoundException) {
                                  return null;
                              } else {
                                  throw new CompletionException(e);
                              }
                          })
                          .thenCompose(data -> {
                              log.debug(context.getRequestId(),
                                      "Recording last segment {} for stream {}/{} on deletion.",
                                      lastActiveSegment, scope, stream);
                              if (data == null) {
                                  return Futures.toVoid(storeHelper.createZNodeIfNotExist(deletePath, maxSegmentNumberBytes));
                              } else {
                                  final int oldLastActiveSegment = data.getObject();
                                  Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                          "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                          oldLastActiveSegment, scope, stream, lastActiveSegment);
                                  return Futures.toVoid(storeHelper.setData(deletePath, maxSegmentNumberBytes, data.getVersion()));
                              }
                          });
    }

    @Override
    public CompletableFuture<Void> deleteStream(String scope, String name, OperationContext context, Executor executor) {
        ZKScope zkScope = (ZKScope) getScope(scope, context);
        ZKStream zkStream = (ZKStream) getStream(scope, name, context);
        return Futures.exceptionallyExpecting(zkStream.getStreamPosition()
                .thenCompose(id -> zkScope.removeStreamFromScope(name, id)),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                .thenCompose(v -> super.deleteStream(scope, name, context, executor));
    }

    @VisibleForTesting
    public void setStoreHelperForTesting(ZKStoreHelper storeHelper) {
        this.storeHelper = storeHelper;
    }

    @Override
    public void close() {
        completedTxnGC.stopAsync();
        completedTxnGC.awaitTerminated();
    }
    // endregion
}
