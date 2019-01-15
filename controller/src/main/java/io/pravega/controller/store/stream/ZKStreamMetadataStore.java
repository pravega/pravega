/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.common.lang.Int96;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.Retry;
import io.pravega.controller.server.retention.BucketChangeListener;
import io.pravega.controller.server.retention.BucketOwnershipListener;
import io.pravega.controller.server.retention.BucketOwnershipListener.BucketNotification;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.util.Config;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.pravega.controller.server.retention.BucketChangeListener.StreamNotification;
import static io.pravega.controller.server.retention.BucketChangeListener.StreamNotification.NotificationType;

/**
 * ZK stream metadata store.
 */
@Slf4j
class ZKStreamMetadataStore extends AbstractStreamMetadataStore implements AutoCloseable {
    @VisibleForTesting
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
    static final int COUNTER_RANGE = 10000;
    static final String COUNTER_PATH = "/counter";
    static final String DELETED_STREAMS_PATH = "/lastActiveStreamSegment/%s";
    private static final String BUCKET_ROOT_PATH = "/buckets";
    private static final String BUCKET_OWNERSHIP_PATH = BUCKET_ROOT_PATH + "/ownership";
    private static final String BUCKET_PATH = BUCKET_ROOT_PATH + "/%d";
    private static final String RETENTION_PATH = BUCKET_PATH + "/%s";
    private static final String TRANSACTION_ROOT_PATH = "/transactions";
    private static final String COMPLETED_TXN_GC_NAME = "completedTxnGC";
    static final String ACTIVE_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/activeTx";
    static final String COMPLETED_TX_ROOT_PATH = TRANSACTION_ROOT_PATH + "/completedTx";
    static final String COMPLETED_TX_BATCH_ROOT_PATH = COMPLETED_TX_ROOT_PATH + "/batches";
    static final String COMPLETED_TX_BATCH_PATH = COMPLETED_TX_BATCH_ROOT_PATH + "/%d";
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private ZKStoreHelper storeHelper;
    private final ConcurrentMap<Integer, PathChildrenCache> bucketCacheMap;
    private final AtomicReference<PathChildrenCache> bucketOwnershipCacheRef;
    private final Object lock;
    @GuardedBy("lock")
    private final AtomicInt96 limit;
    @GuardedBy("lock")
    private final AtomicInt96 counter;
    @GuardedBy("lock")
    private volatile CompletableFuture<Void> refreshFutureRef;

    private final ZKGarbageCollector completedTxnGC;

    ZKStreamMetadataStore(CuratorFramework client, Executor executor) {
        this (client, Config.BUCKET_COUNT, executor);
    }

    @VisibleForTesting
    ZKStreamMetadataStore(CuratorFramework client, int bucketCount, Executor executor) {
        this(client, bucketCount, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS));
    }

    @VisibleForTesting
    ZKStreamMetadataStore(CuratorFramework client, int bucketCount, Executor executor, Duration gcPeriod) {
        super(new ZKHostIndex(client, "/hostTxnIndex", executor), bucketCount);
        storeHelper = new ZKStoreHelper(client, executor);
        bucketCacheMap = new ConcurrentHashMap<>();
        bucketOwnershipCacheRef = new AtomicReference<>();
        this.lock = new Object();
        this.counter = new AtomicInt96();
        this.limit = new AtomicInt96();
        this.refreshFutureRef = null;
        this.completedTxnGC = new ZKGarbageCollector(COMPLETED_TXN_GC_NAME, storeHelper, this::gcCompletedTxn, gcPeriod);
        this.completedTxnGC.startAsync();
        this.completedTxnGC.awaitRunning();
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
                    log.debug("deleting batches {} on new scheme" + toDeleteList);

                    // delete all those marked for toDelete.
                    return Futures.allOf(toDeleteList.stream()
                            .map(toDelete -> storeHelper.deleteTree(String.format(COMPLETED_TX_BATCH_PATH, toDelete)))
                            .collect(Collectors.toList()));
                });
    }

    @Override
    ZKStream newStream(final String scope, final String name) {
        return new ZKStream(scope, name, storeHelper, completedTxnGC::getLatestBatch);
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        CompletableFuture<Int96> future;
        synchronized (lock) {
            Int96 next = counter.incrementAndGet();
            if (next.compareTo(limit.get()) > 0) {
                // ignore the counter value and after refreshing call getNextCounter
                future = refreshRangeIfNeeded().thenCompose(x -> getNextCounter());
            } else {
                future = CompletableFuture.completedFuture(next);
            }
        }
        return future;
    }

    @Override
    Version getEmptyVersion() {
        return Version.IntVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
    }

    @VisibleForTesting
    CompletableFuture<Void> refreshRangeIfNeeded() {
        CompletableFuture<Void> refreshFuture;
        synchronized (lock) {
            // Ensure that only one background refresh is happening. For this we will reference the future in refreshFutureRef
            // If reference future ref is not null, we will return the reference to that future.
            // It is set to null when refresh completes.
            refreshFuture = this.refreshFutureRef;
            if (this.refreshFutureRef == null) {
                // no ongoing refresh, check if refresh is still needed
                if (counter.get().compareTo(limit.get()) >= 0) {
                    log.info("Refreshing counter range. Current counter is {}. Current limit is {}", counter.get(), limit.get());

                    // Need to refresh counter and limit. Start a new refresh future. We are under lock so no other
                    // concurrent thread can start the refresh future.
                    refreshFutureRef = getRefreshFuture()
                            .exceptionally(e -> {
                                // if any exception is thrown here, we would want to reset refresh future so that it can be retried.
                                synchronized (lock) {
                                    refreshFutureRef = null;
                                }
                                log.warn("Exception thrown while trying to refresh transaction counter range", e);
                                throw new CompletionException(e);
                            });
                    // Note: refreshFutureRef is reset to null under the lock, and since we have the lock in this thread
                    // until we release it, refresh future ref cannot be reset to null. So we will always return a non-null
                    // future from here.
                    refreshFuture = refreshFutureRef;
                } else {
                    // nothing to do
                    refreshFuture = CompletableFuture.completedFuture(null);
                }
            }
        }
        return refreshFuture;
    }

    @VisibleForTesting
    CompletableFuture<Void> getRefreshFuture() {
        return storeHelper.createZNodeIfNotExist(COUNTER_PATH, Int96.ZERO.toBytes())
                .thenCompose(v -> storeHelper.getData(COUNTER_PATH)
                        .thenCompose(data -> {
                            Int96 previous = Int96.fromBytes(data.getData());
                            Int96 nextLimit = previous.add(COUNTER_RANGE);
                            return storeHelper.setData(COUNTER_PATH, new Data(nextLimit.toBytes(), data.getVersion()))
                                    .thenAccept(x -> {
                                        // Received new range, we should reset the counter and limit under the lock
                                        // and then reset refreshfutureref to null
                                        synchronized (lock) {
                                            // Note: counter is set to previous range's highest value. Always get the
                                            // next counter by calling counter.incrementAndGet otherwise there will
                                            // be a collision with counter used by someone else.
                                            counter.set(previous.getMsb(), previous.getLsb());
                                            limit.set(nextLimit.getMsb(), nextLimit.getLsb());
                                            refreshFutureRef = null;
                                            log.info("Refreshed counter range. Current counter is {}. Current limit is {}", counter.get(), limit.get());
                                        }
                                    });
                        }));
    }

    @Override
    ZKScope newScope(final String scopeName) {
        return new ZKScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
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
    public CompletableFuture<List<String>> listScopes() {
        return storeHelper.listScopes();
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName) {
        ZKStream stream = newStream(scopeName, streamName);
        return storeHelper.checkExists(stream.getStreamPath());
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        return storeHelper.getData(String.format(DELETED_STREAMS_PATH, getScopedStreamName(scopeName, streamName)))
                          .handleAsync((data, ex) -> {
                              if (ex == null) {
                                  return BitConverter.readInt(data.getData(), 0) + 1;
                              } else if (ex instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.error("Problem found while getting a safe starting segment number for {}.",
                                          getScopedStreamName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          });
    }

    @Override
    @SneakyThrows
    public void registerBucketOwnershipListener(BucketOwnershipListener listener) {
        Preconditions.checkNotNull(listener);

        PathChildrenCacheListener bucketListener = (client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED:
                    // no action required
                    break;
                case CHILD_REMOVED:
                    int bucketId = Integer.parseInt(ZKPaths.getNodeFromPath(event.getData().getPath()));
                    listener.notify(new BucketNotification(bucketId, BucketNotification.NotificationType.BucketAvailable));
                    break;
                case CONNECTION_LOST:
                    listener.notify(new BucketNotification(Integer.MIN_VALUE, BucketNotification.NotificationType.ConnectivityError));
                    break;
                default:
                    log.warn("Received unknown event {}", event.getType());
            }
        };

        bucketOwnershipCacheRef.compareAndSet(null, new PathChildrenCache(storeHelper.getClient(), BUCKET_OWNERSHIP_PATH, true));

        bucketOwnershipCacheRef.get().getListenable().addListener(bucketListener);
        bucketOwnershipCacheRef.get().start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        log.info("bucket ownership listener registered");
    }

    @Override
    public void unregisterBucketOwnershipListener() {
        if (bucketOwnershipCacheRef.get() != null) {
            try {
                bucketOwnershipCacheRef.get().clear();
                bucketOwnershipCacheRef.get().close();
            } catch (IOException e) {
                log.warn("unable to close listener for bucket ownership {}", e);
            }
        }
    }

    @Override
    @SneakyThrows
    public void registerBucketChangeListener(int bucket, BucketChangeListener listener) {
        Preconditions.checkNotNull(listener);

        PathChildrenCacheListener bucketListener = (client, event) -> {
            StreamImpl stream;
            switch (event.getType()) {
                case CHILD_ADDED:
                    stream = getStreamFromPath(event.getData().getPath());
                    listener.notify(new StreamNotification(stream.getScope(), stream.getStreamName(), NotificationType.StreamAdded));
                    break;
                case CHILD_REMOVED:
                    stream = getStreamFromPath(event.getData().getPath());
                    listener.notify(new StreamNotification(stream.getScope(), stream.getStreamName(), NotificationType.StreamRemoved));
                    break;
                case CHILD_UPDATED:
                    stream = getStreamFromPath(event.getData().getPath());
                    listener.notify(new StreamNotification(stream.getScope(), stream.getStreamName(), NotificationType.StreamUpdated));
                    break;
                case CONNECTION_LOST:
                    listener.notify(new StreamNotification(null, null, NotificationType.ConnectivityError));
                    break;
                default:
                    log.warn("Received unknown event {} on bucket", event.getType(), bucket);
            }
        };

        String bucketRoot = String.format(BUCKET_PATH, bucket);

        bucketCacheMap.put(bucket, new PathChildrenCache(storeHelper.getClient(), bucketRoot, true));
        PathChildrenCache pathChildrenCache = bucketCacheMap.get(bucket);
        pathChildrenCache.getListenable().addListener(bucketListener);
        pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        log.info("bucket {} change notification listener registered", bucket);
    }

    @Override
    public void unregisterBucketListener(int bucket) {
        PathChildrenCache cache = bucketCacheMap.remove(bucket);
        if (cache != null) {
            try {
                cache.clear();
                cache.close();
            } catch (IOException e) {
                log.warn("unable to close watch on bucket {} with exception {}", bucket, e);
            }
        }
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < bucketCount);

        // try creating an ephemeral node
        String bucketPath = ZKPaths.makePath(BUCKET_OWNERSHIP_PATH, String.valueOf(bucket));

        return storeHelper.createEphemeralZNode(bucketPath, SerializationUtils.serialize(processId))
                .thenCompose(created -> {
                    if (!created) {
                        // Note: data may disappear by the time we do a getData. Let exception be thrown from here
                        // so that caller may retry.
                        return storeHelper.getData(bucketPath)
                                .thenApply(data -> (SerializationUtils.deserialize(data.getData())).equals(processId));
                    } else {
                        return CompletableFuture.completedFuture(true);
                    }
                });
    }

    @Override
    public CompletableFuture<List<String>> getStreamsForBucket(int bucket, Executor executor) {
        return storeHelper.getChildren(String.format(BUCKET_PATH, bucket))
                .thenApply(list -> list.stream().map(this::decodedScopedStreamName).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addUpdateStreamForAutoStreamCut(final String scope, final String stream, final RetentionPolicy retentionPolicy,
                                                                   final OperationContext context, final Executor executor) {
        Preconditions.checkNotNull(retentionPolicy);
        int bucket = getBucket(scope, stream);
        String retentionPath = String.format(RETENTION_PATH, bucket, encodedScopedStreamName(scope, stream));
        byte[] serialize = SerializationUtils.serialize(retentionPolicy);

        return storeHelper.getData(retentionPath)
                .exceptionally(e -> {
                    if (e instanceof StoreException.DataNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                }).thenCompose(data -> {
                    if (data == null) {
                        return Futures.toVoid(storeHelper.createZNodeIfNotExist(retentionPath, serialize));
                    } else {
                        return Futures.toVoid(storeHelper.setData(retentionPath, new Data(serialize, data.getVersion())));
                    }
                });
    }

    @Override
    public CompletableFuture<Void> removeStreamFromAutoStreamCut(final String scope, final String stream,
                                                                 final OperationContext context, final Executor executor) {
        int bucket = getBucket(scope, stream);
        String retentionPath = String.format(RETENTION_PATH, bucket, encodedScopedStreamName(scope, stream));

        return storeHelper.deleteNode(retentionPath)
                .exceptionally(e -> {
                    if (e instanceof StoreException.DataNotFoundException) {
                        return null;
                    } else {
                        throw new CompletionException(e);
                    }
                });
    }

    @Override
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, final int lastActiveSegment,
                                                    OperationContext context, final Executor executor) {
        final String deletePath = String.format(DELETED_STREAMS_PATH, getScopedStreamName(scope, stream));
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return storeHelper.getData(deletePath)
                          .exceptionally(e -> {
                              if (e instanceof StoreException.DataNotFoundException) {
                                  return null;
                              } else {
                                  throw new CompletionException(e);
                              }
                          })
                          .thenCompose(data -> {
                              log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
                              if (data == null) {
                                  return Futures.toVoid(storeHelper.createZNodeIfNotExist(deletePath, maxSegmentNumberBytes));
                              } else {
                                  final int oldLastActiveSegment = BitConverter.readInt(data.getData(), 0);
                                  Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                          "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                          oldLastActiveSegment, scope, stream, lastActiveSegment);
                                  return Futures.toVoid(storeHelper.setData(deletePath, new Data(maxSegmentNumberBytes, data.getVersion())));
                              }
                          });
    }

    /**
     * When managing the Controller's metadata in Zookeeper, we explicitly create parent bucket zNodes (so they are of
     * type "zNode"). Otherwise, they may be inadvertently created as Zookeeper "containers" by Curator. This would lead
     * these zNodes to be candidates for automatic removal by Zookeeper if they become empty.
     */
    @Override
    public void initializeMetadataStore() {
        List<CompletableFuture<Void>> initializationFutures = new ArrayList<>();
        Retry.RetryWithBackoff retryPolicy = Retry.withExpBackoff(50, 2, 10);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            final String bucketPath = String.format(BUCKET_PATH, bucket);
            initializationFutures.add(retryPolicy.retryWhen(ex -> true).run(() ->
                    storeHelper.addNode(bucketPath).whenComplete((v, ex) -> {
                        if (ex == null) {
                            log.debug("Stream bucket correctly initialized: {}.", bucketPath);
                        } else if (Exceptions.unwrap(ex) instanceof StoreException.DataExistsException) {
                            log.debug("Stream bucket already initialized: {}.", bucketPath);
                        } else {
                            throw new CompletionException("Unexpected exception initializing Stream bucket.", ex);
                        }
                    })));
        }

        // Make sure that Stream buckets are created after leaving this method.
        Futures.allOf(initializationFutures).join();
    }

    private String encodedScopedStreamName(String scope, String stream) {
        String scopedStreamName = getScopedStreamName(scope, stream);
        return Base64.getEncoder().encodeToString(scopedStreamName.getBytes());
    }

    private String decodedScopedStreamName(String encodedScopedStreamName) {
        return new String(Base64.getDecoder().decode(encodedScopedStreamName));
    }

    private StreamImpl getStreamFromPath(String path) {
        String scopedStream = decodedScopedStreamName(ZKPaths.getNodeFromPath(path));
        String[] splits = scopedStream.split("/");
        return new StreamImpl(splits[0], splits[1]);
    }

    // region getters and setters for testing
    @VisibleForTesting
    void setCounterAndLimitForTesting(int counterMsb, long counterLsb, int limitMsb, long limitLsb) {
        synchronized (lock) {
            limit.set(limitMsb, limitLsb);
            counter.set(counterMsb, counterLsb);
        }
    }

    @VisibleForTesting
    Int96 getLimitForTesting() {
        synchronized (lock) {
            return limit.get();
        }
    }

    @VisibleForTesting
    Int96 getCounterForTesting() {
        synchronized (lock) {
            return counter.get();
        }
    }

    @VisibleForTesting
    public void setStoreHelperForTesting(ZKStoreHelper storeHelper) {
        this.storeHelper = storeHelper;
    }

    @Override
    public void close() throws Exception {
        completedTxnGC.stopAsync();
        completedTxnGC.awaitTerminated();
    }
    // endregion
}
