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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.bucket.BucketChangeListener;
import io.pravega.controller.server.bucket.BucketOwnershipListener;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static io.pravega.controller.server.bucket.BucketOwnershipListener.BucketNotification;
import static io.pravega.controller.server.bucket.BucketChangeListener.StreamNotification;
import static io.pravega.controller.server.bucket.BucketChangeListener.StreamNotification.NotificationType;

/**
 * TODO: shivesh
 */
@Slf4j
public class ZookeeperBucketStore implements BucketStore {
    private static final String BUCKET_ROOT_PATH = "/buckets";
    private static final String OWNERSHIP_CHILD_PATH = "ownership";
    @Getter
    private final int bucketCount;
    private final ConcurrentMap<String, PathChildrenCache> bucketOwnershipCacheMap;
    private final ConcurrentMap<String, PathChildrenCache> bucketCacheMap;
    private final ZKStoreHelper storeHelper;

    protected ZookeeperBucketStore(int bucketCount, CuratorFramework client, Executor executor) {
        this.bucketCount = bucketCount;
        this.bucketCacheMap = new ConcurrentHashMap<>();
        this.bucketOwnershipCacheMap = new ConcurrentHashMap<>();
        storeHelper = new ZKStoreHelper(client, executor);
    }

    @Override
    @SneakyThrows
    public void registerBucketOwnershipListener(String bucketRoot, BucketOwnershipListener listener) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(bucketRoot));
        Preconditions.checkNotNull(listener);

        String bucketRootPath = ZKPaths.makePath(BUCKET_ROOT_PATH, bucketRoot);
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
                    log.warn("Received unknown event {} on bucket root {} ", event.getType(), bucketRoot);
            }
        };

        String bucketOwnershipPath = ZKPaths.makePath(bucketRootPath, OWNERSHIP_CHILD_PATH);
        bucketOwnershipCacheMap.computeIfAbsent(bucketRoot, 
                x -> {
                    PathChildrenCache pathChildrenCache = new PathChildrenCache(storeHelper.getClient(), 
                            bucketOwnershipPath, true);

                    pathChildrenCache.getListenable().addListener(bucketListener);
                    pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                    log.info("bucket ownership listener registered on bucket root {}", bucketRoot);

                    return pathChildrenCache;
                });
        }

    @Override
    public synchronized void unregisterBucketOwnershipListener(String bucketRoot) {
        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.remove(bucketRoot);
        if (pathChildrenCache != null) {
            try {
                pathChildrenCache.clear();
                pathChildrenCache.close();
            } catch (IOException e) {
                log.warn("unable to close listener for bucket ownership {}", e);
            }
        }
    }

    @Override
    @SneakyThrows
    public synchronized void registerBucketChangeListener(String bucketRoot, int bucket, BucketChangeListener listener) {
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
                    listener.notify(new StreamNotification(null, null, BucketChangeListener.StreamNotification.NotificationType.ConnectivityError));
                    break;
                default:
                    log.warn("Received unknown event {} on bucket", event.getType(), bucket);
            }
        };

        String bucketPath = getBucketPath(bucketRoot, bucket);
        
        bucketCacheMap.computeIfAbsent(bucketPath, x -> {
            PathChildrenCache pathChildrenCache = new PathChildrenCache(storeHelper.getClient(), bucketPath, true);
            pathChildrenCache.getListenable().addListener(bucketListener);
            pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
            log.info("bucket {} change notification listener registered", bucket);
            return pathChildrenCache;
        });
    }

    @Override
    public void unregisterBucketChangeListener(String bucketRoot, int bucket) {
        String bucketPath = getBucketPath(bucketRoot, bucket);

        PathChildrenCache cache = bucketCacheMap.remove(bucketPath);
        if (cache != null) {
            try {
                cache.clear();
                cache.close();
            } catch (IOException e) {
                log.warn("unable to close watch on bucket {} with exception {}", bucketPath, e);
            }
        }
    }

    private String getBucketPath(String bucketRoot, int bucket) {
        String bucketRootPath = ZKPaths.makePath(BUCKET_ROOT_PATH, bucketRoot);
        return ZKPaths.makePath(bucketRootPath, "" + bucket);
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(String bucketRoot, int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < bucketCount);

        String bucketRootPath = ZKPaths.makePath(BUCKET_ROOT_PATH, bucketRoot);
        String bucketOwnershipPath = ZKPaths.makePath(bucketRootPath, OWNERSHIP_CHILD_PATH);

        // try creating an ephemeral node
        String bucketPath = ZKPaths.makePath(bucketOwnershipPath, String.valueOf(bucket));

        return storeHelper.createEphemeralZNode(bucketPath, SerializationUtils.serialize(processId))
                          .thenCompose(created -> {
                              if (!created) {
                                  return storeHelper.getData(bucketPath)
                                                    .thenApply(data -> (SerializationUtils.deserialize(data.getData())).equals(processId));
                              } else {
                                  return CompletableFuture.completedFuture(true);
                              }
                          });
    }

    @Override
    public CompletableFuture<List<String>> getStreamsForBucket(String bucketRoot, int bucket, Executor executor) {
        String bucketPath = getBucketPath(bucketRoot, bucket);

        return storeHelper.getChildren(bucketPath)
                          .thenApply(list -> list.stream().map(this::decodedScopedStreamName).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addUpdateStreamToBucketStore(final String bucketRoot, final String scope, final String stream, 
                                                                final Executor executor) {
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketPath = getBucketPath(bucketRoot, bucket);
        String streamPath = ZKPaths.makePath(bucketPath, encodedScopedStreamName(scope, stream));

        return storeHelper.getData(streamPath)
                          .exceptionally(e -> {
                              if (e instanceof StoreException.DataNotFoundException) {
                                  return null;
                              } else {
                                  throw new CompletionException(e);
                              }
                          }).thenCompose(data -> {
                    if (data == null) {
                        return Futures.toVoid(storeHelper.createZNodeIfNotExist(streamPath));
                    } else {
                        return Futures.toVoid(storeHelper.setData(streamPath));
                    }
                });
    }

    @Override
    public CompletableFuture<Void> removeStreamFromBucketStore(final String bucketRoot, final String scope, 
                                                               final String stream, final Executor executor) {
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketPath = getBucketPath(bucketRoot, bucket);
        String streamPath = ZKPaths.makePath(bucketPath, encodedScopedStreamName(scope, stream));

        return storeHelper.deleteNode(streamPath)
                          .exceptionally(e -> {
                              if (e instanceof StoreException.DataNotFoundException) {
                                  return null;
                              } else {
                                  throw new CompletionException(e);
                              }
                          });
    }

    private String encodedScopedStreamName(String scope, String stream) {
        String scopedStreamName = BucketStore.getScopedStreamName(scope, stream);
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
}
