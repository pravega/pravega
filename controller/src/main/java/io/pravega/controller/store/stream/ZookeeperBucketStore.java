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
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.bucket.BucketChangeListener;
import io.pravega.controller.server.bucket.BucketOwnershipListener;
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
import java.util.concurrent.atomic.AtomicReference;
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
    private static final String BUCKET_OWNERSHIP_PATH = BUCKET_ROOT_PATH + "/ownership";
    private static final String BUCKET_PATH = BUCKET_ROOT_PATH + "/%d";
    private static final String RETENTION_PATH = BUCKET_PATH + "/retention" + "/%s";
    private static final String WATERMARK_PATH = BUCKET_PATH + "/watermark" + "/%s";
    
    private final int bucketCount;
    private final ConcurrentMap<Integer, PathChildrenCache> bucketCacheMap;
    private final AtomicReference<PathChildrenCache> bucketOwnershipCacheRef;
    private final ZKStoreHelper storeHelper;

    protected ZookeeperBucketStore(int bucketCount, CuratorFramework client, Executor executor) {
        this.bucketCount = bucketCount;
        this.bucketCacheMap = new ConcurrentHashMap<>();
        this.bucketOwnershipCacheRef = new AtomicReference<>();
        storeHelper = new ZKStoreHelper(client, executor);
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
    public void registerBucketChangeListenerForRetention(int bucket, BucketChangeListener listener) {
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

        String bucketRoot = String.format(BUCKET_PATH, bucket);

        bucketCacheMap.put(bucket, new PathChildrenCache(storeHelper.getClient(), bucketRoot, true));
        PathChildrenCache pathChildrenCache = bucketCacheMap.get(bucket);
        pathChildrenCache.getListenable().addListener(bucketListener);
        pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        log.info("bucket {} change notification listener registered", bucket);
    }

    @Override
    public void unregisterBucketChangeListenerForRetention(int bucket) {
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
                                  return storeHelper.getData(bucketPath)
                                                    .thenApply(data -> (SerializationUtils.deserialize(data.getData())).equals(processId));
                              } else {
                                  return CompletableFuture.completedFuture(true);
                              }
                          });
    }

    @Override
    public CompletableFuture<List<String>> getStreamsForRetention(int bucket, Executor executor) {
        return storeHelper.getChildren(String.format(BUCKET_PATH, bucket))
                          .thenApply(list -> list.stream().map(this::decodedScopedStreamName).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addUpdateStreamForRetention(final String scope, final String stream, final RetentionPolicy retentionPolicy,
                                                                   final Executor executor) {
        Preconditions.checkNotNull(retentionPolicy);
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
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
    public CompletableFuture<Void> removeStreamFromRetention(final String scope, final String stream, final Executor executor) {
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
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
