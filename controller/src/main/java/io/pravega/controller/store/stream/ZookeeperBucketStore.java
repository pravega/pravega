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
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.bucket.BucketChangeListener;
import io.pravega.controller.server.bucket.BucketOwnershipListener;
import lombok.Getter;
import lombok.Lombok;
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
    private static final String ROOT_PATH = "/";
    private static final String OWNERSHIP_CHILD_PATH = "ownership";
    @Getter
    private final int bucketCount;
    private final ConcurrentMap<ServiceType, PathChildrenCache> bucketOwnershipCacheMap;
    private final ConcurrentMap<String, PathChildrenCache> bucketCacheMap;
    private final ZKStoreHelper storeHelper;

    protected ZookeeperBucketStore(int bucketCount, CuratorFramework client, Executor executor) {
        this.bucketCount = bucketCount;
        this.bucketCacheMap = new ConcurrentHashMap<>();
        this.bucketOwnershipCacheMap = new ConcurrentHashMap<>();
        storeHelper = new ZKStoreHelper(client, executor);
    }

    @Override
    public void registerBucketOwnershipListener(ServiceType serviceType, BucketOwnershipListener listener) {
        Preconditions.checkNotNull(listener);

        String bucketRootPath = ZKPaths.makePath(ROOT_PATH, serviceType.getName());
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
                    log.warn("Received unknown event {} on bucket root {} ", event.getType(), serviceType);
            }
        };

        String bucketOwnershipPath = ZKPaths.makePath(bucketRootPath, OWNERSHIP_CHILD_PATH);
        bucketOwnershipCacheMap.computeIfAbsent(serviceType, 
                x -> {
                    PathChildrenCache pathChildrenCache = new PathChildrenCache(storeHelper.getClient(), 
                            bucketOwnershipPath, true);

                    pathChildrenCache.getListenable().addListener(bucketListener);
                    try {
                        pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
                    } catch (Exception e) {
                        log.error("Starting ownership listener for service {} threw exception {}", serviceType, e);
                        throw Lombok.sneakyThrow(e);
                    }
                    log.info("bucket ownership listener registered on bucket root {}", serviceType);

                    return pathChildrenCache;
                });
        }

    @Override
    public synchronized void unregisterBucketOwnershipListener(ServiceType serviceType) {
        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.remove(serviceType);
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
    public synchronized void registerBucketChangeListener(ServiceType serviceType, int bucket, BucketChangeListener listener) {
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

        String bucketPath = getBucketPath(serviceType.getName(), bucket);
        
        bucketCacheMap.computeIfAbsent(bucketPath, x -> {
            PathChildrenCache pathChildrenCache = new PathChildrenCache(storeHelper.getClient(), bucketPath, true);
            pathChildrenCache.getListenable().addListener(bucketListener);
            try {
                pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
            } catch (Exception e) {
                log.error("Starting listener on bucket {} for service {} threw exception {}", bucket, serviceType, e);
                throw Lombok.sneakyThrow(e);
            }
            log.info("bucket {} change notification listener registered", bucket);
            return pathChildrenCache;
        });
    }

    private void withThrows(Runnable runnable) {
    }


    @Override
    public void unregisterBucketChangeListener(ServiceType serviceType, int bucket) {
        String bucketPath = getBucketPath(serviceType.getName(), bucket);

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
        String bucketRootPath = ZKPaths.makePath(ROOT_PATH, bucketRoot);
        return ZKPaths.makePath(bucketRootPath, "" + bucket);
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(ServiceType serviceType, int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < bucketCount);

        String bucketRootPath = ZKPaths.makePath(ROOT_PATH, serviceType.getName());
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
    public CompletableFuture<List<String>> getStreamsForBucket(ServiceType serviceType, int bucket, Executor executor) {
        String bucketPath = getBucketPath(serviceType.getName(), bucket);

        return storeHelper.getChildren(bucketPath)
                          .thenApply(list -> list.stream().map(this::decodedScopedStreamName).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> addStreamToBucketStore(final ServiceType serviceType, final String scope, final String stream,
                                                          final Executor executor) {
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketPath = getBucketPath(serviceType.getName(), bucket);
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
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> removeStreamFromBucketStore(final ServiceType serviceType, final String scope, 
                                                               final String stream, final Executor executor) {
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketPath = getBucketPath(serviceType.getName(), bucket);
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
