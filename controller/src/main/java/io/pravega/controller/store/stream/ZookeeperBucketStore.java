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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.client.StoreType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;

import java.util.Base64;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@Slf4j
public class ZookeeperBucketStore implements BucketStore {
    private static final String ROOT_PATH = "/";
    private static final String OWNERSHIP_CHILD_PATH = "ownership";
    private final ImmutableMap<ServiceType, Integer> bucketCountMap;
    @Getter
    private final ZKStoreHelper storeHelper;

    ZookeeperBucketStore(ImmutableMap<ServiceType, Integer> bucketCountMap, CuratorFramework client, Executor executor) {
        this.bucketCountMap = bucketCountMap;
        storeHelper = new ZKStoreHelper(client, executor);
    }

    @Override
    public StoreType getStoreType() {
        return StoreType.Zookeeper;
    }

    @Override
    public int getBucketCount(ServiceType serviceType) {
        return bucketCountMap.get(serviceType);
    }
    
    public CompletableFuture<Void> createBucketsRoot(ServiceType serviceType) {
        String bucketRootPath = getBucketRootPath(serviceType);
        String bucketOwnershipPath = getBucketOwnershipPath(serviceType);
        return Futures.toVoid(storeHelper.createZNodeIfNotExist(bucketRootPath)
                          .thenCompose(x -> storeHelper.createZNodeIfNotExist(bucketOwnershipPath)));
    }

    public CompletableFuture<Void> createBucket(ServiceType serviceType, int bucketId) {
        String bucketPath = getBucketPath(serviceType, bucketId);
        return Futures.toVoid(createBucketsRoot(serviceType)
                          .thenCompose(x -> storeHelper.createZNodeIfNotExist(bucketPath)));
    }

    @Override
    public CompletableFuture<Set<String>> getStreamsForBucket(ServiceType serviceType, int bucket, Executor executor) {
        String bucketPath = getBucketPath(serviceType, bucket);

        return storeHelper.getChildren(bucketPath)
                          .thenApply(list -> list.stream().map(this::decodedScopedStreamName).collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Void> addStreamToBucketStore(final ServiceType serviceType, final String scope, final String stream,
                                                          final Executor executor) {
        int bucketCount = bucketCountMap.get(serviceType);
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketPath = getBucketPath(serviceType, bucket);
        String streamPath = ZKPaths.makePath(bucketPath, encodedScopedStreamName(scope, stream));

        return storeHelper.checkExists(streamPath)
            .thenCompose(exists -> {
                if (exists) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    return Futures.toVoid(storeHelper.createZNodeIfNotExist(streamPath));
                }
            });
    }

    @Override
    public CompletableFuture<Void> removeStreamFromBucketStore(final ServiceType serviceType, final String scope, 
                                                               final String stream, final Executor executor) {
        int bucketCount = bucketCountMap.get(serviceType);
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketPath = getBucketPath(serviceType, bucket);
        String streamPath = ZKPaths.makePath(bucketPath, encodedScopedStreamName(scope, stream));

        return Futures.exceptionallyExpecting(storeHelper.deleteNode(streamPath), 
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null);
    }
    
    public CompletableFuture<Boolean> takeBucketOwnership(final ServiceType serviceType, int bucketId, String processId) {
        String bucketPath = ZKPaths.makePath(getBucketOwnershipPath(serviceType), "" + bucketId);
        return storeHelper.createEphemeralZNode(bucketPath, SerializationUtils.serialize(processId))
                          .thenCompose(created -> {
                              if (!created) {
                                  return storeHelper.getData(bucketPath, x -> x)
                                                    .thenApply(data -> (SerializationUtils.deserialize(data.getObject())).equals(processId));
                              } else {
                                  return CompletableFuture.completedFuture(true);
                              }
                          });
    }

    public PathChildrenCache getBucketPathChildrenCache(ServiceType serviceType, int bucketId) {
        return storeHelper.getPathChildrenCache(getBucketPath(serviceType, bucketId), true);
    }

    public PathChildrenCache getServiceOwnershipPathChildrenCache(ServiceType serviceType) {
        return storeHelper.getPathChildrenCache(getBucketOwnershipPath(serviceType), true);
    }

    public StreamImpl getStreamFromPath(String path) {
        String scopedStream = decodedScopedStreamName(ZKPaths.getNodeFromPath(path));
        String[] splits = scopedStream.split("/");
        return new StreamImpl(splits[0], splits[1]);
    }

    private String encodedScopedStreamName(String scope, String stream) {
        String scopedStreamName = BucketStore.getScopedStreamName(scope, stream);
        return Base64.getEncoder().encodeToString(scopedStreamName.getBytes());
    }

    private String decodedScopedStreamName(String encodedScopedStreamName) {
        return new String(Base64.getDecoder().decode(encodedScopedStreamName));
    }
    
    private String getBucketRootPath(final ServiceType serviceType) {
        return ZKPaths.makePath(ROOT_PATH, serviceType.getName());
    }

    private String getBucketOwnershipPath(final ServiceType serviceType) {
        String bucketRootPath = getBucketRootPath(serviceType);
        return ZKPaths.makePath(bucketRootPath, OWNERSHIP_CHILD_PATH);
    }

    private String getBucketPath(final ServiceType serviceType, final int bucket) {
        String bucketRootPath = ZKPaths.makePath(ROOT_PATH, serviceType.getName());
        return ZKPaths.makePath(bucketRootPath, "" + bucket);
    }
}
