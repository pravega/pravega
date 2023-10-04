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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.client.StoreType;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;

@SuppressWarnings("deprecation")
@Slf4j
public class ZookeeperBucketStore implements BucketStore {
    private static final String ROOT_PATH = "/";
    private static final String OWNERSHIP_CHILD_PATH = "ownership";
    private static final String BUCKET_CONTROLLER_MAPPING_PATH = "bucketControllerMapping";
    private final ImmutableMap<ServiceType, Integer> bucketCountMap;
    @Getter
    private final ZKStoreHelper storeHelper;
    @Getter
    private final CuratorFramework client;

    ZookeeperBucketStore(ImmutableMap<ServiceType, Integer> bucketCountMap, CuratorFramework client, Executor executor) {
        this.bucketCountMap = bucketCountMap;
        storeHelper = new ZKStoreHelper(client, executor);
        this.client = client;
    }

    public boolean isZKConnected() {
        return storeHelper.isZKConnected();
    }
    
    @Override
    public StoreType getStoreType() {
        return StoreType.Zookeeper;
    }

    @Override
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
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
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
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
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public CompletableFuture<Void> removeStreamFromBucketStore(final ServiceType serviceType, final String scope, 
                                                               final String stream, final Executor executor) {
        int bucketCount = bucketCountMap.get(serviceType);
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketPath = getBucketPath(serviceType, bucket);
        String streamPath = ZKPaths.makePath(bucketPath, encodedScopedStreamName(scope, stream));

        return Futures.exceptionallyExpecting(storeHelper.deleteNode(streamPath), 
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null);
    }

    @Override
    public CompletableFuture<Map<String, Set<Integer>>> getBucketControllerMap(final ServiceType serviceType) {
        String bucketHostMapPath = getBucketControllerMapPath(serviceType);
        return storeHelper.createZNodeIfNotExist(bucketHostMapPath, BucketControllerMap.EMPTY.toBytes())
                .thenCompose(created -> storeHelper.getData(bucketHostMapPath, BucketControllerMap::fromBytes))
                .thenApply(bucketControllerMap -> bucketControllerMap.getObject().getBucketControllerMap());
    }

    @Override
    public CompletableFuture<Void> updateBucketControllerMap(final Map<String, Set<Integer>> newMapping,
                                                             final ServiceType serviceType) {
        Preconditions.checkNotNull(newMapping, "newMapping");
        String bucketHostMapPath = getBucketControllerMapPath(serviceType);
        return storeHelper.createZNodeIfNotExist(bucketHostMapPath, BucketControllerMap.EMPTY.toBytes())
                .thenCompose(created -> storeHelper.setData(bucketHostMapPath,
                        BucketControllerMap.createBucketControllerMap(newMapping).toBytes(), null))
                          .thenAccept(result -> log.debug("updateBucketControllerMap is {}", result));
    }

    @Override
    public CompletableFuture<Set<Integer>> getBucketsForController(final String processId, final ServiceType serviceType) {
        return getBucketControllerMap(serviceType).thenApply(map -> map.get(processId) == null
                ? new HashSet<>() : map.get(processId));
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

    public CompletableFuture<Boolean> releaseBucketOwnership(final ServiceType serviceType, int bucketId, String processId) {
        String bucketPath = ZKPaths.makePath(getBucketOwnershipPath(serviceType), "" + bucketId);
        return storeHelper.checkExists(bucketPath).thenCompose(exists -> {
            if (exists) {
                return storeHelper.getData(bucketPath, x -> x)
                                  .thenApply(data -> (SerializationUtils.deserialize(data.getObject())).equals(processId))
                                  .thenCompose(isValid -> {
                                      if (isValid) {
                                          return storeHelper.deleteNode(bucketPath).thenCompose(v ->
                                                                    storeHelper.checkExists(bucketPath))
                                                            .thenApply(exist -> !exist);
                                      } else {
                                          log.warn("{} : Unable to release the bucket service {} as it is acquired by another controller instance",
                                                  serviceType, bucketId);
                                          return CompletableFuture.completedFuture(false);
                                      }
                                  });
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

    public NodeCache getBucketControllerMapNodeCache(ServiceType serviceType) {
        return storeHelper.getNodeCache(getBucketControllerMapPath(serviceType), false);
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

    public String getBucketControllerMapPath(final ServiceType serviceType) {
        String bucketRootPath = getBucketRootPath(serviceType);
        return ZKPaths.makePath(bucketRootPath, BUCKET_CONTROLLER_MAPPING_PATH);
    }
}
