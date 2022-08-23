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
package io.pravega.controller.server.bucket;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import io.pravega.controller.util.RetryHelper;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.utils.ZKPaths;

@SuppressWarnings("deprecation")
@Slf4j
public class ZooKeeperBucketManager extends BucketManager {
    private final ZookeeperBucketStore bucketStore;
    private final ConcurrentMap<BucketStore.ServiceType, PathChildrenCache> bucketOwnershipCacheMap;
    private LeaderSelector leaderSelector;
    private final String processId;

    ZooKeeperBucketManager(String processId, ZookeeperBucketStore bucketStore, BucketStore.ServiceType serviceType, ScheduledExecutorService executor,
                           Function<Integer, BucketService> bucketServiceSupplier) {
        super(processId, serviceType, executor, bucketServiceSupplier, bucketStore);
        bucketOwnershipCacheMap = new ConcurrentHashMap<>();
        this.bucketStore = bucketStore;
        this.processId = processId;
        addBucketControllerMapListener();
        //The leader which monitors the data cluster and ensures all buckets are mapped to available controllers.
        startLeaderElection(new BucketManagerLeader(bucketStore, 10,
                new UniformBucketDistributor(), serviceType, this));
        startLeader();
    }

    /**
     * Get the health status.
     *
     * @return true if zookeeper is connected.
     */
    @Override
    public boolean isHealthy() {
        return this.bucketStore.isZKConnected();
    }

    @Override
    protected int getBucketCount() {
        return bucketStore.getBucketCount(getServiceType());
    }

    @Override
    public void startBucketOwnershipListener() {
        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.computeIfAbsent(getServiceType(),
                x -> bucketStore.getServiceOwnershipPathChildrenCache(getServiceType()));

        PathChildrenCacheListener bucketListener = (client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED:
                    // no action required
                    break;
                case CHILD_REMOVED:
                    int bucketId = Integer.parseInt(ZKPaths.getNodeFromPath(event.getData().getPath()));
                    bucketStore.getBucketsForController(processId, getServiceType())
                            .thenAccept(buckets -> {
                                if (buckets.contains(bucketId)) {
                                    RetryHelper.withIndefiniteRetriesAsync(() -> tryTakeOwnership(bucketId),
                                            e -> log.warn("{}: exception while attempting to take ownership for bucket {}: {}", getServiceType(),
                                                    bucketId, e.getMessage()), getExecutor());
                                }
                            }).join();
                    break;
                case CONNECTION_LOST:
                    log.warn("{}: Received connectivity error", getServiceType());
                    break;
                default:
                    log.warn("Received unknown event {} on bucket root {} ", event.getType(), getServiceType());
            }
        };

        pathChildrenCache.getListenable().addListener(bucketListener);
        log.info("bucket ownership listener registered on bucket root {}", getServiceType());

        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        } catch (Exception e) {
            log.error("Starting ownership listener for service {} threw exception", getServiceType(), e);
            throw Exceptions.sneakyThrow(e);
        }

    }

    @Override
    public void stopBucketOwnershipListener() {
        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.remove(getServiceType());
        if (pathChildrenCache != null) {
            try {
                pathChildrenCache.clear();
                pathChildrenCache.close();
            } catch (IOException e) {
                log.warn("unable to close listener for bucket ownership", e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> initializeService() {
        return bucketStore.createBucketsRoot(getServiceType());
    }

    @Override
    public CompletableFuture<Void> initializeBucket(int bucket) {
        Preconditions.checkArgument(bucket < bucketStore.getBucketCount(getServiceType()));
        
        return bucketStore.createBucket(getServiceType(), bucket);
    }

    @SneakyThrows(Exception.class)
    @Override
    void addBucketControllerMapListener() {
            NodeCache cache = bucketStore.getBucketControllerMapNodeCache(getServiceType());
            cache.getListenable().addListener(this::manageBuckets);
            log.info("Bucket controller map listener registered for service {}", getServiceType());
            cache.start(true);
            manageBuckets();
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < bucketStore.getBucketCount(getServiceType()));
        return bucketStore.takeBucketOwnership(getServiceType(), bucket, processId);
    }

    @Override
    public void startLeaderElection(BucketManagerLeader bucketManagerLeader) {
        String leaderZKPath = ZKPaths.makePath("cluster", "bucketDistributorLeader");
        leaderSelector = new LeaderSelector(bucketStore.getClient(), leaderZKPath, bucketManagerLeader);
        //Listen for any zookeeper connection state changes
        bucketStore.getClient().getConnectionStateListenable().addListener(
                (curatorClient, newState) -> {
                    switch (newState) {
                        case LOST:
                            log.warn("Connection to zookeeper lost, attempting to interrrupt the leader thread");
                            leaderSelector.interruptLeadership();
                            break;
                        case SUSPENDED:
                            if (leaderSelector.hasLeadership()) {
                                log.info("Zookeeper session suspended, pausing the bucket manager");
                                bucketManagerLeader.suspend();
                            }
                            break;
                        case RECONNECTED:
                            if (leaderSelector.hasLeadership()) {
                                log.info("Zookeeper session reconnected, resume the bucket manager");
                                bucketManagerLeader.resume();
                            }
                            break;
                        //$CASES-OMITTED$
                        default:
                            log.debug("Connection state to zookeeper updated: " + newState.toString());
                    }
                }
        );
    }

    @Override
    public void startLeader() {
        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    @Override
    public void stopLeader() {
        leaderSelector.interruptLeadership();
        leaderSelector.close();
    }
}
