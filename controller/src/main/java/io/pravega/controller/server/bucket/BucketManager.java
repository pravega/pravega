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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is central to bucket management. This is instantiated once per service type. It is responsible for acquiring
 * ownership of buckets for the given service and then starting one bucket service instance for each bucket it owns. 
 * It manages the lifecycle of bucket service.
 * The implementation of this class implements store specific monitoring for bucket ownership changes.
 * This service does not have any internally managed threads. Upon receiving any new notification, it submits a "tryTakeOwnership" 
 * work in the executor's queue. 
 */
@Slf4j
public abstract class BucketManager extends AbstractService {
    private final String processId;
    @Getter(AccessLevel.PROTECTED)
    private final BucketStore.ServiceType serviceType;
    private final Function<Integer, BucketService> bucketServiceSupplier;
    private final Object lock;
    @GuardedBy("lock")
    private final Map<Integer, BucketService> buckets;
    @Getter(AccessLevel.PROTECTED)
    private final ScheduledExecutorService executor;
    private final BucketStore bucketStore;
    /**
     * The tests can add listeners to get notification when the update has happened in the store.
     */
    @Getter
    private final AtomicReference<BucketListener> listenerRef;
    BucketManager(final String processId, final BucketStore.ServiceType serviceType, final ScheduledExecutorService executor,
                  final Function<Integer, BucketService> bucketServiceSupplier, final BucketStore bucketStore) {
        this.processId = processId;
        this.serviceType = serviceType;
        this.executor = executor;
        this.lock = new Object();
        this.buckets = new HashMap<>();
        this.bucketServiceSupplier = bucketServiceSupplier;
        this.bucketStore = bucketStore;
        listenerRef = new AtomicReference<>();
    }

    @Override
    protected void doStart() {
        initializeService().thenAccept(v -> addBucketControllerMapListener())
                           .thenAccept(v -> startLeaderElection())
                           .thenCompose(v -> bucketStore.getBucketsForController(processId, serviceType))
                           .thenCompose(buckets -> Futures.allOf(buckets.stream().map(x -> initializeBucket(x)
                                                                                .thenCompose(v -> tryTakeOwnership(x)))
                                                                        .collect(Collectors.toList())))
                           .thenAccept(x -> startBucketOwnershipListener())
                           .whenComplete((r, e) -> {
                               if (e != null) {
                                   notifyFailed(e);
                               } else {
                                   notifyStarted();
                               }
                           });
    }

    protected abstract int getBucketCount();

    public abstract boolean isHealthy();

    public abstract void startLeaderElection();

    public abstract void startLeader();

    public abstract void stopLeader();

    protected CompletableFuture<Void> manageBuckets(int noOfClusters) {
        AtomicReference<Set<Integer>> removeableBuckets = new AtomicReference<>();
        return bucketStore.getBucketsForController(processId, getServiceType())
                          .thenCompose(newBuckets -> {
                              Set<Integer> addBuckets;
                              synchronized (lock) {
                                  addBuckets = newBuckets.stream().filter(x -> !buckets.containsKey(x))
                                                         .collect(Collectors.toSet());
                                  removeableBuckets.set(buckets.keySet().stream().filter(x -> !newBuckets.contains(x))
                                                               .collect(Collectors.toSet()));
                              }
                              log.debug("{} : Buckets added to process : {} are {}", serviceType, processId, addBuckets);
                              log.debug("{} : Total no of controllers are {}", serviceType, noOfClusters);
                              //If there is only one controller instance, then will try to take ownership from here as there
                              //controller will not wait for stopping the service from other instance.
                              if (noOfClusters == 1) {
                                  return Futures.allOf(addBuckets.stream()
                                                                 .map(x -> initializeBucket(x).thenCompose(v -> tryTakeOwnership(x)))
                                                                 .collect(Collectors.toList()));
                              } else {
                                  return CompletableFuture.completedFuture(null);
                              }
                          })
                          .thenAccept(v -> {
                              if (removeableBuckets.get().size() > 0) {
                                  stopBucketServices(removeableBuckets.get());
                              }
                              log.debug("{} : Buckets remove from process : {} are {} for ", serviceType, processId, removeableBuckets.get());
                          });
    }

    CompletableFuture<Void> tryTakeOwnership(int bucket) {
        return takeBucketOwnership(bucket, processId, executor)
                         .thenCompose(isOwner -> {
                    if (isOwner) {
                        log.info("{}: Taken ownership for bucket {}", serviceType, bucket);

                        // Once we have taken ownership of the bucket, we will register listeners on the bucket. 
                        CompletableFuture<Void> bucketFuture = new CompletableFuture<>();

                        synchronized (lock) {
                            if (buckets.containsKey(bucket)) {
                                bucketFuture.complete(null);
                            } else {
                                BucketService bucketService = startNewBucketService(bucket, bucketFuture);
                                buckets.put(bucket, bucketService);
                            }
                        }
                        
                        return bucketFuture;
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private BucketService startNewBucketService(int bucket, CompletableFuture<Void> bucketFuture) {
        BucketService bucketService = bucketServiceSupplier.apply(bucket);
        bucketService.addListener(new Listener() {
            @Override
            public void running() {
                super.running();
                log.info("{}: successfully started bucket service bucket: {} ", BucketManager.this.serviceType, bucket);
                bucketFuture.complete(null);
            }

            @Override
            public void failed(State from, Throwable failure) {
                super.failed(from, failure);
                log.error("{}: Failed to start bucket: {} ", BucketManager.this.serviceType, bucket);
                synchronized (lock) {
                    buckets.remove(bucket);
                }
                bucketFuture.completeExceptionally(failure);
            }
        }, executor);
        bucketService.startAsync();
        return bucketService;
    }

    @Override
    protected void doStop() {
        log.info("{}: Stop request received for bucket manager", serviceType);
        Set<Integer> tmp;
        synchronized (lock) { 
            tmp = buckets.keySet();
        }
        stopBucketServices(tmp);
        stopBucketOwnershipListener();
        stopLeader();
    }

    public void stopBucketServices(Set<Integer> bucketIds) {
        Set<Integer> bInt = new HashSet<>();
        Futures.allOf(bucketIds.stream().map(bucketId -> {
                   BucketService bucketService;
                   synchronized (lock) {
                       bucketService = buckets.get(bucketId);
                   }
                   CompletableFuture<Void> bucketFuture = new CompletableFuture<>();
                   bucketService.addListener(new Listener() {
                       @Override
                       public void terminated(State from) {
                           super.terminated(from);
                           bInt.add(bucketId);
                           bucketFuture.complete(null);
                       }

                       @Override
                       public void failed(State from, Throwable failure) {
                           super.failed(from, failure);
                           bucketFuture.completeExceptionally(failure);
                       }
                   }, executor);
                   bucketService.stopAsync();

                   return bucketFuture;
               }).collect(Collectors.toList()))
               .whenComplete((r, e) -> {
                   synchronized (lock) {
                       bInt.stream().forEach(x -> buckets.remove(x));
                   }
                   if (e != null) {
                       log.error("{}: bucket service shutdown failed with exception", serviceType, e);
                       notifyFailed(e);
                   } else {
                       log.info("{}: bucket service stopped", serviceType);
                       notifyStopped();
                   }
               });
    }
    
    abstract void startBucketOwnershipListener();

    abstract void stopBucketOwnershipListener();

    abstract CompletableFuture<Void> initializeService();
    
    abstract CompletableFuture<Void> initializeBucket(int bucket);

    abstract void addBucketControllerMapListener();

    /**
     * Method to take ownership of a bucket.
     *
     * @param bucket      bucket id
     * @param processId   process id
     * @param executor    executor
     * @return future, which when completed, will contain a boolean which tells if ownership attempt succeeded or failed.
     */
    abstract CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, Executor executor);

    @VisibleForTesting
    Map<Integer, BucketService> getBucketServices() {
        return Collections.unmodifiableMap(buckets);
    }

    @VisibleForTesting
    public void addListener(BucketListener listener) {
        this.listenerRef.set(listener);
    }

    /**
     * Functional interface to notify tests about changes to the map as they occur.
     */
    @VisibleForTesting
    @FunctionalInterface
    public interface BucketListener {
        void signal();
    }
}
