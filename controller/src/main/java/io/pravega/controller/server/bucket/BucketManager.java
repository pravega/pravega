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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.util.RetryHelper;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
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
    private static final int NO_OF_RETRIES = 10;
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

    BucketManager(final String processId, final BucketStore.ServiceType serviceType, final ScheduledExecutorService executor,
                  final Function<Integer, BucketService> bucketServiceSupplier, final BucketStore bucketStore) {
        this.processId = processId;
        this.serviceType = serviceType;
        this.executor = executor;
        this.lock = new Object();
        this.buckets = new HashMap<>();
        this.bucketServiceSupplier = bucketServiceSupplier;
        this.bucketStore = bucketStore;
    }

    @Override
    protected void doStart() {
        initializeService().thenAccept(v -> startAllListeners())
                           .thenCompose(v -> bucketStore.getBucketsForController(processId, serviceType))
                           .thenCompose(buckets -> Futures.allOf(buckets.stream().map(x -> initializeBucket(x)
                                                                                .thenCompose(v -> startBucketService(x)))
                                                                        .collect(Collectors.toList())))
                           .whenComplete((r, e) -> {
                               if (e != null) {
                                   notifyFailed(e);
                                   log.error("Unable to start bucket manager", e);
                               } else {
                                   notifyStarted();
                                   log.info("Bucket manager started successfully");
                               }
                           });
    }

    protected abstract int getBucketCount();

    public abstract boolean isHealthy();

    public abstract void startLeaderElection();

    public abstract void startLeader();

    public abstract void stopLeader();

    /**
     * This method will invoke whenever there is any change in buckets controller mapping.
     * If new instance of controller is added, then existing controllers need to release the buckets.
     * If any instance of controller failed, then existing controllers need to take the ownership of newly assigned buckets.
     *
     * @return Completable future which when complete indicate that controller manages buckets according to new mapping.
     */
    protected CompletableFuture<Void> manageBuckets() {
        return bucketStore.getBucketsForController(processId, getServiceType())
                          .thenCompose(newBuckets -> {
                              Set<Integer> addBuckets, removableBuckets;
                              synchronized (lock) {
                                  addBuckets = newBuckets.stream().filter(x -> !buckets.containsKey(x))
                                                         .collect(Collectors.toSet());
                                  removableBuckets = buckets.keySet().stream().filter(x -> !newBuckets.contains(x))
                                                            .collect(Collectors.toSet());
                              }
                              log.info("{}: Buckets added to process : {} are {}.", serviceType, processId, addBuckets);
                              log.info("{}: Buckets removed from process : {} are {}.", serviceType, processId,
                                      removableBuckets);
                              Preconditions.checkArgument(Collections.disjoint(addBuckets, removableBuckets),
                                      "Invalid distribution of buckets");
                              return Futures.allOf(addBuckets.stream().map(x -> initializeBucket(x)
                                                                     .thenCompose(v -> startBucketService(x)))
                                                             .collect(Collectors.toList()))
                                            .thenAccept(v -> {
                                                if (!removableBuckets.isEmpty()) {
                                                    stopBucketServices(removableBuckets, false);
                                                }
                                            });
                          });
    }

    CompletableFuture<Void> tryTakeOwnership(int bucket) {
        return takeBucketOwnership(bucket, processId, executor)
                         .thenCompose(isOwner -> {
                    if (isOwner) {
                        log.info("{}: Taken ownership for bucket {}.", serviceType, bucket);

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
                        log.warn("{}: Unable to take ownership of bucket {} because another instance doesn't release it.",
                                serviceType, bucket);
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
                log.info("{}: successfully started bucket service bucket: {}.", BucketManager.this.serviceType, bucket);
                bucketFuture.complete(null);
            }

            @Override
            public void failed(State from, Throwable failure) {
                super.failed(from, failure);
                log.error("{}: Failed to start bucket: {}.", BucketManager.this.serviceType, bucket);
                if (from == State.STARTING) {
                    synchronized (lock) {
                        buckets.remove(bucket);
                    }
                    bucketFuture.completeExceptionally(failure);
                }
            }
        }, executor);
        bucketService.startAsync();
        return bucketService;
    }

    @Override
    protected void doStop() {
        log.info("{}: Stop request received for bucket manager.", serviceType);
        Set<Integer> tmp;
        synchronized (lock) { 
            tmp = new HashSet<>(buckets.keySet());
        }
        stopBucketServices(tmp, true);
        stopBucketOwnershipListener();
        stopBucketControllerMapListener();
        stopLeader();
    }

    /**
     * This method will stop the running bucket services.
     * Once the services stopped successfully then buckets becomes available and others process can acquire the ownership of it.
     *
     * @param bucketIds set of bucket ids which need to be stopped.
     * @param notify    if true will notify that abstract service stopped successfully or not.
     */
    public void stopBucketServices(Set<Integer> bucketIds, boolean notify) {
        Futures.allOf(bucketIds.stream().map(this::stopBucketService)
                               .collect(Collectors.toList()))
               .whenComplete((r, e) -> {
                   if (e != null) {
                       log.error("{}: bucket service shutdown failed with exception.", serviceType, e);
                       if (notify) {
                           notifyFailed(e);
                       }
                   } else {
                       log.info("{}: bucket service stopped.", serviceType);
                       if (notify) {
                           notifyStopped();
                       }
                   }
               });
    }

    private CompletableFuture<Void> stopBucketService(Integer bucketId) {
        BucketService bucketService;
        synchronized (lock) {
            bucketService = buckets.get(bucketId);
        }
        CompletableFuture<Void> bucketFuture = new CompletableFuture<>();
        bucketService.addListener(new Listener() {
            @Override
            public void terminated(State from) {
                super.terminated(from);
                synchronized (lock) {
                    buckets.remove(bucketId);
                    log.info("{}: Bucket service {} stopped.", serviceType, bucketId);
                }
                releaseBucketOwnership(bucketId, processId).thenCompose(released -> {
                    if (!released) {
                        log.warn("{}: Unable to stop bucket service {} because it doesn't release it's ownership.",
                                serviceType, bucketId);
                        return startBucketService(bucketId);
                    }
                    return CompletableFuture.completedFuture(null);
                }).whenComplete((r, e) -> {
                    if (e == null) {
                        bucketFuture.complete(null);
                    } else {
                        //if unable to stop the service, reacquire the ownership of bucket service.
                        log.warn("{}: Reacquiring the ownership of bucket service {}.", serviceType, bucketId);
                        startBucketService(bucketId).whenComplete((result, exception) -> {
                            log.info("{} : Trying to restart the service {}.", getServiceType(), bucketId);
                        });
                        bucketFuture.completeExceptionally(e);
                    }
                });
            }

            @Override
            public void failed(State from, Throwable failure) {
                super.failed(from, failure);
                log.warn("{}: Unable to stop bucket service {} due to {}.", serviceType, bucketId, failure);
                bucketFuture.completeExceptionally(failure);
            }
        }, executor);
        bucketService.stopAsync();
        return bucketFuture;
    }

    abstract void startBucketOwnershipListener();

    abstract void stopBucketOwnershipListener();

    abstract CompletableFuture<Void> initializeService();
    
    abstract CompletableFuture<Void> initializeBucket(int bucket);

    abstract void startBucketControllerMapListener();

    abstract void stopBucketControllerMapListener();

    /**
     * Method to release bucket ownership.
     *
     * @param bucket        bucket id.
     * @param processId     process id.
     * @return future, which when completed, will contain a boolean which tells if ownership release succeeded or failed.
     */
    abstract CompletableFuture<Boolean> releaseBucketOwnership(int bucket, String processId);

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

    /**
     * Method to start all listener required for bucket manager.
     */
    private void startAllListeners() {
        startBucketControllerMapListener();
        startLeaderElection();
        startBucketOwnershipListener();
    }

    /**
     * Start a new bucket service on controller instance. If any exception occurs while starting the bucket, it will
     * retry for 10 times.
     *
     * @param bucketId Id of bucket service.
     * @return future, which when complete indicate that ownership acquired successfully.
     */
    protected CompletableFuture<Void> startBucketService(int bucketId) {
       return RetryHelper.withRetriesAsync(() -> tryTakeOwnership(bucketId),
               RetryHelper.UNCONDITIONAL_PREDICATE, NO_OF_RETRIES, getExecutor());
    }
}
