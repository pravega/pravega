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
import io.pravega.controller.util.RetryHelper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
        initializeService().thenAccept(v -> addBucketControllerMapListener())
                           .thenAccept(v -> startLeaderElection())
                           .thenCompose(v -> bucketStore.getBucketsForController(processId, serviceType))
                           .thenCompose(buckets -> Futures.allOf(buckets.stream().map(x -> initializeBucket(x)
                                                                                .thenCompose(v -> tryTakeOwnership(x)))
                                                                        .collect(Collectors.toList())))
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

    /**
     * This method will invoke whenever there is any change in buckets controller mapping.
     * If new instance of controller is added, then existing controllers need to release the buckets.
     * If any instance of controller failed, then existing controllers need to take the ownership of failed controller's buckets.
     *
     * @param numberOfControllers Number of controllers.
     * @return Completable future which when complete indicate that controller manages buckets according to new mapping.
     */
    protected CompletableFuture<Void> manageBuckets(int numberOfControllers) {
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
                              log.info("{}: Buckets added to process : {} are {}.", serviceType, processId, addBuckets);
                              log.info("{}: Total number of controllers is {}.", serviceType, numberOfControllers);
                              return Futures.allOf(addBuckets.stream().map(x -> initializeBucket(x)
                                                                     .thenCompose(v -> tryTakeOwnership(x)))
                                                             .collect(Collectors.toList()));
                          })
                          .thenAccept(v -> {
                              if (removeableBuckets.get().size() > 0) {
                                  stopBucketServices(removeableBuckets.get());
                              }
                              log.info("{}: Buckets removed from process : {} are {}.", serviceType, processId, removeableBuckets.get());
                          });
    }

    CompletableFuture<Void> tryTakeOwnership(int bucket) {
        return RetryHelper.withIndefiniteRetriesAsync(() -> takeBucketOwnership(bucket),
                e -> log.warn("{}: exception while attempting to take ownership for bucket {}: {}.", getServiceType(),
                        bucket, e.getMessage()), getExecutor());
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
        log.info("{}: Stop request received for bucket manager.", serviceType);
        Set<Integer> tmp;
        synchronized (lock) { 
            tmp = buckets.keySet();
        }
        stopBucketServices(tmp);
        stopLeader();
    }

    /**
     * This method will stop the running bucket services.
     * Once the bucket services stopped successfully then buckets becomes available and others process can acquire the ownership of it.
     *
     * @param bucketIds set of bucket ids which need to be stopped.
     */
    public void stopBucketServices(Set<Integer> bucketIds) {
        Futures.allOf(bucketIds.stream().map(bucketId ->
                       RetryHelper.withIndefiniteRetriesAsync(() -> releaseBucketOwnership(bucketId),
                               e -> log.warn("{}: exception while attempting to release ownership for bucket {}: {}.",
                                       getServiceType(), bucketId, e.getMessage()), getExecutor())

               ).collect(Collectors.toList()))
               .whenComplete((r, e) -> {
                   if (e != null) {
                       log.error("{}: bucket service shutdown failed with exception.", serviceType, e);
                       notifyFailed(e);
                   } else {
                       log.info("{}: bucket service stopped.", serviceType);
                       notifyStopped();
                   }
               });
    }

    /**
     * Method used to start running bucket service on a controller instance.
     *
     * @param bucket Bucket id need to be start.
     * @return Future which when complete indicate that bucket service started successfully.
     */
    private CompletableFuture<Void> takeBucketOwnership(int bucket) {
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
    }

    /**
     * Method used to release running bucket service from a controller instance.
     *
     * @param bucket Bucket id need to be release.
     * @return Future which when complete indicate that bucket service release successfully.
     */
    private CompletableFuture<Void> releaseBucketOwnership(int bucket) {
        BucketService bucketService;
        synchronized (lock) {
            bucketService = buckets.get(bucket);
        }
        CompletableFuture<Void> bucketFuture = new CompletableFuture<>();
        bucketService.addListener(new Listener() {
            @Override
            public void terminated(State from) {
                super.terminated(from);
                log.warn("{}: Bucket service {} stopped successfully.", serviceType, bucket);
                synchronized (lock) {
                    buckets.remove(bucket);
                    log.info("{}: New buckets size is {}.", serviceType, buckets.size());
                }
                bucketFuture.complete(null);
            }

            @Override
            public void failed(State from, Throwable failure) {
                super.failed(from, failure);
                log.warn("{}: Unable to stop bucket service {} due to {}.", serviceType, bucket, failure);
                bucketFuture.completeExceptionally(failure);
            }
        }, executor);
        bucketService.stopAsync();
        return bucketFuture;
    }

    abstract CompletableFuture<Void> initializeService();
    
    abstract CompletableFuture<Void> initializeBucket(int bucket);

    abstract void addBucketControllerMapListener();

    @VisibleForTesting
    Map<Integer, BucketService> getBucketServices() {
        return Collections.unmodifiableMap(buckets);
    }
}
