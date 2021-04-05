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
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.Data;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Resource pool class implements functionality for creating and maintaining a pool of reusable resources. 
 * It manages the lifecycle of underlying resources that it creates and destroys while ensuring that it enforces a ceiling on
 * maximum concurrent resources and maximum idle resources. 
 * Users can request for new resource from this class and it will opportunistically use existing resource or create new 
 * resource and complete the request. 
 * It is callers responsibility to call close on the resource wrapper that is returned by the pool. 
 * Upon invoking close, a resource is automatically returned to the pool once its usage is done. 
 * If more resources are requested than the maximum concurrent allowed resource count, then this class will add them to 
 * a wait queue and as resources are returned to the pool, the waiting requests are fulfilled. 
 * If a returned resource is invalid, it is destroyed and a replacement resource is created to fulfill waiting requests. 
 * If there are no waiting requests, this class tries to maintain an idle pool of resources of maxidleSize. 
 * Once maximum allowed idle resources are present with it, any additional resource returned to it is discarded.    
 * 
 * The ResourcePool can be shutdown. However, the shutdown trigger does not prevent callers to attempt 
 * to create new resources as long as they have a valid handle to the resource pool. 
 * Shutdown ensures that it drains all idle resources once shutdown is triggered.  
 */
public class ResourcePool<T> {
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final ArrayDeque<T> idleResources;
    @GuardedBy("lock")
    private boolean isRunning;
    @GuardedBy("lock")
    private final ArrayDeque<WaitingRequest<T>> waitQueue;
    @GuardedBy("lock")
    private int resourceCount;
    private final int maxConcurrent;
    private final int maxIdle;

    private final Listener listener;
    private final Supplier<CompletableFuture<T>> tSupplier;
    private final Consumer<T> tDestroyer;

    public ResourcePool(Supplier<CompletableFuture<T>> tSupplier, Consumer<T> tDestroyer, int maxConcurrent, int maxIdle) {
        this(tSupplier, tDestroyer, maxConcurrent, maxIdle, null);
    }

    @VisibleForTesting
    ResourcePool(Supplier<CompletableFuture<T>> tSupplier, Consumer<T> tDestroyer,
                 int maxConcurrent, int maxIdle, Listener listener) {
        Preconditions.checkNotNull(tSupplier);
        Preconditions.checkNotNull(tDestroyer);
        Preconditions.checkArgument(maxConcurrent >= maxIdle);
        Preconditions.checkArgument(maxIdle >= 0);
        this.idleResources = new ArrayDeque<>();
        this.isRunning = true;
        this.waitQueue = new ArrayDeque<>();
        this.resourceCount = 0;
        this.maxConcurrent = maxConcurrent;
        this.maxIdle = maxIdle;
        this.listener = listener;
        this.tSupplier = tSupplier;
        this.tDestroyer = tDestroyer;
    }

    /**
     * Method to get a resource initialized with supplied arg.
     * This method attempts to find an existing available resource.
     * If not found, it submits a new waiting request for whenever a resource becomes available. A resource could become available
     * because such a resource was returned to the pool or a new resource was created.
     * It also opportunistically submits a request to create a new resource if required.
     *
     * @return A completableFuture which when completed will have the resource object that the caller requested.
     */
    public CompletableFuture<CloseableResource<T>> getResource() {
        CompletableFuture<CloseableResource<T>> future;
        boolean tryCreateNewResource = false;
        synchronized (lock) {
            T t = idleResources.poll();
            if (t != null) {
                // return the object from the queue
                future = CompletableFuture.completedFuture(new CloseableResource<>(t, this));
            } else {
                future = new CompletableFuture<>();
                WaitingRequest<T> request = new WaitingRequest<>(future);
                waitQueue.add(request);
                tryCreateNewResource = true;
            }
        }

        if (tryCreateNewResource) {
            tryCreateNewResource();
        }

        return future;
    }
    
    /**
     * Method to return resource back to the pool. Callers are expected to call close on the closableResource which in turn
     * calls the return resource method to return the resource to the pool so that it can be reused. 
     * 
     * @param t resource to be returned
     * @param isValid is resource valid
     */
    private void returnResource(T t, boolean isValid) {
        if (!isValid) {
            handleInvalid(t);
        } else {
            WaitingRequest<T> waiting;
            boolean toDestroy = false;
            synchronized (lock) {
                waiting = waitQueue.poll();
                if (waiting == null) {
                    if (!isRunning) {
                        // The resource will be closed if returned anytime after the shutdown has been initiated.
                        resourceCount--;
                        toDestroy = true;
                    } else {
                        // as resources are returned to us, we put them in queue to be reused
                        // if returned resource increases our idle resource count, do not include it
                        if (idleResources.size() < maxIdle) {
                            idleResources.offer(t);
                        } else {
                            resourceCount--;
                            toDestroy = true;
                        }
                    }
                }
            }

            if (waiting != null) {
                waiting.future.complete(new CloseableResource<>(t, this));
            }

            if (toDestroy) {
                if (listener != null) {
                    listener.notify(Event.Destroyed);
                }
                tDestroyer.accept(t);
            }
        }
    }

    private void tryCreateNewResource() {
        WaitingRequest<T> waiting;
        synchronized (lock) {
            if (resourceCount < maxConcurrent) {
                waiting = waitQueue.poll();
                if (waiting != null) {
                    resourceCount++;
                }
            } else {
                waiting = null;
            }
        }

        if (waiting != null) {
            try {
                tSupplier.get().whenComplete((t, e) -> {
                    if (e != null) {
                        waiting.future.completeExceptionally(e);
                    } else {
                        if (listener != null) {
                            listener.notify(Event.Created);
                        }
                        waiting.future.complete(new CloseableResource<>(t, this));
                    }
                });
            } catch (Throwable e) {
                // synchronous failure from supplier. we will fail the waiting request with it
                waiting.future.completeExceptionally(e);
            }
        }
    }

    private void handleInvalid(T t) {
        tDestroyer.accept(t);
        if (listener != null) {
            listener.notify(Event.Destroyed);
        }
        boolean tryCreateNewresource;
        synchronized (lock) {
            resourceCount--;
            tryCreateNewresource = !waitQueue.isEmpty();
        }

        if (tryCreateNewresource) {
            tryCreateNewResource();
        }
    }

    // region for testing
    @VisibleForTesting
    int resourceCount() {
        synchronized (lock) {
            return resourceCount;
        }
    }

    @VisibleForTesting
    int idleCount() {
        synchronized (lock) {
            return idleResources.size();
        }
    }

    @VisibleForTesting
    int waitingCount() {
        synchronized (lock) {
            return waitQueue.size();
        }
    }

    @VisibleForTesting
    static class Listener {
        private final LinkedBlockingQueue<Event> eventQueue;

        Listener(LinkedBlockingQueue<Event> eventQueue) {
            Preconditions.checkNotNull(eventQueue);
            this.eventQueue = eventQueue;
        }

        public void notify(Event event) {
            eventQueue.offer(event);
        }
    }
    // endregion

    /**
     * Shutdown the resource manager where all returned resources are closed and not put back into the
     * idle queue of resources.
     * It is important to note that even after shutdown is initiated, if `getresource` is invoked, it will return a resource.
     */
    public void shutdown() {
        // as resources are returned we need to shut them down
        T t;
        synchronized (lock) {
            isRunning = false;
            t = idleResources.poll();
        }
        while (t != null) {
            returnResource(t, true);
            synchronized (lock) {
                t = idleResources.poll();
            }
        }
    }

    enum Event {
        Created,
        Destroyed
    }

    @Data
    private static class WaitingRequest<T> {
        private final CompletableFuture<CloseableResource<T>> future;
    }

    /**
     * A closeable resource wrapper class which returns the resource back to the pool automatically once it is closed.
     * Its close method is idempotent and can be invoked multiple times without returning the resource more than once. 
     * @param <T> Type of underlying resource
     */
    public static class CloseableResource<T> implements AutoCloseable {
        private final ResourcePool<T> resourcePool;
        private final T resource;
        private final AtomicBoolean invalid;
        private AtomicBoolean isClosed;
        private CloseableResource(T resource, ResourcePool<T> resourcePool) {
            this.resourcePool = resourcePool;
            this.resource = resource;
            this.invalid = new AtomicBoolean(false);
            this.isClosed = new AtomicBoolean(false);
        }

        public T getResource() {
            return resource;
        }
        
        public void invalidate() {
            invalid.set(true);
        }

        @Override
        public void close() {
            // Close is idempotent. 
            // If close had already been invoked on this resource wrapper, then we do not return the resource to the pool.
            if (isClosed.compareAndSet(false, true)) {
                resourcePool.returnResource(resource, !invalid.get());
            }
        }
    }
}
