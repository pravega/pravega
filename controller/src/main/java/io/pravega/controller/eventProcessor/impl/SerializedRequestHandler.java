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
package io.pravega.controller.eventProcessor.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.RequestHandler;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * SerializedRequestHandler class is used to serialize requests for a key and process them.
 * It maintains a map of key and its work queue.
 * Any new request received for the key is queued up if queue is not empty. If a worker queue for the key is not found in map,
 * a new queue is created and the event is put in the queue and this is added to the worker map.
 * The processing is then scheduled asynchronously for the key.
 *
 * Once all pending processing for a key ends, the key is removed from the work map the moment its queue becomes empty.
 */
@AllArgsConstructor
public abstract class SerializedRequestHandler<T extends ControllerEvent> implements RequestHandler<T> {

    protected final ScheduledExecutorService executor;

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<String, ConcurrentLinkedQueue<Work>> workers = new HashMap<>();

    @Override
    public final CompletableFuture<Void> process(final T streamEvent, Supplier<Boolean> isCancelled) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Work work = new Work(streamEvent, System.currentTimeMillis(), result, isCancelled);
        String key = streamEvent.getKey();

        final ConcurrentLinkedQueue<Work> queue;

        synchronized (lock) {
            if (workers.containsKey(key)) {
                workers.get(key).add(work);
                queue = null;
            } else {
                queue = new ConcurrentLinkedQueue<>();
                queue.add(work);
                workers.put(key, queue);
            }
        }

        if (queue != null) {
            executor.execute(() -> run(key, queue));
        }

        return result;
    }

    public abstract CompletableFuture<Void> processEvent(final T event);

    public boolean toPostpone(final T event, final long pickupTime, final Throwable exception) {
        return false;
    }


    /**
     * Run method is called only if work queue is not empty. So we can safely do a workQueue.poll.
     * WorkQueue.poll should only happen in the run method and no where else.
     *
     * @param key       key for which we want to process the next event
     * @param workQueue work queue for the key
     */
    private void run(String key, ConcurrentLinkedQueue<Work> workQueue) {
        Work work = workQueue.poll();
        CompletableFuture<Void> future;
        try {
            assert work != null;
            if (!work.getCancelledSupplier().get()) {
                future = processEvent(work.getEvent());
            } else {
                future = new CompletableFuture<>();
                future.cancel(true);
            }
        } catch (Exception e) {
            future = Futures.failedFuture(e);
        }

        future.whenComplete((r, e) -> {
            if (e != null && toPostpone(work.getEvent(), work.getPickupTime(), e)) {
                handleWorkPostpone(key, workQueue, work);
            } else {
                if (e != null) {
                    work.getResult().completeExceptionally(e);
                } else {
                    work.getResult().complete(r);
                }

                handleWorkComplete(key, workQueue, work);
            }
        });
    }

    private void handleWorkPostpone(String key, ConcurrentLinkedQueue<Work> workQueue, Work work) {
        // if the request handler decides to postpone the processing,
        // put the work at the back of the queue to be picked again.
        // Note: we have not completed the work's result future here.
        // Since there is at least one event in the queue (we just
        // added) so we will call run again.
        synchronized (lock) {
            workers.get(key).add(work);
        }

        executor.execute(() -> run(key, workQueue));
    }

    private void handleWorkComplete(String key, ConcurrentLinkedQueue<Work> workQueue, Work work) {
        work.getResult().whenComplete((rw, ew) -> {
            boolean toExecute = false;
            synchronized (lock) {
                if (workQueue.isEmpty()) {
                    workers.remove(key);
                } else {
                    toExecute = true;
                }
            }

            if (toExecute) {
                executor.execute(() -> run(key, workQueue));
            }
        });
    }

    @VisibleForTesting
    List<Pair<T, CompletableFuture<Void>>> getEventQueueForKey(String key) {
        List<Pair<T, CompletableFuture<Void>>> retVal = null;

        synchronized (lock) {
            if (workers.containsKey(key)) {
                retVal = workers.get(key).stream().map(x -> new ImmutablePair<>(x.getEvent(), x.getResult())).collect(Collectors.toList());
            }
        }

        return retVal;
    }

    @Data
    private class Work {
        private final T event;
        private final long pickupTime;
        private final CompletableFuture<Void> result;
        private final Supplier<Boolean> cancelledSupplier;
    }

}
