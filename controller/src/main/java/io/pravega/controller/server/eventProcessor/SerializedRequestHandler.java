/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
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
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * SerializedRequestHandler class is used to serialize requests for a key and process them.
 * It maintains a map of key and its work queue.
 * Any new request received for the key is queued up if queue is not empty. If a worker queue for the key is not found in map,
 * a new queue is created and the event is put in the queue and this is added to the worker map.
 * The processing is then scheduled asynchronously for the key.
 * <p>
 * Once all pending processing for a key ends, the key is removed from the work map the moment its queue becomes empty.
 */
@AllArgsConstructor
public abstract class SerializedRequestHandler<T extends ControllerEvent> implements RequestHandler<T> {

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<String, ConcurrentLinkedQueue<Work>> workers = new HashMap<>();

    private final ExecutorService executor;

    @Override
    public CompletableFuture<Void> process(final T streamEvent, final EventProcessor.Writer<T> writer) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Work work = new Work(streamEvent, writer, result);
        String key = streamEvent.getKey();

        synchronized (lock) {
            if (workers.containsKey(key)) {
                workers.get(key).add(work);
            } else {
                ConcurrentLinkedQueue<Work> queue = new ConcurrentLinkedQueue<>();
                queue.add(work);
                workers.put(key, queue);
                // Start work processing asynchronously and release the lock.
                executor.execute(() -> run(key, queue));
            }
        }
        return result;
    }

    abstract CompletableFuture<Void> processEvent(final T event, final EventProcessor.Writer<T> writer);

    /**
     * Run method is called only if work queue is non empty. So we can safely do a workQueue.poll.
     * WorkQueue.poll should only happen in the run method and no where else.
     * @param key key for which we want to process the next event
     * @param workQueue work queue for the key
     */
    private void run(String key, ConcurrentLinkedQueue<Work> workQueue) {
        Work work = workQueue.poll();
        FutureHelpers.completeAfter(() -> processEvent(work.getEvent(), work.getWriter()), work.getResult());
        work.getResult().whenComplete((r, e) -> {
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
        private final EventProcessor.Writer<T> writer;
        private final CompletableFuture<Void> result;
    }
}
