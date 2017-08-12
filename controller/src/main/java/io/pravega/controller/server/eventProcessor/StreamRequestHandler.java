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
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.shared.controller.event.StreamEvent;
import lombok.Data;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Stream Request handler class is used to serialize requests for a stream and process them.
 * It maintains a map of stream and its work queue.
 * Any new request received for the stream is queued up if queue is not empty. If stream worker is not present,
 * a new queue is created and the event is put in the queue and this is added to the worker map.
 * The processing is then scheduled asynchronously for the stream.
 * <p>
 * The processing for a stream ends and the stream is removed from the work map the moment its queue becomes empty.
 */
public abstract class StreamRequestHandler<T extends StreamEvent> implements RequestHandler<T> {

    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Map<String, ConcurrentLinkedQueue<Work>> workers = new HashMap<>();

    @Override
    public CompletableFuture<Void> process(final T streamEvent, final EventProcessor.Writer<T> writer) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Work work = new Work(streamEvent, writer, result);
        String scopedStreamName = streamEvent.getScopedStreamName();

        synchronized (lock) {
            if (workers.containsKey(scopedStreamName)) {
                workers.get(scopedStreamName).add(work);
            } else {
                ConcurrentLinkedQueue<Work> queue = new ConcurrentLinkedQueue<>();
                queue.add(work);
                workers.put(scopedStreamName, queue);
                // Start work processing asyncrhonously and release the lock.
                CompletableFuture.runAsync(() -> run(scopedStreamName, queue));
            }
        }
        return result;
    }

    abstract CompletableFuture<Void> processEvent(final T event, final EventProcessor.Writer<T> writer);

    private CompletableFuture<Void> process(Work work) {
        return processEvent(work.getEvent(), work.getWriter()).whenComplete((r, e) -> {
            if (e != null) {
                work.getResult().completeExceptionally(e);
            } else {
                work.getResult().complete(r);
            }
        });
    }

    private void run(String scopedStreamName, ConcurrentLinkedQueue<Work> workQueue) {
        Work work = workQueue.poll();
        process(work).whenComplete((r, e) -> {
            synchronized (lock) {
                if (workQueue.isEmpty()) {
                    workers.remove(scopedStreamName);
                } else {
                    CompletableFuture.runAsync(() -> run(scopedStreamName, workQueue));
                }
            }
        });
    }

    @VisibleForTesting
    List<Pair<T, CompletableFuture<Void>>> getEventQueueForStream(String scope, String stream) {
        String scopedStreamName = String.format("%s/%s", scope, stream);

        synchronized (lock) {
            if (workers.containsKey(scopedStreamName)) {
                return workers.get(scopedStreamName).stream().map(x -> new ImmutablePair<>(x.getEvent(), x.getResult())).collect(Collectors.toList());
            } else {
                return null;
            }
        }
    }

    @Data
    private class Work {
        private final T event;
        private final EventProcessor.Writer<T> writer;
        private final CompletableFuture<Void> result;
    }
}
