/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import io.pravega.common.AbstractTimer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Keeps track of the duration of arbitrary async operations and provides methods to query the ones that are incomplete.
 */
@ThreadSafe
class TaskTracker {
    //region Members

    private final AbstractTimer timer;
    private final ConcurrentHashMap<Integer, TrackTarget> pending;
    private final AtomicInteger trackId;
    private final Duration createTime;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link TaskTracker} class.
     *
     * @param timer A {@link AbstractTimer} that can be used to provide the current time.
     */
    TaskTracker(@NonNull AbstractTimer timer) {
        this.timer = timer;
        this.pending = new ConcurrentHashMap<>();
        this.trackId = new AtomicInteger();
        this.createTime = timer.getElapsed();
    }

    //endregion

    //region Operations

    /**
     * Gets a {@link Duration} that expresses the amount of time elapsed since the creation of this tracker.
     *
     * @return a {@link Duration}.
     */
    public Duration getElapsedSinceCreation() {
        return this.timer.getElapsed().minus(this.createTime);
    }

    /**
     * Gets a {@link Duration} that expresses the highest amount of time elapsed for any pending task. If no tasks are
     * pending, this will return {@link Duration#ZERO}.
     *
     * @return A {@link Duration}.
     */
    public Duration getLongestPendingDuration() {
        long max = 0;
        long currentTime = this.timer.getElapsedNanos();
        for (val t : this.pending.values()) {
            max = Math.max(currentTime - t.getStartTimeNanos(), max);
        }

        return Duration.ofNanos(max);
    }

    /**
     * Gets an unordered collection of {@link PendingTask} instances representing all the tasks that haven't yet
     * been completed.
     *
     * @return a Collection of {@link PendingTask} for all invocations of {@link #track} that have not yet completed.
     */
    public Collection<PendingTask> getAllPending() {
        long currentTime = this.timer.getElapsedNanos();
        return this.pending.values().stream()
                .map(t -> new PendingTask(Duration.ofNanos(currentTime - t.getStartTimeNanos()), t.getArgs()))
                .collect(Collectors.toList());
    }

    /**
     * Tracks the async execution of a task.
     *
     * @param taskSupplier A {@link Supplier} that, when invoked, will return a {@link CompletableFuture} which can be
     *                     used to track the outcome of a task. This task will be added to the list of pending tasks
     *                     and will be removed when it completes, successfully or exceptionally.
     * @param context      Any context relating to this task. These args will be part of the {@link PendingTask} that
     *                     is associated with this task (when invoking {@link #getAllPending()}.
     * @param <T>          Return type.
     * @return The result of {@link Supplier}.
     */
    public <T> CompletableFuture<T> track(Supplier<CompletableFuture<T>> taskSupplier, Object... context) {
        val target = new TrackTarget(this.trackId.getAndIncrement(), this.timer.getElapsedNanos(), context);
        val previous = this.pending.put(target.getId(), target);
        assert previous == null;
        try {
            CompletableFuture<T> result = taskSupplier.get();
            result.whenComplete((r, ex) -> complete(target.getId()));
            return result;
        } catch (Throwable ex) {
            complete(target.getId());
            throw ex;
        }
    }

    private void complete(int id) {
        TrackTarget t = this.pending.remove(id);
        assert t != null;
    }

    //endregion

    //region Helper Classes

    @Data
    private static class TrackTarget {
        private final int id;
        private final long startTimeNanos;
        private final Object[] args;
    }

    /**
     * A Task that has begun executing but not yet completed.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    public static class PendingTask {
        /**
         * Elapsed time since execution began.
         */
        private final Duration elapsed;
        /**
         * Any context that was provided with the execution of this Task.
         */
        private final Object[] context;

        @Override
        public String toString() {
            return String.format("Elapsed = %s ms, Context = [%s]", this.elapsed.toMillis(),
                    Arrays.stream(this.context).map(Object::toString).collect(Collectors.joining(", ")));
        }
    }

    //endregion
}
