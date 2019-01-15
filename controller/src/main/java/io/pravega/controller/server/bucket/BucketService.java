/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.BucketStore;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * This class represents an instance of a background worker service that runs background work for all streams under a 
 * specific bucket for a specific service type. This means a separate instance of this class is instantiated for each 
 * bucket for each service type.   
 * Each object has two dedicated threads, notification loop and worker loop, that poll for any outstanding notifications and work. 
 * 
 * This is an abstract class and its implementations should monitor the underlying bucket store and call `notify` method 
 * whenever streams are added to or removed from the bucket. These notifications are added to the notification queue.
 * 
 * Notification loop scans the notification queue and if a new stream has been added to the bucket, it
 * posts a new entry into work priority queue.  
 * 
 * Work posted into the priority queue has `stream identifier` and `time`. The queue is ordered based on time when the work 
 * should be executed next. We ensure that there is exactly one work per stream that is posted into the work priority queue. 
 * 
 * Worker loop peeks at the first element from the priority queue. If the first element in the queue can be executed, 
 * it is taken from the queue else, the loop sleeps till it can dequeue a work. 
 * 
 * Worker loop also only dequeues `maxConcurrentExecutions` number of work items from the priority queue. 
 * This is managed via a semaphore. Whenever a new work is picked from the queue, a semaphore is acquired and the `work` 
 * is submitted for execution.   
 * Whenever the work completes, we add the entry back into the work queue with next schedule for the stream after `execution duration` 
 * and releases the semaphore so that more outstanding work from the queue can be picked up. 
 * This ensures that we only have a limited number of outstanding work items irrespective of number of streams under the bucket. 
 * WIth exactly one entry per stream in the priority queue, we also ensure fairness. 
 */
abstract class BucketService extends AbstractService {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(BucketService.class));

    protected final ScheduledExecutorService executor;

    @Getter(AccessLevel.PROTECTED)
    private final int bucketId;
    @Getter(AccessLevel.PROTECTED)
    private final BucketStore.ServiceType serviceType;
    private final Semaphore semaphore;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final PriorityQueue<QueueElement> workQueue;
    @GuardedBy("lock")
    private final Set<Stream> knownStreams;
    private final LinkedBlockingQueue<StreamNotification> notifications;
    private final CompletableFuture<Void> serviceStartFuture;
    private final CompletableFuture<Void> notificationLoop;
    private final CompletableFuture<Void> workerLoop;
    private final Duration executionPeriod;
    private final BucketWork bucketWork;
    private final Thread notification;
    private final Thread worker;
    
    BucketService(BucketStore.ServiceType serviceType, int bucketId, ScheduledExecutorService executor,
                  int maxConcurrentExecutions, Duration executionPeriod, BucketWork bucketWork) {
        this.serviceType = serviceType;
        this.bucketId = bucketId;
        this.executor = executor;
        this.notifications = new LinkedBlockingQueue<>();
        this.serviceStartFuture = new CompletableFuture<>();
        this.notificationLoop = new CompletableFuture<>();
        this.workerLoop = new CompletableFuture<>();
        semaphore = new Semaphore(maxConcurrentExecutions);
        this.knownStreams = new HashSet<>();
        this.workQueue = new PriorityQueue<>(Comparator.comparingLong(x -> x.nextExecutionTimeInMillis));
        this.executionPeriod = executionPeriod;
        this.bucketWork = bucketWork;
        String threadRoot = String.format("%s-%d-", serviceType.getName(), bucketId);
        String notificationThreadName = String.format("%s-notification", threadRoot);
        String workerThreadName = String.format("%s-worker", threadRoot);
        this.notification = new Thread(this::notificationLoop, notificationThreadName);
        this.worker = new Thread(this::workerLoop, workerThreadName);
    }

    @Override
    public void doStart() {
        CompletableFuture.runAsync(() -> {
            startBucketChangeListener();
            
            notifyStarted();
            
            notification.start();
            worker.start();

            serviceStartFuture.complete(null);
        });
    }

    abstract void startBucketChangeListener();

    abstract void stopBucketChangeListener();

    private void notificationLoop() {
        log.info("{}: Notification loop terminated for bucket {}", serviceType, bucketId);

        try {
            long executionDuration = executionPeriod.toMillis();
            while (isRunning()) {
                StreamNotification notification =
                        Exceptions.handleInterruptedCall(() -> notifications.poll(100, TimeUnit.MILLISECONDS));
                if (notification != null) {
                    final StreamImpl stream;
                    switch (notification.getType()) {
                        case StreamAdded:
                            log.info("{}: New stream {}/{} added to bucket {} ", serviceType, notification.getScope(),
                                    notification.getStream(), bucketId);
                            stream = new StreamImpl(notification.getScope(), notification.getStream());
                            synchronized (lock) {
                                if (!knownStreams.contains(stream)) {
                                    knownStreams.add(stream);
                                    workQueue.add(new QueueElement(stream, executionDuration));
                                }
                            }
                            break;
                        case StreamRemoved:
                            log.info("{}: Stream {}/{} removed from bucket {}", serviceType, notification.getScope(),
                                    notification.getStream(), bucketId);
                            stream = new StreamImpl(notification.getScope(), notification.getStream());
                            synchronized (lock) {
                                knownStreams.remove(stream);
                            }
                            break;
                        case ConnectivityError:
                            log.warn("{}: StreamNotification for connectivity error", serviceType);
                            break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("{}: Exception thrown from notification loop for bucket", serviceType, bucketId, e);
        } finally{
            log.info("{}: Notification loop terminated for bucket {}", serviceType, bucketId);
            notificationLoop.complete(null);
        }
    }

    private void workerLoop() {
        try {
            log.info("{}: Worker loop started for bucket {}", serviceType, bucketId);

            while (isRunning()) {
                long time = System.currentTimeMillis();
                QueueElement element;
                long delayInMillis = 0;

                synchronized (lock) {
                    element = workQueue.peek();
                    if (element == null) {
                        delayInMillis = 100;
                    } else if (element.nextExecutionTimeInMillis <= time) {
                        // Note: we can poll on queue while holding the lock because we know the element exists.

                        element = workQueue.poll();
                        assert element != null;

                        if (!knownStreams.contains(element.getStream())) {
                            // the stream is removed from the known set. Ignore any queue entry for this stream. 
                            // let next cycle of process work happen immediately
                            element = null;
                        }
                    } else {
                        // no work to be started. 
                        delayInMillis = element.nextExecutionTimeInMillis - time;
                        element = null;
                    }
                }

                if (element != null) {
                    Stream stream = element.getStream();
                    Exceptions.handleInterrupted(semaphore::acquire);
                    bucketWork.doWork(stream).whenComplete((r, e) -> {
                        long nextRun = System.currentTimeMillis() + executionPeriod.toMillis();
                        synchronized (lock) {
                            if (knownStreams.contains(stream)) {
                                workQueue.add(new QueueElement(stream, nextRun));
                            }
                        }
                        semaphore.release();
                    });
                }

                long sleepTime = delayInMillis;
                Exceptions.handleInterrupted(() -> Thread.sleep(sleepTime));
            }
        } catch (Exception e) {
            log.error("{}: Exception thrown from worker loop for bucket", serviceType, bucketId, e);
        } finally {
            log.info("{}: Worker loop terminated for bucket {}", serviceType, bucketId);
            workerLoop.complete(null);
        }
    }
    
    @Override
    protected void doStop() {
        log.info("{}: Stop request received for bucket {}", serviceType, bucketId);
        serviceStartFuture.thenRun(() -> {
            notification.interrupt();
            worker.interrupt();
            stopBucketChangeListener();

            CompletableFuture.allOf(notificationLoop, workerLoop).whenComplete((r, e) -> {
                if (e != null) {
                    log.error("{}: Error while stopping bucket {}", serviceType, bucketId, e);
                    notifyFailed(e);
                } else {
                    log.info("{}: Cancellation for all background work for bucket {} issued", serviceType, bucketId);
                    notifyStopped();
                }
            });
        });
    }

    public void notify(StreamNotification notification) {
        notifications.add(notification);
    }

    @VisibleForTesting
    Set<Stream> getWorkFutureSet() {
        return Collections.unmodifiableSet(knownStreams);
    }

    @Data
    private static class QueueElement {
        private final Stream stream;
        private final long nextExecutionTimeInMillis;
    }

    @Data
    class StreamNotification {
        private final String scope;
        private final String stream;
        private final NotificationType type;
    }

    public enum NotificationType {
        StreamAdded,
        StreamRemoved,
        ConnectivityError
    }
}
