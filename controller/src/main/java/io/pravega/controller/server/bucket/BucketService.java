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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.controller.store.stream.BucketStore;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class represents an instance of a background worker service that runs background work for all streams under a 
 * specific bucket for a specific service type. This means a separate instance of this class is instantiated for each 
 * bucket for each service type.   
 * Each object has two categories of work that it performs - process notification and execute the supplied worker function 
 * on known sets of streams.
 * 
 * This is an abstract class and its implementations should monitor the underlying bucket store and call `notify` method 
 * whenever streams are added to or removed from the bucket. These stream change notifications are added to the notification queue.
 * 
 * The notification loop periodically dequeues at most `MAX_NOTIFICATIONS_TO_TAKE` notifications from the queue and asynchronously 
 * process these notifications. Upon completion of notification, the loop continues with the next iteration. 
 * The queue used for notification is `BlockingDrainingQueue` which exposes an asynchronous `take` method. 
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
 * Worker loop runs as an infinite loop and, at the most, dequeues `availableSlots` number of work items from the priority queue. 
 * During each loop iteration it first checks if there are slots available to pick a work. If not, it postpones itself with a delay. 
 * Available slots is managed via a thread safe counter. Whenever a new work is picked from the queue, the counter is decremented 
 * and work is started asynchronously. 
 * Whenever the work completes, we add the entry back into the work queue with next schedule for the stream after `execution duration` 
 * and increment the available slots counter so that more outstanding work from the queue can be picked up. 
 * This ensures that we only have a limited number of outstanding work items irrespective of number of streams under the bucket. 
 * With exactly one entry per stream in the priority queue, we also ensure fairness. 
 */
abstract class BucketService extends AbstractService {
    private static final int MAX_NOTIFICATIONS_TO_TAKE = 100;
    private static final long DELAY_IN_MILLIS = 100L;
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(BucketService.class));

    protected final ScheduledExecutorService executor;

    @Getter(AccessLevel.PROTECTED)
    private final int bucketId;
    @Getter(AccessLevel.PROTECTED)
    private final BucketStore.ServiceType serviceType;
    @GuardedBy("lock")
    private int availableSlots;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final PriorityQueue<QueueElement> workQueue;
    @GuardedBy("lock")
    private final Set<Stream> knownStreams;
    private final BlockingDrainingQueue<StreamNotification> notifications;
    private final CompletableFuture<Void> serviceStartFuture;
    private final AtomicReference<CompletableFuture<Void>> notificationLoop;
    private final AtomicReference<CompletableFuture<Void>> workerLoop;
    private final Duration executionPeriod;
    private final BucketWork bucketWork;
    
    BucketService(BucketStore.ServiceType serviceType, int bucketId, ScheduledExecutorService executor,
                  int maxConcurrentExecutions, Duration executionPeriod, BucketWork bucketWork) {
        this.serviceType = serviceType;
        this.bucketId = bucketId;
        this.executor = executor;
        this.notifications = new BlockingDrainingQueue<>();
        this.serviceStartFuture = new CompletableFuture<>();
        this.notificationLoop = new AtomicReference<>(CompletableFuture.completedFuture(null));
        this.workerLoop = new AtomicReference<>(CompletableFuture.completedFuture(null));
        this.availableSlots = maxConcurrentExecutions;
        this.knownStreams = new HashSet<>();
        this.workQueue = new PriorityQueue<>(Comparator.comparingLong(x -> x.nextExecutionTimeInMillis));
        this.executionPeriod = executionPeriod;
        this.bucketWork = bucketWork;
    }

    @Override
    public void doStart() {
        CompletableFuture.runAsync(() -> {
            try {
                startBucketChangeListener();

                notifyStarted();

                notificationLoop.set(Futures.loop(this::isRunning, this::processNotification, executor));
                log.info("{}: Notification loop started for bucket {}", serviceType, bucketId);

                workerLoop.set(Futures.loop(this::isRunning, this::work, executor));
                log.info("{}: Notification loop started for bucket {}", serviceType, bucketId);

            } finally {
                log.info("{}: bucket {} service start completed", getServiceType(), getBucketId());
                serviceStartFuture.complete(null);
            }
        });
    }

    abstract void startBucketChangeListener();

    abstract void stopBucketChangeListener();

    private CompletableFuture<Void> processNotification() {
        return notifications.take(MAX_NOTIFICATIONS_TO_TAKE).thenAccept(queue -> {
            queue.forEach(notification -> {
                final StreamImpl stream;
                switch (notification.getType()) {
                    case StreamAdded:
                        handleStreamAdded(notification);
                        break;
                    case StreamRemoved:
                        handleStreamRemoved(notification);
                        break;
                    case ConnectivityError:
                        log.warn("{}: StreamNotification for connectivity error", serviceType);
                        break;
                }
            });
        });
    }

    private void handleStreamRemoved(StreamNotification notification) {
        StreamImpl stream;
        log.info("{}: Stream {}/{} removed from bucket {}", serviceType, notification.getScope(),
                notification.getStream(), bucketId);
        stream = new StreamImpl(notification.getScope(), notification.getStream());
        synchronized (lock) {
            knownStreams.remove(stream);
        }
    }

    private void handleStreamAdded(StreamNotification notification) {
        StreamImpl stream;
        log.info("{}: New stream {}/{} added to bucket {} ", serviceType, notification.getScope(),
                notification.getStream(), bucketId);
        stream = new StreamImpl(notification.getScope(), notification.getStream());
        long nextRun = System.currentTimeMillis() + executionPeriod.toMillis();

        synchronized (lock) {
            if (!knownStreams.contains(stream)) {
                knownStreams.add(stream);
                workQueue.add(new QueueElement(stream, nextRun));
            }
        }
    }

    /**
     * This method indicates Each workloop iteration.
     * In each iteration, it checks the count for all submitted ongoing `bucketWork` for streams (via availableSlots).
     * If there are available slots, it checks the work queue to see if the first element can be taken from the queue yet. 
     * If not, it returns a delayed future with fixed delay for the next iteration to be tried. 
     * If the queue is empty, it completes this iteration with a delayed future with a fixed constant delay to poll the queue later. 
     * If the queue has an element which can be taken for processing, then the element is removed from the queue and one of available 
     * slots is aquired. the "bucketWork" corresponding to queue element is submitted with a callback registered on completion 
     * to release the available slot. It returns a future with 0 delay so that next iteration can be scheduled immediately.  
     * @return CompletableFuture which when completed indicates that the current iteration is complete and next iteration 
     * can be initiated.
     */
    private CompletableFuture<Void> work() {
        long time = System.currentTimeMillis();
        QueueElement element;
        long delayInMillis = 0L;
        synchronized (lock) {
            element = workQueue.peek();
            if (availableSlots > 0 && element != null && element.nextExecutionTimeInMillis <= time) {
                element = workQueue.poll();

                if (!knownStreams.contains(element.getStream())) {
                    // the stream is removed from the known set. Ignore any queue entry for this stream. 
                    // let next cycle of process work happen immediately
                    element = null;
                } else {
                    availableSlots--;
                }
            } else {
                // cant do any work. set element to null and add fixed delay.
                delayInMillis = DELAY_IN_MILLIS;
                element = null;
            }
        }

        if (element != null) {
            Stream stream = element.getStream();
            
            bucketWork.doWork(stream).handle((r, e) -> {
                long nextRun = System.currentTimeMillis() + executionPeriod.toMillis();
                synchronized (lock) {
                    // if known stream contains this stream, add the work back into the queue with next run time 
                    if (knownStreams.contains(stream)) {
                        workQueue.add(new QueueElement(stream, nextRun));
                    }
                    // add the slot back
                    availableSlots++;
                    return null;
                }
            });
        } 
        
        // return a delayed future after which this loop is executed again. 
        // delay is typically `0` if we have found a slot to process and a non empty queue. 
        return Futures.delayedFuture(Duration.ofMillis(delayInMillis), executor);
    }
    
    @Override
    protected void doStop() {
        log.info("{}: Stop request received for bucket {}", serviceType, bucketId);
        serviceStartFuture.thenRun(() -> {
            notificationLoop.get().cancel(true);
            workerLoop.get().cancel(true);
            stopBucketChangeListener();

            CompletableFuture.allOf(notificationLoop.get(), workerLoop.get()).whenComplete((r, e) -> {
                log.info("{}: Cancellation for all background work for bucket {} issued", serviceType, bucketId);
                notifyStopped();
            });
        });
    }

    public void notify(StreamNotification notification) {
        notifications.add(notification);
    }

    @VisibleForTesting
    Set<Stream> getKnownStreams() {
        return Collections.unmodifiableSet(knownStreams);
    }

    @VisibleForTesting
    Collection<QueueElement> getWorkerQueue() {
        return Collections.unmodifiableCollection(workQueue);
    }
    
    @Data
    static class QueueElement {
        private final Stream stream;
        private final long nextExecutionTimeInMillis;
    }

    @Data
    static class StreamNotification {
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
