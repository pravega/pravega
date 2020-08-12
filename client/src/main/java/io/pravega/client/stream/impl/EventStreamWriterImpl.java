/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.ByteBufferUtils;
import io.pravega.common.util.Retry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * This class takes in events, finds out which segment they belong to and then calls write on the appropriate segment.
 * It deals with segments that are sealed by re-sending the unacked events to the new correct segment.
 * 
 * @param <Type> The type of event that is sent
 */
@Slf4j
@ToString(of = { "stream", "closed" })
public class EventStreamWriterImpl<Type> implements EventStreamWriter<Type> {

    /**
     * These two locks are used to enforce the following behavior:
     *
     * a. When a Write is happening, segmentSealedCallback cannot be executed concurrently, this is used to handle
     * missing event.
     * b. When a Write is happening, a newer write cannot be executed concurrently.
     * c. When a Write is happening, flush cannot be executed concurrently.
     * d. When a Flush is being invoked, segmentSealedCallback can be executed concurrently.
     * e. When a Flush is being invoked, write cannot be executed concurrently.
     * f. When a Close is being invoked, write cannot be executed concurrently.
     * g. When a Close is being invoked, Flush and segmentSealedCallback can be executed concurrently.
     */
    private final Object writeFlushLock = new Object();
    private final Object writeSealLock = new Object();

    private final Stream stream;
    private final String writerId;
    private final Serializer<Type> serializer;
    private final Controller controller;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventWriterConfig config;
    private final SegmentSelector selector;
    private final Consumer<Segment> segmentSealedCallBack;
    private final ConcurrentLinkedQueue<Segment> sealedSegmentQueue = new ConcurrentLinkedQueue<>();
    private final ExecutorService retransmitPool;
    private final Pinger pinger;
    private final DelegationTokenProvider tokenProvider;
    
    EventStreamWriterImpl(Stream stream, String writerId, Controller controller, SegmentOutputStreamFactory outputStreamFactory,
                          Serializer<Type> serializer, EventWriterConfig config, ExecutorService retransmitPool,
                          ScheduledExecutorService internalExecutor) {
        this.writerId = writerId;
        this.stream = Preconditions.checkNotNull(stream);
        this.controller = Preconditions.checkNotNull(controller);
        this.segmentSealedCallBack = this::handleLogSealed;
        this.tokenProvider = DelegationTokenProviderFactory.create(this.controller, this.stream.getScope(), this.stream.getStreamName());
        this.selector = new SegmentSelector(stream, controller, outputStreamFactory, config, tokenProvider);
        this.serializer = Preconditions.checkNotNull(serializer);
        this.config = config;
        this.retransmitPool = Preconditions.checkNotNull(retransmitPool);
        this.pinger = new Pinger(config.getTransactionTimeoutTime(), stream, controller, internalExecutor);
        List<PendingEvent> failedEvents = selector.refreshSegmentEventWriters(segmentSealedCallBack);
        assert failedEvents.isEmpty() : "There should not be any events to have failed";
        if (config.isAutomaticallyNoteTime()) {
            //See: https://github.com/pravega/pravega/issues/4218
            internalExecutor.scheduleWithFixedDelay(() -> noteTimeInternal(System.currentTimeMillis()), 5, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public CompletableFuture<Void> writeEvent(Type event) {
        return writeEventInternal(null, event);
    }

    @Override
    public CompletableFuture<Void> writeEvent(String routingKey, Type event) {
        Preconditions.checkNotNull(routingKey);
        return writeEventInternal(routingKey, event);
    }
    
    private CompletableFuture<Void> writeEventInternal(String routingKey, Type event) {
        Preconditions.checkNotNull(event);
        Exceptions.checkNotClosed(closed.get(), this);
        ByteBuffer data = serializer.serialize(event);
        CompletableFuture<Void> ackFuture = new CompletableFuture<Void>();
        synchronized (writeFlushLock) {
            synchronized (writeSealLock) {                
                SegmentOutputStream segmentWriter = selector.getSegmentOutputStreamForKey(routingKey);
                while (segmentWriter == null) {
                    log.info("Don't have a writer for segment: {}", selector.getSegmentForEvent(routingKey));
                    handleMissingLog();
                    segmentWriter = selector.getSegmentOutputStreamForKey(routingKey);
                }
                segmentWriter.write(PendingEvent.withHeader(routingKey, data, ackFuture));
            }
        }
        return ackFuture;
    }
    
    @GuardedBy("writeSealLock")
    private void handleMissingLog() {
        List<PendingEvent> toResend = selector.refreshSegmentEventWriters(segmentSealedCallBack);
        resend(toResend);
    }

    /**
     * If a log sealed is encountered, we need to 1. Find the new segments to write to. 2. For each outstanding
     * message find which new segment it should go to and send it there. 
     */
    private void handleLogSealed(Segment segment) {
        sealedSegmentQueue.add(segment);
        retransmitPool.execute(() -> {
            Retry.indefinitelyWithExpBackoff(config.getInitialBackoffMillis(), config.getBackoffMultiple(),
                                             config.getMaxBackoffMillis(),
                                             t -> log.error("Encountered exception when handling a sealed segment: ", t))
                 .run(() -> {
                     /*
                      * Using writeSealLock prevents concurrent segmentSealedCallback for different segments
                      * from being invoked concurrently, or concurrently with write.
                      * 
                      * By calling flush while the write lock is held we can ensure that any inflight
                      * entries that will succeed in being written to a new segment are written and any
                      * segmentSealedCallbacks that will be called happen before the next write is invoked.
                      */
                     synchronized (writeSealLock) {
                         Segment toSeal = sealedSegmentQueue.poll();
                         log.info("Sealing segment {} ", toSeal);
                         while (toSeal != null) {
                             resend(selector.refreshSegmentEventWritersUponSealed(toSeal, segmentSealedCallBack));
                             // remove segment writer after resending inflight events of the sealed segment.
                             selector.removeSegmentWriter(toSeal);
                             /* In the case of segments merging Flush ensures there can't be anything left
                              * inflight that will need to be resent to the new segment when the write lock
                              * is released. (To preserve order)
                              */
                             for (SegmentOutputStream writer : selector.getWriters().values()) {
                                 try {
                                     writer.write(PendingEvent.withoutHeader(null, ByteBufferUtils.EMPTY, null));
                                     writer.flush();
                                 } catch (SegmentSealedException e) {
                                     // Segment sealed exception observed during a flush. Re-run flush on all the
                                     // available writers.
                                     log.info("Flush on segment {} failed due to {}, it will be retried.", writer.getSegmentName(), e.getMessage());
                                 }
                             }
                             toSeal = sealedSegmentQueue.poll();
                             log.info("Sealing another segment {} ", toSeal);
                         }
                     }
                     return null;
                 });
        });
    }

    @GuardedBy("writeSealLock")
    private void resend(List<PendingEvent> toResend) {
        while (!toResend.isEmpty()) {
            List<PendingEvent> unsent = new ArrayList<>();
            boolean sendFailed = false;
            log.info("Resending {} events", toResend.size());
            for (PendingEvent event : toResend) {
                if (sendFailed) {
                    unsent.add(event);
                } else {
                    SegmentOutputStream segmentWriter = selector.getSegmentOutputStreamForKey(event.getRoutingKey());
                    if (segmentWriter == null) {
                        log.info("No writer for segment during resend.");
                        unsent.addAll(selector.refreshSegmentEventWriters(segmentSealedCallBack));
                        sendFailed = true;
                    } else {
                        segmentWriter.write(event);
                    }
                }
            }
            toResend = unsent;
        }
    }

    @Override
    public void flush() {
        Preconditions.checkState(!closed.get());
        synchronized (writeFlushLock) {
            boolean success = false;
            while (!success) {
                success = true;
                for (SegmentOutputStream writer : selector.getWriters().values()) {
                    try {
                        writer.flush();
                    } catch (SegmentSealedException e) {
                        // Segment sealed exception observed during a flush. Re-run flush on all the
                        // available writers.
                        success = false;
                        log.warn("Flush on segment {} failed due to {}, it will be retried.", writer.getSegmentName(), e.getMessage());
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        if (closed.getAndSet(true)) {
            return;
        }
        pinger.close();
        synchronized (writeFlushLock) {
            boolean success = false;
            while (!success) {
                success = true;
                for (SegmentOutputStream writer : selector.getWriters().values()) {
                    try {
                        writer.close();
                    } catch (SegmentSealedException e) {
                        // Segment sealed exception observed during a close. Re-run close on all the available writers.
                        success = false;
                        log.warn("Close failed due to {}, it will be retried.", e.getMessage());
                    }
                }
            }
        }
        ExecutorServiceHelpers.shutdown(retransmitPool);
    }

    @Override
    public EventWriterConfig getConfig() {
        return config;
    }

    @Override
    public void noteTime(long timestamp) {
        Preconditions.checkState(!config.isAutomaticallyNoteTime(), "To note time, automatic noting of time should be disabled.");
        noteTimeInternal(timestamp);
    }

    private void noteTimeInternal(long timestamp) {
        Map<Segment, Long> offsets = selector.getWriters()
                                             .entrySet()
                                             .stream()
                                             .collect(Collectors.toMap(e -> e.getKey(),
                                                                       e -> e.getValue().getLastObservedWriteOffset()));
        WriterPosition position = new WriterPosition(offsets);
        controller.noteTimestampFromWriter(writerId, stream, timestamp, position);
    }
}
