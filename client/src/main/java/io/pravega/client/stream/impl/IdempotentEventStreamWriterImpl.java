/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.IdempotentEventStreamWriter;
import io.pravega.client.stream.Segment;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.segment.SegmentOutputStream;
import io.pravega.client.stream.impl.segment.SegmentOutputStreamFactory;
import io.pravega.client.stream.impl.segment.SegmentSealedException;
import io.pravega.common.Exceptions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class takes in events, finds out which segment they belong to and then calls write on the appropriate segment.
 * It deals with segments that are sealed by re-sending the unacked events to the new correct segment.
 * 
 * @param <Type> The type of event that is sent
 */
@Slf4j
@ToString(of = { "stream", "closed" })
public class IdempotentEventStreamWriterImpl<Type> implements IdempotentEventStreamWriter<Type> {

    private final Object lock = new Object();
    private final UUID writerId;
    private final Stream stream;
    private final Serializer<Type> serializer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventWriterConfig config;
    @GuardedBy("lock")
    private final SegmentSelector selector;
    @GuardedBy("lock")
    private long lastSequence = Long.MIN_VALUE;

    IdempotentEventStreamWriterImpl(Stream stream, UUID writerId, Controller controller,
            SegmentOutputStreamFactory outputStreamFactory, Serializer<Type> serializer, EventWriterConfig config) {
        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(writerId);
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(outputStreamFactory);
        Preconditions.checkNotNull(serializer);
        this.stream = stream;
        this.writerId = writerId;
        this.selector = new SegmentSelector(stream, writerId, controller, outputStreamFactory);
        this.serializer = serializer;
        this.config = config;
        List<PendingEvent> failedEvents = selector.refreshSegmentEventWriters();
        assert failedEvents.isEmpty() : "There should not be any events to have failed";
    }
    
    @Override
    public AckFuture writeEvent(String routingKey, long sequence, Type event) {
        Preconditions.checkNotNull(routingKey);
        return writeEventInternal(routingKey, sequence, event);
    }
    
    AckFuture writeEventInternal(String routingKey, long sequence, Type event) {
        Preconditions.checkNotNull(event);
        Exceptions.checkNotClosed(closed.get(), this);
        ByteBuffer data = serializer.serialize(event);
        CompletableFuture<Boolean> result = new CompletableFuture<Boolean>();
        synchronized (lock) {
            checkArgument(lastSequence < sequence, "Sequence was out of order. Previously saw {} and now {}",
                          lastSequence, sequence);
            lastSequence = sequence;
            SegmentOutputStream segmentWriter = selector.getSegmentOutputStreamForKey(routingKey);
            while (segmentWriter == null) {
                log.info("Don't have a writer for segment: {}", selector.getSegmentForEvent(routingKey));
                handleMissingLog();
                segmentWriter = selector.getSegmentOutputStreamForKey(routingKey);
            }
            try {
                segmentWriter.write(new PendingEvent(routingKey, sequence, data, result));
            } catch (SegmentSealedException e) {
                log.info("Segment was sealed: {}", segmentWriter);
                handleLogSealed(Segment.fromScopedName(segmentWriter.getSegmentName()));
            }
        }
        return new AckFutureImpl(result, () -> {
            if (!closed.get()) {
                flushInternal();
            }
        });
    }
    
    @GuardedBy("lock")
    private void handleMissingLog() {
        List<PendingEvent> toResend = selector.refreshSegmentEventWriters();
        resend(toResend);
    }

    /**
     * If a log sealed is encountered, we need to 1. Find the new segments to write to. 2. For each outstanding
     * message find which new segment it should go to and send it there. This can happen recursively if segments turn
     * over very quickly.
     */
    @GuardedBy("lock")
    private void handleLogSealed(Segment segment) {
        List<PendingEvent> toResend = selector.refreshSegmentEventWritersUponSealed(segment);
        resend(toResend);
    }

    @GuardedBy("lock")
    private void resend(List<PendingEvent> toResend) {
        while (!toResend.isEmpty()) {
            List<PendingEvent> unsent = new ArrayList<>();
            boolean sendFailed = false;
            for (PendingEvent event : toResend) {
                if (sendFailed) {
                    unsent.add(event);
                } else {
                    SegmentOutputStream segmentWriter = selector.getSegmentOutputStreamForKey(event.getRoutingKey());
                    if (segmentWriter == null) {
                        unsent.addAll(selector.refreshSegmentEventWriters());
                        sendFailed = true;
                    } else {
                        try {
                            segmentWriter.write(event);
                        } catch (SegmentSealedException e) {
                            log.info("Segment was sealed while handling seal: {}", segmentWriter);
                            Segment segment = Segment.fromScopedName(segmentWriter.getSegmentName());
                            unsent.addAll(selector.refreshSegmentEventWritersUponSealed(segment));
                            sendFailed = true;
                        }
                    }
                }
            }
            toResend = unsent;
        }
    }

    @Override
    public void flush() {
        Preconditions.checkState(!closed.get());
        flushInternal();
    }
    
    private void flushInternal() {
        boolean success = false;
        String sealedSegment = null;
        while (!success) {
            success = true;
            synchronized (lock) {
                for (SegmentOutputStream writer : selector.getWriters()) {
                    try {
                        writer.flush();
                    } catch (SegmentSealedException e) {
                        log.info("Segment was sealed during flush: {}", writer);
                        success = false;
                        sealedSegment = writer.getSegmentName();
                        break;
                    }
                }
                if (!success) {
                    handleLogSealed(Segment.fromScopedName(sealedSegment));
                }
            }
        }
    }

    @Override
    public void close() {
        if (closed.getAndSet(true)) {
            return;
        }
        synchronized (lock) {
            boolean success = false;
            String sealedSegment = null;
            while (!success) {
                success = true;
                for (SegmentOutputStream writer : selector.getWriters()) {
                    try {
                        writer.close();
                    } catch (SegmentSealedException e) {
                        log.info("Segment was sealed during close: {}", writer);
                        success = false;
                        sealedSegment = writer.getSegmentName();
                        break;
                    }
                }
                if (!success) {
                    handleLogSealed(Segment.fromScopedName(sealedSegment));
                }
            }
        }
    }

    @Override
    public EventWriterConfig getConfig() {
        return config;
    }

}
