/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.stream.AckFuture;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;

import lombok.extern.slf4j.Slf4j;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

/**
 * This class takes in events, finds out which segment they belong to and then calls write on the appropriate segment.
 * It deals with segments that are sealed by re-sending the unacked events to the new correct segment.
 */
@Slf4j
public class EventStreamWriterImpl<Type> implements EventStreamWriter<Type> {

    private final Object lock = new Object();
    private final Stream stream;
    private final Serializer<Type> serializer;
    private final SegmentOutputStreamFactory outputStreamFactory;
    private final Controller controller;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventRouter router;
    private final EventWriterConfig config;
    @GuardedBy("lock")
    private final Map<Segment, SegmentEventWriter<Type>> writers = new HashMap<>();

    EventStreamWriterImpl(Stream stream, Controller controller, SegmentOutputStreamFactory outputStreamFactory, EventRouter router, Serializer<Type> serializer,
            EventWriterConfig config) {
        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(outputStreamFactory);
        Preconditions.checkNotNull(router);
        Preconditions.checkNotNull(serializer);
        this.stream = stream;
        this.controller = controller;
        this.outputStreamFactory = outputStreamFactory;
        this.router = router;
        this.serializer = serializer;
        this.config = config;
        synchronized (lock) {
            List<PendingEvent<Type>> list = setupSegmentEventWriters();
            if (!list.isEmpty()) {
                throw new IllegalStateException("EventStreamWriter initialized with unsent messages?!");
            }
        }
    }

    /**
     * Populate {@link #writers} by setting up a segmentEventWriter for each segment in the stream.
     * 
     * @return The events that were sent but never acked to segments that are now sealed, and hence need to be
     *         retransmitted.
     */
    private List<PendingEvent<Type>> setupSegmentEventWriters() {
        Collection<Segment> segments = Retry.withExpBackoff(1, 10, 5)
            .retryingOn(SegmentSealedException.class)
            .throwingOn(RuntimeException.class)
            .run(() -> {
                Collection<Segment> s = getAndHandleExceptions(controller.getCurrentSegments(stream.getScope(), stream.getStreamName()), RuntimeException::new).getSegments();
                for (Segment segment : s) {
                    if (!writers.containsKey(segment)) {
                        SegmentOutputStream out = outputStreamFactory.createOutputStreamForSegment(segment,
                                                                                         config.getSegmentConfig());
                        writers.put(segment, new SegmentEventWriterImpl<>(out, serializer));
                    }
                }
                return s;
            });
        List<PendingEvent<Type>> toResend = new ArrayList<>();

        Iterator<Entry<Segment, SegmentEventWriter<Type>>> iter = writers.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Segment, SegmentEventWriter<Type>> entry = iter.next();
            if (!segments.contains(entry.getKey())) {
                SegmentEventWriter<Type> writer = entry.getValue();
                iter.remove();
                try {
                    writer.close();
                } catch (SegmentSealedException e) {
                    log.warn("Caught exception closing old writer: ", e);
                }
                toResend.addAll(writer.getUnackedEvents());
            }
        }
        return toResend;
    }

    @Override
    public AckFuture writeEvent(String routingKey, Type event) {
        Preconditions.checkState(!closed.get());
        CompletableFuture<Boolean> result = new CompletableFuture<Boolean>();
        synchronized (lock) {
            if (!attemptWrite(new PendingEvent<Type>(event, routingKey, result))) {
                handleLogSealed();
            }
        }
        return new AckFutureImpl(result);
    }

    /**
     * If a log sealed is encountered, we need to 1. Find the new segments to write to. 2. For each outstanding
     * message find which new segment it should go to and send it there. This can happen recursively if segments turn
     * over very quickly.
     */
    private void handleLogSealed() {
        List<PendingEvent<Type>> toResend = setupSegmentEventWriters();
        while (toResend.isEmpty()) {
            List<PendingEvent<Type>> unsent = new ArrayList<>();
            for (PendingEvent<Type> event : toResend) {
                if (!attemptWrite(event)) {
                    unsent.add(event);
                }
            }
            if (!unsent.isEmpty()) {
                unsent.addAll(setupSegmentEventWriters());
            }
            toResend = unsent;
        }
    }

    private boolean attemptWrite(PendingEvent<Type> event) {
        SegmentEventWriter<Type> segmentWriter = getSegmentWriter(event.getRoutingKey());
        if (segmentWriter == null || segmentWriter.isAlreadySealed()) {
            return false;
        }
        try {
            segmentWriter.write(event);
            return true;
        } catch (SegmentSealedException e) {
            return false;
        }
    }

    private SegmentEventWriter<Type> getSegmentWriter(String routingKey) {
        Segment log = router.getSegmentForEvent(routingKey);
        return writers.get(log);
    }

    private static class TransactionImpl<Type> implements Transaction<Type> {

        private final Map<Segment, SegmentTransaction<Type>> inner;
        private final UUID txId;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final EventRouter router;
        private final Controller controller;
        private final Stream stream;

        TransactionImpl(UUID txId, Map<Segment, SegmentTransaction<Type>> transactions, EventRouter router,
                Controller controller, Stream stream) {
            this.txId = txId;
            this.inner = transactions;
            this.router = router;
            this.controller = controller;
            this.stream = stream;
        }

        @Override
        public void writeEvent(Type event) throws TxnFailedException {
            writeEvent(txId.toString(), event);
        }
        
        @Override
        public void writeEvent(String routingKey, Type event) throws TxnFailedException {
            Preconditions.checkState(!closed.get());
            Segment s = router.getSegmentForEvent(routingKey);
            SegmentTransaction<Type> transaction = inner.get(s);
            transaction.writeEvent(event);
        }

        @Override
        public void commit() throws TxnFailedException {
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.flush();
            }
            FutureHelpers.getAndHandleExceptions(controller.commitTransaction(stream, txId), TxnFailedException::new);
            closed.set(true);
        }

        @Override
        public void abort() {
            FutureHelpers.getAndHandleExceptions(controller.abortTransaction(stream, txId), RuntimeException::new);
            closed.set(true);
        }

        @Override
        public Status checkStatus() {
            return FutureHelpers.getAndHandleExceptions(controller.checkTransactionStatus(stream, txId), RuntimeException::new);
        }

        @Override
        public void flush() throws TxnFailedException {
            Preconditions.checkState(!closed.get());
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.flush();
            }
        }

        @Override
        public UUID getTxnId() {
            return txId;
        }

    }

    @Override
    public Transaction<Type> beginTxn(long timeout) {
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        ArrayList<Segment> segmentIds;
        synchronized (lock) {
            segmentIds = new ArrayList<>(writers.keySet());
        }
        UUID txId = FutureHelpers.getAndHandleExceptions(controller.createTransaction(stream, timeout), RuntimeException::new);
        for (Segment s : segmentIds) {
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txId);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl<Type>(txId, transactions, router, controller, stream);
    }
    
    @Override
    public Transaction<Type> getTxn(UUID txId) {
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        ArrayList<Segment> segmentIds;
        synchronized (lock) {
            segmentIds = new ArrayList<>(writers.keySet());
        }
        for (Segment s : segmentIds) {
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txId);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl<Type>(txId, transactions, router, controller, stream);
    }

    @Override
    public void flush() {
        Preconditions.checkState(!closed.get());
        boolean success = false;
        while (!success) {
            success = true;
            synchronized (lock) {
                for (SegmentEventWriter<Type> p : writers.values()) {
                    try {
                        p.flush();
                    } catch (SegmentSealedException e) {
                        success = false;
                    }
                }
                if (!success) {
                    handleLogSealed();
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
            while (!success) {
                success = true;
                for (SegmentEventWriter<Type> p : writers.values()) {
                    try {
                        p.close();
                    } catch (SegmentSealedException e) {
                        success = false;
                    }
                }
                if (!success) {
                    handleLogSealed();
                }
            }
            writers.clear();
        }
    }

    @Override
    public EventWriterConfig getConfig() {
        return config;
    }

}
