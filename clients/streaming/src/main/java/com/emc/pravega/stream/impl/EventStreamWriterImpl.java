/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.stream.AckFuture;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.PingFailedException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

/**
 * This class takes in events, finds out which segment they belong to and then calls write on the appropriate segment.
 * It deals with segments that are sealed by re-sending the unacked events to the new correct segment.
 * 
 * @param <Type> The type of event that is sent
 */
@Slf4j
public class EventStreamWriterImpl<Type> implements EventStreamWriter<Type> {

    private final Object lock = new Object();
    private final Stream stream;
    private final Serializer<Type> serializer;
    private final SegmentOutputStreamFactory outputStreamFactory;
    private final Controller controller;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventWriterConfig config;
    @GuardedBy("lock")
    private final SegmentSelector selector;

    EventStreamWriterImpl(Stream stream, Controller controller, SegmentOutputStreamFactory outputStreamFactory,
            Serializer<Type> serializer, EventWriterConfig config) {
        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(outputStreamFactory);
        Preconditions.checkNotNull(serializer);
        this.stream = stream;
        this.controller = controller;
        this.outputStreamFactory = outputStreamFactory;
        this.selector = new SegmentSelector(stream, controller, outputStreamFactory);
        this.serializer = serializer;
        this.config = config;
        List<PendingEvent> failedEvents = selector.refreshSegmentEventWriters();
        assert failedEvents.isEmpty();
    }

    @Override
    public AckFuture writeEvent(Type event) {
        return writeEventInternal(null, event);
    }

    @Override
    public AckFuture writeEvent(String routingKey, Type event) {
        Preconditions.checkNotNull(routingKey);
        return writeEventInternal(routingKey, event);
    }
    
    private AckFuture writeEventInternal(String routingKey, Type event) {
        Preconditions.checkNotNull(event);
        Preconditions.checkState(!closed.get());
        ByteBuffer data = serializer.serialize(event);
        CompletableFuture<Boolean> result = new CompletableFuture<Boolean>();
        synchronized (lock) {
            SegmentOutputStream segmentWriter = selector.getSegmentOutputStreamForKey(routingKey);
            while (segmentWriter == null) {
                log.info("Don't have a writer for segment: {}", selector.getSegmentForEvent(routingKey));
                handleLogSealed();
                segmentWriter = selector.getSegmentOutputStreamForKey(routingKey);
            }
            try {
                segmentWriter.write(new PendingEvent(routingKey, data, result));
            } catch (SegmentSealedException e) {
                log.info("Segment was sealed: {}", segmentWriter.toString());
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
    @GuardedBy("lock")
    private void handleLogSealed() {
        List<PendingEvent> toResend = selector.refreshSegmentEventWriters();
        while (!toResend.isEmpty()) {
            List<PendingEvent> unsent = new ArrayList<>();
            for (PendingEvent event : toResend) {
                SegmentOutputStream segmentWriter = selector.getSegmentOutputStreamForKey(event.getRoutingKey());
                if (segmentWriter == null) {
                    unsent.add(event);
                } else {
                    try {
                        segmentWriter.write(event);
                    } catch (SegmentSealedException e) {
                        log.info("Segment was sealed while handling seal: {}", segmentWriter.toString());
                        selector.removeWriter(segmentWriter);
                        unsent.addAll(segmentWriter.getUnackedEvents());
                    }
                }
            }
            if (!unsent.isEmpty()) {
                unsent.addAll(selector.refreshSegmentEventWriters());
            }
            toResend = unsent;
        }
    }

    private static class TransactionImpl<Type> implements Transaction<Type> {

        private final Map<Segment, SegmentTransaction<Type>> inner;
        private final UUID txId;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final SegmentSelector router;
        private final Controller controller;
        private final Stream stream;

        TransactionImpl(UUID txId, Map<Segment, SegmentTransaction<Type>> transactions, SegmentSelector router,
                Controller controller, Stream stream) {
            this.txId = txId;
            this.inner = transactions;
            this.router = router;
            this.controller = controller;
            this.stream = stream;
        }

        /**
         * Uses the transactionId to generate the routing key so that we only need to use one segment.
         */
        @Override
        public void writeEvent(Type event) throws TxnFailedException {
            writeEvent(txId.toString(), event);
        }
        
        @Override
        public void writeEvent(String routingKey, Type event) throws TxnFailedException {
            Preconditions.checkNotNull(event);
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
        public void ping(long lease) throws PingFailedException {
            Preconditions.checkArgument(lease > 0);
            FutureHelpers.getAndHandleExceptions(controller.pingTransaction(stream, txId, lease),
                    PingFailedException::new);
        }

        @Override
        public UUID getTxnId() {
            return txId;
        }

    }

    @Override
    public Transaction<Type> beginTxn(long timeout, long maxExecutionTime, long scaleGracePeriod) {
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        UUID txId = FutureHelpers.getAndHandleExceptions(
                controller.createTransaction(stream, timeout, maxExecutionTime, scaleGracePeriod),
                RuntimeException::new);
        for (Segment s : selector.getSegments()) {
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txId);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl<Type>(txId, transactions, selector, controller, stream);
    }
    
    @Override
    public Transaction<Type> getTxn(UUID txId) {
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        for (Segment s : selector.getSegments()) {
            SegmentOutputStream out = outputStreamFactory.createOutputStreamForTransaction(s, txId);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl<Type>(txId, transactions, selector, controller, stream);
    }

    @Override
    public void flush() {
        Preconditions.checkState(!closed.get());
        boolean success = false;
        while (!success) {
            success = true;
            synchronized (lock) {
                for (SegmentOutputStream writer : selector.getWriters()) {
                    try {
                        writer.flush();
                    } catch (SegmentSealedException e) {
                        log.info("Segment was sealed during flush: {}", writer.toString());
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
                for (SegmentOutputStream writer : selector.getWriters()) {
                    try {
                        writer.close();
                    } catch (SegmentSealedException e) {
                        log.info("Segment was sealed during close: {}", writer.toString());
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
    public EventWriterConfig getConfig() {
        return config;
    }

}
