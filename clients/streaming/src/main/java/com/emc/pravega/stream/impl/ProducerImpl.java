/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.GuardedBy;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import lombok.extern.slf4j.Slf4j;

/**
 * This class takes in events, finds out which segment they belong to and then calls produce on the appropriate segment.
 * It deals with segments that are sealed by re-sending the unacked events to the new correct segment.
 */
@Slf4j
public class ProducerImpl<Type> implements Producer<Type> {

    private final Object lock = new Object();
    private final Stream stream;
    private final Serializer<Type> serializer;
    private final SegmentOutputStreamFactory outputStreamFactory;
    private final Controller controller;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventRouter router;
    private final ProducerConfig config;
    @GuardedBy("lock")
    private final Map<Segment, SegmentProducer<Type>> producers = new HashMap<>();

    ProducerImpl(Stream stream, Controller controller, SegmentOutputStreamFactory outputStreamFactory, EventRouter router, Serializer<Type> serializer,
            ProducerConfig config) {
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
            List<PendingEvent<Type>> list = setupSegmentProducers();
            if (!list.isEmpty()) {
                throw new IllegalStateException("Producer initialized with unsent messages?!");
            }
        }
    }

    /**
     * Populate {@link #producers} by setting up a segmentProducer for each segment in the stream.
     * 
     * @return The events that were sent but never acked to segments that are now sealed, and hence need to be
     *         retransmitted.
     */
    private List<PendingEvent<Type>> setupSegmentProducers() {
        Collection<Segment> segments = Retry.withExpBackoff(1, 10, 5)
            .retryingOn(SegmentSealedException.class)
            .throwingOn(RuntimeException.class)
            .run(() -> {
                Collection<Segment> s = getAndHandleExceptions(controller.getCurrentSegments(stream.getScope(), stream.getStreamName()), RuntimeException::new).getSegments();
                for (Segment segment : s) {
                    if (!producers.containsKey(segment)) {
                        SegmentOutputStream out = outputStreamFactory.createOutputStreamForSegment(segment,
                                                                                         config.getSegmentConfig());
                        producers.put(segment, new SegmentProducerImpl<>(out, serializer));
                    }
                }
                return s;
            });
        List<PendingEvent<Type>> toResend = new ArrayList<>();

        Iterator<Entry<Segment, SegmentProducer<Type>>> iter = producers.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Segment, SegmentProducer<Type>> entry = iter.next();
            if (!segments.contains(entry.getKey())) {
                SegmentProducer<Type> producer = entry.getValue();
                iter.remove();
                try {
                    producer.close();
                } catch (SegmentSealedException e) {
                    log.warn("Caught exception closing old producer: ", e);
                }
                toResend.addAll(producer.getUnackedEvents());
            }
        }
        return toResend;
    }

    @Override
    public Future<Void> publish(String routingKey, Type event) {
        Preconditions.checkState(!closed.get());
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        synchronized (lock) {
            if (!attemptPublish(new PendingEvent<Type>(event, routingKey, result))) {
                handleLogSealed();
            }
        }
        return FutureHelpers.toVoid(result);
    }

    /**
     * If a log sealed is encountered, we need to 1. Find the new segments to produce to. 2. For each outstanding
     * message find which new segment it should go to and send it there. This can happen recursively if segments turn
     * over very quickly.
     */
    private void handleLogSealed() {
        List<PendingEvent<Type>> toResend = setupSegmentProducers();
        while (toResend.isEmpty()) {
            List<PendingEvent<Type>> unsent = new ArrayList<>();
            for (PendingEvent<Type> event : toResend) {
                if (!attemptPublish(event)) {
                    unsent.add(event);
                }
            }
            if (!unsent.isEmpty()) {
                unsent.addAll(setupSegmentProducers());
            }
            toResend = unsent;
        }
    }

    private boolean attemptPublish(PendingEvent<Type> event) {
        SegmentProducer<Type> segmentProducer = getSegmentProducer(event.getRoutingKey());
        if (segmentProducer == null || segmentProducer.isAlreadySealed()) {
            return false;
        }
        try {
            segmentProducer.publish(event);
            return true;
        } catch (SegmentSealedException e) {
            return false;
        }
    }

    private SegmentProducer<Type> getSegmentProducer(String routingKey) {
        Segment log = router.getSegmentForEvent(routingKey);
        return producers.get(log);
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
        public void publish(String routingKey, Type event) throws TxnFailedException {
            Preconditions.checkState(!closed.get());
            Segment s = router.getSegmentForEvent(routingKey);
            SegmentTransaction<Type> transaction = inner.get(s);
            transaction.publish(event);
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
        public void drop() {
            FutureHelpers.getAndHandleExceptions(controller.dropTransaction(stream, txId), RuntimeException::new);
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
        public UUID getTransactionId() {
            return txId;
        }

    }

    @Override
    public Transaction<Type> beginTransaction(long timeout) {
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        ArrayList<Segment> segmentIds;
        synchronized (lock) {
            segmentIds = new ArrayList<>(producers.keySet());
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
    public Transaction<Type> getTransaction(UUID txId) {
        Map<Segment, SegmentTransaction<Type>> transactions = new HashMap<>();
        ArrayList<Segment> segmentIds;
        synchronized (lock) {
            segmentIds = new ArrayList<>(producers.keySet());
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
                for (SegmentProducer<Type> p : producers.values()) {
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
                for (SegmentProducer<Type> p : producers.values()) {
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
            producers.clear();
        }
    }

    @Override
    public ProducerConfig getConfig() {
        return config;
    }

}
