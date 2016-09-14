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

import java.util.ArrayList;
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

import com.emc.pravega.common.util.Retry;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.EventRouter;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamSegments;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.segment.SegmentManager;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.base.Preconditions;

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
    private final SegmentManager segmentManager;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventRouter router;
    private final ProducerConfig config;
    @GuardedBy("lock")
    private final Map<SegmentId, SegmentProducer<Type>> producers = new HashMap<>();
    private final ControllerApi.Producer apiProducer;

    ProducerImpl(Stream stream, ControllerApi.Producer apiProducer, SegmentManager segmentManager, EventRouter router, Serializer<Type> serializer,
            ProducerConfig config) {
        Preconditions.checkNotNull(stream);
        Preconditions.checkNotNull(router);
        Preconditions.checkNotNull(serializer);
        this.apiProducer = apiProducer;
        this.segmentManager = segmentManager;
        this.stream = stream;
        this.router = router;
        this.serializer = serializer;
        this.config = config;
        synchronized (lock) {
            List<Event<Type>> list = setupSegmentProducers();
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
    private List<Event<Type>> setupSegmentProducers() {
        StreamSegments segments = Retry.withExpBackoff(1, 10, 5)
            .retryingOn(SegmentSealedException.class)
            .throwingOn(RuntimeException.class)
            .run(() -> {
                StreamSegments s = stream.getLatestSegments();
                for (SegmentId segment : s.getSegments()) {
                    if (!producers.containsKey(segment)) {
                        SegmentOutputStream out = segmentManager.openSegmentForAppending(segment.getQualifiedName(),
                                                                                         config.getSegmentConfig());
                        producers.put(segment, new SegmentProducerImpl<>(out, serializer));
                    }
                }
                return s;
            });
        List<Event<Type>> toResend = new ArrayList<>();

        Iterator<Entry<SegmentId, SegmentProducer<Type>>> iter = producers.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<SegmentId, SegmentProducer<Type>> entry = iter.next();
            if (!segments.segments.contains(entry.getKey())) {
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
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (lock) {
            if (!attemptPublish(new Event<Type>(event, routingKey, result))) {
                handleLogSealed();
            }
        }
        return result;
    }

    /**
     * If a log sealed is encountered, we need to 1. Find the new segments to produce to. 2. For each outstanding
     * message find which new segment it should go to and send it there. This can happen recursively if segments turn
     * over very quickly.
     */
    private void handleLogSealed() {
        List<Event<Type>> toResend = setupSegmentProducers();
        while (toResend.isEmpty()) {
            List<Event<Type>> unsent = new ArrayList<>();
            for (Event<Type> event : toResend) {
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

    private boolean attemptPublish(Event<Type> event) {
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
        SegmentId log = router.getSegmentForEvent(stream, routingKey);
        return producers.get(log);
    }

    private class TransactionImpl implements Transaction<Type> {

        private final Map<SegmentId, SegmentTransaction<Type>> inner;
        private final UUID txId;

        TransactionImpl(UUID txId, Map<SegmentId, SegmentTransaction<Type>> transactions) {
            this.txId = txId;
            this.inner = transactions;
        }

        @Override
        public void publish(String routingKey, Type event) throws TxFailedException {
            SegmentId s = router.getSegmentForEvent(stream, routingKey);
            SegmentTransaction<Type> transaction = inner.get(s);
            transaction.publish(event);
        }

        @Override
        public void commit() throws TxFailedException {
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.flush();
            }
            segmentManager.commitTransaction(txId);
        }

        @Override
        public void drop() {
            segmentManager.dropTransaction(txId);
        }

        @Override
        public Status checkStatus() {
            return segmentManager.checkTransactionStatus(txId);
        }

        @Override
        public void flush() throws TxFailedException {
            for (SegmentTransaction<Type> tx : inner.values()) {
                tx.flush();
            }
        }

    }

    @Override
    public Transaction<Type> startTransaction(long timeout) {
        UUID txId = UUID.randomUUID();
        Map<SegmentId, SegmentTransaction<Type>> transactions = new HashMap<>();
        ArrayList<SegmentId> segmentIds;
        synchronized (lock) {
            segmentIds = new ArrayList<>(producers.keySet());
        }
        for (SegmentId s : segmentIds) {
            segmentManager.createTransaction(s.getName(), txId, timeout);
            SegmentOutputStream out = segmentManager.openTransactionForAppending(s.getName(), txId);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl(txId, transactions);
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
