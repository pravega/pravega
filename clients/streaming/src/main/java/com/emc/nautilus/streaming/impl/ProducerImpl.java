package com.emc.nautilus.streaming.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.emc.nautilus.logclient.LogServiceClient;
import com.emc.nautilus.logclient.SegmentOutputStream;
import com.emc.nautilus.logclient.SegmentSealedExcepetion;
import com.emc.nautilus.streaming.EventRouter;
import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.SegmentId;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.StreamSegments;
import com.emc.nautilus.streaming.Transaction;
import com.emc.nautilus.streaming.TxFailedException;

public class ProducerImpl<Type> implements Producer<Type> {

    private final TransactionManager txManager;
    private final Stream stream;
    private final Serializer<Type> serializer;
    private final LogServiceClient logClient;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final EventRouter router;
    private final ProducerConfig config;
    private final Map<SegmentId, SegmentProducer<Type>> producers = new HashMap<>();

    ProducerImpl(TransactionManager txManager, Stream stream, LogServiceClient logClient, EventRouter router,
            Serializer<Type> serializer, ProducerConfig config) {
        this.txManager = txManager;
        this.logClient = logClient;
        this.stream = stream;
        this.router = router;
        this.serializer = serializer;
        this.config = config;
        List<Event<Type>> list = setupLogProducers();
        if (!list.isEmpty()) {
            throw new IllegalStateException("Producer initialized with unsent messages?!");
        }
    }

    private List<Event<Type>> setupLogProducers() {
        StreamSegments logs = stream.getLatestSegments();
        List<SegmentId> newSegments = new ArrayList<>(logs.segments);
        newSegments.removeAll(producers.keySet());
        List<SegmentId> oldLogs = new ArrayList<>(producers.keySet());
        oldLogs.removeAll(logs.segments);

        for (SegmentId segment : newSegments) {
            SegmentOutputStream log = logClient.openSegmentForAppending(segment.getQualifiedName(),
                                                                        config.getSegmentConfig());
            producers.put(segment, new SegmentProducerImpl<>(log, serializer));
        }
        List<Event<Type>> toResend = new ArrayList<>();
        for (SegmentId l : oldLogs) {
            SegmentProducer<Type> producer = producers.remove(l);
            try {
                producer.close();
            } catch (SegmentSealedExcepetion e) {
                // Suppressing expected exception
            }
            toResend.addAll(producer.getUnackedEvents());
        }
        return toResend;
    }

    @Override
    public Future<Void> publish(String routingKey, Type event) {
        if (closed.get()) {
            throw new IllegalStateException("Producer closed");
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (producers) {
            if (!attemptPublish(new Event<>(routingKey, event, result))) {
                handleLogSealed();
            }
        }
        return result;
    }

    private void handleLogSealed() {
        List<Event<Type>> toResend = setupLogProducers();
        while (toResend.isEmpty()) {
            List<Event<Type>> unsent = new ArrayList<>();
            for (Event<Type> event : toResend) {
                if (!attemptPublish(event)) {
                    unsent.add(event);
                }
            }
            if (!unsent.isEmpty()) {
                unsent.addAll(setupLogProducers());
            }
            toResend = unsent;
        }
    }

    private boolean attemptPublish(Event<Type> event) {
        SegmentProducer<Type> log = getLogProducer(event.getRoutingKey());
        if (log == null || log.isAlreadySealed()) {
            return false;
        }
        try {
            log.publish(event);
            return true;
        } catch (SegmentSealedExcepetion e) {
            return false;
        }
    }

    private SegmentProducer<Type> getLogProducer(String routingKey) {
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
            for (SegmentTransaction<Type> log : inner.values()) {
                log.flush();
            }
            txManager.commitTransaction(txId);
        }

        @Override
        public void drop() {
            txManager.dropTransaction(txId);
        }

        @Override
        public Status checkStatus() {
            return txManager.checkTransactionStatus(txId);
        }

    }

    @Override
    public Transaction<Type> startTransaction(long timeout) {
        UUID txId = txManager.createTransaction(stream, timeout);
        Map<SegmentId, SegmentTransaction<Type>> transactions = new HashMap<>();
        ArrayList<SegmentId> segmentIds;
        synchronized (producers) {
            segmentIds = new ArrayList<>(producers.keySet());
        }
        for (SegmentId s : segmentIds) {
            SegmentOutputStream out = logClient.openTransactionForAppending(s.getName(), txId);
            SegmentTransactionImpl<Type> impl = new SegmentTransactionImpl<>(txId, out, serializer);
            transactions.put(s, impl);
        }
        return new TransactionImpl(txId, transactions);
    }

    @Override
    public void flush() {
        if (closed.get()) {
            throw new IllegalStateException("Producer closed");
        }
        boolean success = false;
        while (!success) {
            success = true;
            synchronized (producers) {
                for (SegmentProducer<Type> p : producers.values()) {
                    try {
                        p.flush();
                    } catch (SegmentSealedExcepetion e) {
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
        synchronized (producers) {
            boolean success = false;
            while (!success) {
                success = true;
                for (SegmentProducer<Type> p : producers.values()) {
                    try {
                        p.close();
                    } catch (SegmentSealedExcepetion e) {
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
