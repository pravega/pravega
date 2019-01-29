/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.shared.segment.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamingException;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;

/**
 * Store adapter wrapping a real Pravega Client.
 */
abstract class ClientAdapterBase extends StoreAdapter {
    //region Members
    static final String SCOPE = "SelfTest";
    static final ByteArraySerializer SERIALIZER = new ByteArraySerializer();
    private static final long TXN_TIMEOUT = 30 * 1000;
    private static final long TXN_MAX_EXEC_TIME = TXN_TIMEOUT;
    private static final EventWriterConfig WRITER_CONFIG = EventWriterConfig.builder()
                                                                            .transactionTimeoutTime(TXN_MAX_EXEC_TIME)
                                                                            .build();
    final TestConfig testConfig;
    private final ScheduledExecutorService testExecutor;
    private final ConcurrentHashMap<String, List<EventStreamWriter<byte[]>>> streamWriters;
    private final ConcurrentHashMap<String, List<TransactionalEventStreamWriter<byte[]>>> transactionalWriters;
    private final ConcurrentHashMap<String, UUID> transactionIds;
    private final AtomicReference<ClientReader> clientReader;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ClientAdapterBase class.
     *
     * @param testConfig   The TestConfig to use.
     * @param testExecutor An Executor to use for async client-side operations.
     */
    ClientAdapterBase(TestConfig testConfig, ScheduledExecutorService testExecutor) {
        this.testConfig = Preconditions.checkNotNull(testConfig, "testConfig");
        this.testExecutor = Preconditions.checkNotNull(testExecutor, "testExecutor");
        this.streamWriters = new ConcurrentHashMap<>();
        this.transactionalWriters = new ConcurrentHashMap<>();
        this.transactionIds = new ConcurrentHashMap<>();
        this.clientReader = new AtomicReference<>();
    }

    //endregion

    //region StoreAdapter Implementation

    @Override
    public boolean isFeatureSupported(Feature feature) {
        // Derived classes will indicate which features they do support.
        return true;
    }

    @Override
    protected void startUp() throws Exception {
        if (isFeatureSupported(Feature.TailRead)) {
            this.clientReader.set(new ClientReader(new URI(getControllerUrl()), this.testConfig, getClientFactory(), this.testExecutor));
        }
    }

    @Override
    protected void shutDown() {
        ClientReader reader = this.clientReader.getAndSet(null);
        if (reader != null) {
            reader.close();
        }

        this.streamWriters.values().forEach(l -> l.forEach(EventStreamWriter::close));
        this.streamWriters.clear();
        this.transactionalWriters.values().forEach(l -> l.forEach(TransactionalEventStreamWriter::close));
        this.transactionalWriters.clear();
    }

    @Override
    public CompletableFuture<Void> createStream(String streamName, Duration timeout) {
        ensureRunning();
        return CompletableFuture.runAsync(() -> {
            if (this.streamWriters.containsKey(streamName)) {
                throw new CompletionException(new StreamSegmentExistsException(streamName));
            }

            StreamConfiguration config = StreamConfiguration
            .builder()
            .scalingPolicy(ScalingPolicy.fixed(this.testConfig.getSegmentsPerStream()))
                    .build();
            if (!getStreamManager().createStream(SCOPE, streamName, config)) {
                throw new CompletionException(new StreamingException(String.format("Unable to create Stream '%s'.", streamName)));
            }

            int writerCount = Math.max(1, this.testConfig.getProducerCount() / this.testConfig.getStreamCount());
            List<EventStreamWriter<byte[]>> writers = new ArrayList<>(writerCount);
            if (this.streamWriters.putIfAbsent(streamName, writers) == null) {
                for (int i = 0; i < writerCount; i++) {
                    writers.add(getClientFactory().createEventWriter(streamName, SERIALIZER, WRITER_CONFIG));
                }
            }
            List<TransactionalEventStreamWriter<byte[]>> txnWriters = new ArrayList<>(writerCount);
            if (this.transactionalWriters.putIfAbsent(streamName, txnWriters) == null) {
                for (int i = 0; i < writerCount; i++) {
                    txnWriters.add(getClientFactory().createTransactionalEventWriter(streamName, SERIALIZER, WRITER_CONFIG));
                }
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(String streamName, Duration timeout) {
        ensureRunning();
        String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(streamName);
        if (isTransaction(streamName, parentName)) {
            // We have a transaction to abort.
            return abortTransaction(streamName, timeout);
        } else {
            return CompletableFuture.runAsync(() -> {
                if (getStreamManager().deleteStream(SCOPE, streamName)) {
                    closeWriters(streamName);
                } else {
                    throw new CompletionException(new StreamingException(String.format("Unable to delete stream '%s'.", streamName)));
                }
            }, this.testExecutor);
        }
    }

    @Override
    public CompletableFuture<Void> append(String streamName, Event event, Duration timeout) {
        ensureRunning();
        ArrayView s = event.getSerialization();
        byte[] payload = s.arrayOffset() == 0 ? s.array() : Arrays.copyOfRange(s.array(), s.arrayOffset(), s.getLength());
        String routingKey = Integer.toString(event.getRoutingKey());
        String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(streamName);
        if (isTransaction(streamName, parentName)) {
            // Dealing with a Transaction.
            return CompletableFuture.runAsync(() -> {
                try {
                    UUID txnId = getTransactionId(streamName);
                    getTransactionalWriter(parentName, event.getRoutingKey()).getTxn(txnId).writeEvent(routingKey, payload);
                } catch (Exception ex) {
                    this.transactionIds.remove(streamName);
                    throw new CompletionException(ex);
                }
            }, this.testExecutor);
        } else {
            try {
                return getWriter(streamName, event.getRoutingKey()).writeEvent(routingKey, payload);
            } catch (Exception ex) {
                return Futures.failedFuture(ex);
            }
        }
    }

    @Override
    public CompletableFuture<Void> seal(String streamName, Duration timeout) {
        ensureRunning();
        return CompletableFuture.runAsync(() -> {
            if (getStreamManager().sealStream(SCOPE, streamName)) {
                closeWriters(streamName);
            } else {
                throw new CompletionException(new StreamingException(String.format("Unable to seal stream '%s'.", streamName)));
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStream, Duration timeout) {
        ensureRunning();
        return CompletableFuture.supplyAsync(() -> {
            TransactionalEventStreamWriter<byte[]> writer = getTransactionalWriter(parentStream, 0);
            UUID txnId = writer.beginTxn().getTxnId();
            String txnName = StreamSegmentNameUtils.getTransactionNameFromId(parentStream, txnId);
            this.transactionIds.put(txnName, txnId);
            return txnName;
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureRunning();
        String parentStream = StreamSegmentNameUtils.getParentStreamSegmentName(transactionName);
        return CompletableFuture.runAsync(() -> {
            try {
                TransactionalEventStreamWriter<byte[]> writer = getTransactionalWriter(parentStream, 0);
                UUID txnId = getTransactionId(transactionName);
                Transaction<byte[]> txn = writer.getTxn(txnId);
                txn.commit();
            } catch (TxnFailedException ex) {
                throw new CompletionException(ex);
            } finally {
                this.transactionIds.remove(transactionName);
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(String transactionName, Duration timeout) {
        ensureRunning();
        String parentStream = StreamSegmentNameUtils.getParentStreamSegmentName(transactionName);
        return CompletableFuture.runAsync(() -> {
            try {
                TransactionalEventStreamWriter<byte[]> writer = getTransactionalWriter(parentStream, 0);
                UUID txnId = getTransactionId(transactionName);
                Transaction<byte[]> txn = writer.getTxn(txnId);
                txn.abort();
            } finally {
                this.transactionIds.remove(transactionName);
            }
        }, this.testExecutor);
    }

    @Override
    public StoreReader createReader() {
        ClientReader reader = this.clientReader.get();
        if (reader == null) {
            throw new UnsupportedOperationException("reading is not supported on this adapter.");
        }

        return reader;
    }

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return null;
    }

    //endregion

    //region Helper methods

    /**
     * Gets a reference to the Stream Manager.
     */
    protected abstract StreamManager getStreamManager();

    /**
     * Gets a reference to the ClientFactory used to create EventStreamWriters and EventStreamReaders.
     */
    protected abstract EventStreamClientFactory getClientFactory();

    /**
     * Gets a String representing the URL to the Controller.
     */
    protected abstract String getControllerUrl();

    private void closeWriters(String streamName) {
        List<EventStreamWriter<byte[]>> writers = this.streamWriters.remove(streamName);
        if (writers != null) {
            writers.forEach(EventStreamWriter::close);
        }
        List<TransactionalEventStreamWriter<byte[]>> txnWriters = this.transactionalWriters.remove(streamName);
        if (txnWriters != null) {
            txnWriters.forEach(TransactionalEventStreamWriter::close);
        }
    }

    @SneakyThrows(StreamSegmentNotExistsException.class)
    private UUID getTransactionId(String transactionName) {
        UUID txnId = this.transactionIds.getOrDefault(transactionName, null);
        if (txnId == null) {
            throw new StreamSegmentNotExistsException(transactionName);
        }

        return txnId;
    }

    @SneakyThrows(StreamSegmentNotExistsException.class)
    private TransactionalEventStreamWriter<byte[]> getTransactionalWriter(String streamName, int routingKey) {
        List<TransactionalEventStreamWriter<byte[]>> writers = this.transactionalWriters.getOrDefault(streamName, null);
        if (writers == null) {
            throw new StreamSegmentNotExistsException(streamName);
        }

        return writers.get(routingKey % writers.size());
    }

    @SneakyThrows(StreamSegmentNotExistsException.class)
    private EventStreamWriter<byte[]> getWriter(String streamName, int routingKey) {
        List<EventStreamWriter<byte[]>> writers = this.streamWriters.getOrDefault(streamName, null);
        if (writers == null) {
            throw new StreamSegmentNotExistsException(streamName);
        }

        return writers.get(routingKey % writers.size());
    }

    private boolean isTransaction(String streamName, String parentName) {
        return parentName != null && parentName.length() < streamName.length();
    }

    //endregion
}

