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
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.segment.StreamSegmentNameUtils;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamingException;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;

/**
 * Store adapter wrapping a real Pravega Client.
 */
abstract class ClientAdapterBase implements StoreAdapter {
    //region Members
    static final String SCOPE = "SelfTest";
    private static final long TXN_TIMEOUT = Long.MAX_VALUE;
    private static final long TXN_MAX_EXEC_TIME = Long.MAX_VALUE;
    private static final long TXN_SCALE_GRACE_PERIOD = Long.MAX_VALUE;
    private static final ByteArraySerializer SERIALIZER = new ByteArraySerializer();
    private static final EventWriterConfig WRITER_CONFIG = EventWriterConfig.builder().build();
    private static final String LOG_ID = "ClientAdapter";
    final TestConfig testConfig;
    private final ScheduledExecutorService testExecutor;
    private final ConcurrentHashMap<String, EventStreamWriter<byte[]>> streamWriters;
    private final ConcurrentHashMap<String, UUID> transactionIds;
    private final AtomicBoolean closed;
    private final AtomicBoolean initialized;

    //endregion

    /**
     * Creates a new instance of the ClientAdapterBase class.
     *
     * @param testConfig    The TestConfig to use.
     */
    ClientAdapterBase(TestConfig testConfig, ScheduledExecutorService testExecutor) {
        this.testConfig = testConfig;
        this.testExecutor = Preconditions.checkNotNull(testExecutor, "testExecutor");
        this.streamWriters = new ConcurrentHashMap<>();
        this.transactionIds = new ConcurrentHashMap<>();
        this.closed = new AtomicBoolean();
        this.initialized = new AtomicBoolean();

    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.streamWriters.values().forEach(EventStreamWriter::close);
            this.streamWriters.clear();

            TestLogger.log(LOG_ID, "Closed.");
        }
    }

    //endregion

    //region StoreAdapter Implementation

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return true;
    }

    @Override
    public void initialize() throws Exception {
        Preconditions.checkState(!this.initialized.getAndSet(true), "Cannot call initialize() after initialization happened.");
        this.initialized.set(true);
    }

    @Override
    public CompletableFuture<Void> createStream(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            if (this.streamWriters.containsKey(streamName)) {
                throw new CompletionException(new StreamSegmentExistsException(streamName));
            }

            StreamConfiguration config = StreamConfiguration.builder()
                                                            .streamName(streamName)
                                                            .scope(SCOPE)
                                                            .build();
            if (!getStreamManager().createStream(SCOPE, streamName, config)) {
                throw new CompletionException(new StreamingException(String.format("Unable to create Stream '%s'.", streamName)));
            }

            EventStreamWriter<byte[]> writer = getClientFactory().createEventWriter(streamName, SERIALIZER, WRITER_CONFIG);
            this.streamWriters.putIfAbsent(streamName, writer);
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> delete(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            if (getStreamManager().deleteStream(SCOPE, streamName)) {
                closeWriter(streamName);
            } else {
                throw new CompletionException(new StreamingException(String.format("Unable to delete stream '%s'.", streamName)));
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> append(String streamName, Event event, Duration timeout) {
        ensureInitializedAndNotClosed();
        ArrayView s = event.getSerialization();
        byte[] payload = s.arrayOffset() == 0 ? s.array() : Arrays.copyOfRange(s.array(), s.arrayOffset(), s.getLength());
        String routingKey = Integer.toString(event.getRoutingKey());
        String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(streamName);
        if (parentName == null || parentName.length() == streamName.length()) {
            return getWriter(streamName).writeEvent(routingKey, payload);
        } else {
            // Dealing with a Transaction.
            UUID txnId = getTransactionId(streamName);
            return CompletableFuture.runAsync(() -> {
                try {
                    getWriter(parentName).getTxn(txnId).writeEvent(routingKey, payload);
                } catch (TxnFailedException ex) {
                    this.transactionIds.remove(streamName);
                    throw new CompletionException(ex);
                }
            }, this.testExecutor);
        }
    }

    @Override
    public CompletableFuture<Void> seal(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            if (getStreamManager().sealStream(SCOPE, streamName)) {
                closeWriter(streamName);
            } else {
                throw new CompletionException(new StreamingException(String.format("Unable to seal stream '%s'.", streamName)));
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<SegmentProperties> getInfo(String streamName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("getInfo is not supported.");
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStream, Duration timeout) {
        ensureInitializedAndNotClosed();
        EventStreamWriter<byte[]> writer = getWriter(parentStream);
        return CompletableFuture.supplyAsync(() -> {
            UUID txnId = writer.beginTxn(TXN_TIMEOUT, TXN_MAX_EXEC_TIME, TXN_SCALE_GRACE_PERIOD).getTxnId();
            String txnName = StreamSegmentNameUtils.getTransactionNameFromId(parentStream, txnId);
            this.transactionIds.put(txnName, txnId);
            return txnName;
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureInitializedAndNotClosed();
        String parentStream = StreamSegmentNameUtils.getParentStreamSegmentName(transactionName);
        EventStreamWriter<byte[]> writer = getWriter(parentStream);
        UUID txnId = getTransactionId(transactionName);

        return CompletableFuture.runAsync(() -> {
            Transaction<byte[]> txn = writer.getTxn(txnId);
            try {
                txn.commit();
            } catch (TxnFailedException ex) {
                throw new CompletionException(ex);
            } finally {
                this.transactionIds.remove(transactionName);
            }
        }, this.testExecutor);
    }

    @Override
    public StoreReader createReader() {
        throw new UnsupportedOperationException();
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
    protected abstract ClientFactory getClientFactory();

    private void closeWriter(String streamName) {
        EventStreamWriter<byte[]> writer = this.streamWriters.remove(streamName);
        if (writer != null) {
            writer.close();
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
    private EventStreamWriter<byte[]> getWriter(String streamName) {
        EventStreamWriter<byte[]> writer = this.streamWriters.getOrDefault(streamName, null);
        if (writer == null) {
            throw new StreamSegmentNotExistsException(streamName);
        }

        return writer;
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.initialized.get(), "initialize() must be called before invoking this operation.");
    }

    //endregion
}

