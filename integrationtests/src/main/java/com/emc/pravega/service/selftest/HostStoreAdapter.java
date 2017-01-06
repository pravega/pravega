/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.selftest;

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Store adapter wrapping a real StreamSegmentStore and Connection Listener.
 */
public class HostStoreAdapter extends StreamSegmentStoreAdapter {
    private static final String LOG_ID = "HostStoreAdapter";
    private static final String SCOPE = "scope";
    private static final String LISTENING_ADDRESS = "localhost";
    private final int listeningPort;
    private final boolean autoFlush;
    private final ConcurrentHashMap<String, EventStreamWriter<byte[]>> producers;
    private PravegaConnectionListener listener;
    private MockClientFactory clientFactory;

    /**
     * Creates a new instance of the HostStoreAdapter class.
     *
     * @param testConfig    The TestConfig to use.
     * @param builderConfig The ServiceBuilderConfig to use.
     * @param testExecutor  An Executor to use for test-related async operations.
     */
    HostStoreAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, Executor testExecutor) {
        super(testConfig, builderConfig, testExecutor);
        this.listeningPort = testConfig.getClientPort();
        this.autoFlush = testConfig.isClientAutoFlush();
        this.producers = new ConcurrentHashMap<>();
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.producers.values().forEach(EventStreamWriter::close);
        this.producers.clear();

        if (this.clientFactory != null) {
            this.clientFactory.close();
            this.clientFactory = null;
        }

        if (this.listener != null) {
            this.listener.close();
            this.listener = null;
        }

        TestLogger.log(LOG_ID, "Closed.");
        super.close();
    }

    //endregion

    //region StoreAdapter Implementation

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return feature == Feature.Create
                || feature == Feature.Append;
    }

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        return super.initialize(timeout)
                    .thenAccept(v -> {
                        this.listener = new PravegaConnectionListener(false, this.listeningPort, getStreamSegmentStore());
                        this.listener.startListening();
                        this.clientFactory = new MockClientFactory(SCOPE, LISTENING_ADDRESS, this.listeningPort);
                        TestLogger.log(LOG_ID, "Initialized.");
                    });
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            if (this.producers.containsKey(streamSegmentName)) {
                throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
            }

            this.clientFactory.createStream(streamSegmentName, null);
            EventStreamWriter<byte[]> producer = this.clientFactory.createEventWriter(streamSegmentName, new JavaSerializer<>(), new EventWriterConfig(null));
            this.producers.put(streamSegmentName, producer);
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("deleteStreamSegment is not supported.");
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, AppendContext context, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            EventStreamWriter<byte[]> producer = this.producers.getOrDefault(streamSegmentName, null);
            if (producer == null) {
                throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
            }

            Future<Void> r = producer.writeEvent(streamSegmentName, data);
            if (this.autoFlush) {
                producer.flush();
            }

            try {
                r.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> sealStreamSegment(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("sealStreamSegment is not supported.");
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("getStreamSegmentInfo is not supported.");
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("read is not supported.");
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("transactions are not supported.");
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("transactions are not supported.");
    }

    //endregion
}
