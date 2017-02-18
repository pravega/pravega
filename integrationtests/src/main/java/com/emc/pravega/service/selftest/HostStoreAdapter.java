/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.selftest;

import com.emc.pravega.service.contracts.AttributeUpdate;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.impl.ByteArraySerializer;
import com.emc.pravega.stream.mock.MockStreamManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.val;

/**
 * Store adapter wrapping a real StreamSegmentStore and Connection Listener.
 */
public class HostStoreAdapter extends StreamSegmentStoreAdapter {
    private static final String LOG_ID = "HostStoreAdapter";
    private static final String SCOPE = "scope";
    private static final String LISTENING_ADDRESS = "localhost";
    private final int listeningPort;
    private final boolean autoFlush;
    private final int writerCount;
    private final ConcurrentHashMap<String, WriterCollection> writers;
    private PravegaConnectionListener listener;
    private MockStreamManager streamManager;

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
        this.writerCount = testConfig.getClientWriterCount();
        this.writers = new ConcurrentHashMap<>();
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.writers.values().forEach(WriterCollection::close);
        this.writers.clear();

        if (this.streamManager != null) {
            this.streamManager.close();
            this.streamManager = null;
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
                        this.streamManager = new MockStreamManager(SCOPE, LISTENING_ADDRESS, this.listeningPort);
                        TestLogger.log(LOG_ID, "Initialized.");
                    });
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            if (this.writers.containsKey(streamSegmentName)) {
                throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
            }

            this.streamManager.createStream(streamSegmentName, null);
            WriterCollection producers = new WriterCollection(streamSegmentName, this.writerCount, this.streamManager);
            this.writers.putIfAbsent(streamSegmentName, producers);
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("deleteStreamSegment is not supported.");
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureInitializedAndNotClosed();
        return CompletableFuture.runAsync(() -> {
            WriterCollection segmentWriterCollection = this.writers.getOrDefault(streamSegmentName, null);
            if (segmentWriterCollection == null) {
                throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
            }

            EventStreamWriter<byte[]> writer = segmentWriterCollection.next();
            Future<Void> r = writer.writeEvent(streamSegmentName, data);
            if (this.autoFlush) {
                writer.flush();
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
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("transactions are not supported.");
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureInitializedAndNotClosed();
        throw new UnsupportedOperationException("transactions are not supported.");
    }

    //endregion

    private static class WriterCollection implements AutoCloseable {
        private static final ByteArraySerializer SERIALIZER = new ByteArraySerializer();
        private static final EventWriterConfig WRITER_CONFIG = new EventWriterConfig(null);
        private final List<EventStreamWriter<byte[]>> writers;
        private final AtomicInteger nextWriterId;

        WriterCollection(String segmentName, int count, MockStreamManager streamManager) {
            this.writers = Collections.synchronizedList(new ArrayList<>(count));
            this.nextWriterId = new AtomicInteger();
            for (int i = 0; i < count; i++) {
                val writer = streamManager.getClientFactory()
                                          .createEventWriter(segmentName,
                                                  SERIALIZER,
                                                  WRITER_CONFIG);
                this.writers.add(writer);
            }
        }

        EventStreamWriter<byte[]> next() {
            return this.writers.get(this.nextWriterId.getAndIncrement() % this.writers.size());
        }

        @Override
        public void close() {
            this.writers.forEach(EventStreamWriter::close);
        }
    }
}
