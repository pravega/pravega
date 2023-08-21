/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.selftest.adapters;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.AppendProcessor;
import io.pravega.segmentstore.server.host.handler.ConnectionTracker;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.ServerConnection;
import io.pravega.segmentstore.server.host.handler.TrackedConnection;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.segment.ScaleType;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.val;

/**
 * {@link StoreAdapter} that connects to an {@link AppendProcessor} that uses the Segment Store via {@link SegmentStoreAdapter}.
 * Useful to test the "server-side" of things without the Client interfering.
 */
public class AppendProcessorAdapter extends StoreAdapter {
    //region Members

    private final SegmentStoreAdapter segmentStoreAdapter;
    private final TestConfig testConfig;
    @GuardedBy("handlers")
    private final HashMap<String, SegmentHandler> handlers;
    private final ConnectionTracker connectionTracker;
    private AutoScaleMonitor autoScaleMonitor;
    private final ScheduledExecutorService testExecutor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AppendProcessorAdapter class.
     *
     * @param testConfig    The TestConfig to use.
     * @param builderConfig The ServiceBuilderConfig to use.
     * @param testExecutor  An Executor to use for test-related async operations.
     */
    AppendProcessorAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService testExecutor) {
        this.testConfig = testConfig;
        this.segmentStoreAdapter = new SegmentStoreAdapter(testConfig, builderConfig, testExecutor);
        this.handlers = new HashMap<>();
        this.connectionTracker = new ConnectionTracker();
        this.testExecutor = testExecutor;
    }

    //endregion

    //region StoreAdapter Implementation

    @Override
    protected void startUp() throws Exception {
        this.segmentStoreAdapter.startUp();
        this.autoScaleMonitor = new AutoScaleMonitor(this.segmentStoreAdapter.getStreamSegmentStore(), AutoScalerConfig.builder().build());
    }

    @Override
    protected void shutDown() {
        this.segmentStoreAdapter.shutDown();
        if (this.autoScaleMonitor != null) {
            this.autoScaleMonitor.close();
            this.autoScaleMonitor = null;
        }
    }

    @Override
    public ExecutorServiceHelpers.Snapshot getStorePoolSnapshot() {
        return this.segmentStoreAdapter.getStorePoolSnapshot();
    }

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return feature == Feature.Append
                || feature == Feature.CreateStream
                || feature == Feature.DeleteStream;
    }

    @Override
    public CompletableFuture<Void> append(String streamName, Event event, Duration timeout) {
        String segmentName = streamToSegment(streamName);
        SegmentHandler handler;
        synchronized (this.handlers) {
            handler = this.handlers.get(segmentName);
        }
        if (handler == null) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
        }

        return handler.append(event);
    }

    @Override
    public CompletableFuture<Void> createStream(String streamName, Duration timeout) {
        String segmentName = streamToSegment(streamName);
        val attributes = Arrays.asList(
                new AttributeUpdate(Attributes.SCALE_POLICY_TYPE, AttributeUpdateType.Replace, ScaleType.NoScaling.getValue()),
                new AttributeUpdate(Attributes.SCALE_POLICY_RATE, AttributeUpdateType.Replace, 0L));
        return this.segmentStoreAdapter
                .getStreamSegmentStore()
                .createStreamSegment(segmentName, SegmentType.STREAM_SEGMENT, attributes, timeout)
                .thenRun(() -> {
                    SegmentHandler handler = new SegmentHandler(segmentName, this.testConfig.getProducerCount(), this.segmentStoreAdapter.getStreamSegmentStore());
                    synchronized (this.handlers) {
                        this.handlers.put(segmentName, handler);
                    }

                    handler.initialize();
                });
    }

    @Override
    public CompletableFuture<Void> deleteStream(String streamName, Duration timeout) {
        String segmentName = streamToSegment(streamName);
        return this.segmentStoreAdapter
                .getStreamSegmentStore()
                .deleteStreamSegment(segmentName, timeout)
                .thenRun(() -> {
                    synchronized (this.handlers) {
                        this.handlers.remove(segmentName);
                    }
                });
    }

    private String streamToSegment(String streamName) {
        return streamName + "/segment0";
    }

    //endregion

    //region Unimplemented Methods

    @Override
    public StoreReader createReader() {
        throw new UnsupportedOperationException("createReader");
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStream, Duration timeout) {
        throw new UnsupportedOperationException("createTransaction");
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        throw new UnsupportedOperationException("mergeTransaction");
    }

    @Override
    public CompletableFuture<Void> abortTransaction(String transactionName, Duration timeout) {
        throw new UnsupportedOperationException("abortTransaction");
    }

    @Override
    public CompletableFuture<Void> sealStream(String streamName, Duration timeout) {
        throw new UnsupportedOperationException("sealStream");
    }

    @Override
    public CompletableFuture<Void> createTable(String tableName, Duration timeout) {
        throw new UnsupportedOperationException("createTable");
    }

    @Override
    public CompletableFuture<Void> deleteTable(String tableName, Duration timeout) {
        throw new UnsupportedOperationException("deleteTable");
    }

    @Override
    public CompletableFuture<Long> updateTableEntry(String tableName, BufferView key, BufferView value, Long compareVersion, Duration timeout) {
        throw new UnsupportedOperationException("updateTableEntry");
    }

    @Override
    public CompletableFuture<Void> removeTableEntry(String tableName, BufferView key, Long compareVersion, Duration timeout) {
        throw new UnsupportedOperationException("removeTableEntry");
    }

    @Override
    public CompletableFuture<List<BufferView>> getTableEntries(String tableName, List<BufferView> keys, Duration timeout) {
        throw new UnsupportedOperationException("getTableEntries");
    }

    @Override
    public CompletableFuture<AsyncIterator<List<Map.Entry<BufferView, BufferView>>>> iterateTableEntries(String tableName, Duration timeout) {
        throw new UnsupportedOperationException("iterateTableEntries");
    }

    //endregion

    //region SegmentHandler

    private class SegmentHandler implements ServerConnection {
        private final String segmentName;
        private final AppendProcessor appendProcessor;
        private final int producerCount;
        @GuardedBy("resultFutures")
        private final Map<UUID, Map<Long, CompletableFuture<Void>>> resultFutures;
        @GuardedBy("resultFutures")
        private long nextSequence;
        @GuardedBy("resultFutures")
        private CompletableFuture<Void> pause;
        private final AtomicReference<CompletableFuture<Void>> appendSetup;

        SegmentHandler(String segmentName, int producerCount, StreamSegmentStore segmentStore) {
            this.segmentName = segmentName;
            this.producerCount = producerCount;
            IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(testExecutor, segmentStore);
            this.appendProcessor = AppendProcessor.defaultBuilder(indexAppendProcessor)
                                                  .store(segmentStore)
                                                  .connection(new TrackedConnection(this, connectionTracker))
                                                  .statsRecorder(autoScaleMonitor.getStatsRecorder())
                                                  .build();
            this.nextSequence = 1;
            this.resultFutures = new HashMap<>();
            this.appendSetup = new AtomicReference<>();
        }

        void initialize() {
            // Not very efficient, but does the job and is only executed once, upon initialization.
            for (int i = 0; i < this.producerCount; i++) {
                this.appendSetup.set(new CompletableFuture<>());
                this.appendProcessor.setupAppend(new WireCommands.SetupAppend(0, getWriterId(i), this.segmentName, null));
                this.appendSetup.get().join();
            }
            this.appendSetup.set(null);
        }

        CompletableFuture<Void> append(Event event) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            CompletableFuture<Void> p;
            synchronized (this.resultFutures) {
                p = this.pause;
            }

            if (p == null) {
                appendInternal(event, result);
            } else {
                p.thenRun(() -> appendInternal(event, result));
            }

            return result;
        }

        private void appendInternal(Event event, CompletableFuture<Void> result) {
            WireCommands.Event e = new WireCommands.Event(event.getWriteBuffer().retain());
            synchronized (this.resultFutures) {
                // Event.getRoutingKey() is the ProducerId. We can use it to simulate different Writer Ids.
                UUID writerId = getWriterId(event.getRoutingKey());
                Map<Long, CompletableFuture<Void>> writerResultFutures = this.resultFutures.getOrDefault(writerId, null);
                if (writerResultFutures == null) {
                    writerResultFutures = new HashMap<>();
                    this.resultFutures.put(writerId, writerResultFutures);
                }

                writerResultFutures.put(this.nextSequence, result);
                this.appendProcessor.append(new Append(this.segmentName, getWriterId(event.getRoutingKey()), this.nextSequence, e, 0));
                this.nextSequence++;
            }
        }

        private UUID getWriterId(int producerId) {
            return new UUID(0, producerId);
        }

        //region ServerConnection Implementation

        @Override
        public void send(WireCommand cmd) {
            testExecutor.execute(() -> {
                if (cmd instanceof WireCommands.DataAppended) {
                    val ack = (WireCommands.DataAppended) cmd;
                    val results = new ArrayList<CompletableFuture<Void>>();
                    synchronized (this.resultFutures) {
                        val writerResultFutures = this.resultFutures.get(ack.getWriterId());
                        long startEventNumber = Math.max(0, ack.getPreviousEventNumber()) + 1;
                        for (long eventNumber = startEventNumber; eventNumber <= ack.getEventNumber(); eventNumber++) {
                            val f = writerResultFutures.remove(eventNumber);
                            if (f != null) {
                                results.add(f);
                            }
                        }

                    }
                    results.forEach(c -> c.complete(null));
                } else if (cmd instanceof WireCommands.AppendSetup) {
                    this.appendSetup.get().complete(null);
                }
            });
        }

        @Override
        public void pauseReading() {
            synchronized (this.resultFutures) {
                if (this.pause == null) {
                    this.pause = new CompletableFuture<>();
                }
            }
        }

        @Override
        public void resumeReading() {
            CompletableFuture<Void> p;
            synchronized (this.resultFutures) {
                p = this.pause;
                this.pause = null;
            }
            if (p != null) {
                p.complete(null);
            }
        }

        @Override
        public void close() {
            // Not used.
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void setRequestProcessor(RequestProcessor cp) {
            // Not used.
        }

        //endregion
    }

    //endregion
}
