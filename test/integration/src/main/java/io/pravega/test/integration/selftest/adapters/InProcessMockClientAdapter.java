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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableFactoryImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.ExtendedChunkInfo;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.contracts.tables.TableSegmentInfo;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.test.common.NoOpScheduledExecutor;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.selftest.TestConfig;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.val;

/**
 * Client-based adapter that targets an in-process Client with a Mock Controller and Mock StreamSegmentStore.
 */
class InProcessMockClientAdapter extends ClientAdapterBase {
    //region Members

    private static final String LISTENING_ADDRESS = "localhost";
    private final ScheduledExecutorService executor;
    private PravegaConnectionListener listener;
    private MockStreamManager streamManager;
    private AutoScaleMonitor autoScaleMonitor;
    private MockKVTManager kvtManager;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the InProcessMockClientAdapter class.
     *
     * @param testConfig   The TestConfig to use.
     * @param testExecutor An Executor to use for test-related async operations.
     */
    InProcessMockClientAdapter(TestConfig testConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
        this.executor = testExecutor;
    }

    //endregion

    //region ClientAdapterBase Implementation

    @Override
    protected void startUp() throws Exception {
        int segmentStorePort = this.testConfig.getSegmentStorePort(0);
        val store = getStreamSegmentStore();
        this.autoScaleMonitor = new AutoScaleMonitor(store, AutoScalerConfig.builder().build());
        IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(executor, store);
        this.listener = new PravegaConnectionListener(false, false, "localhost", segmentStorePort, store,
                                                      getTableStore(), autoScaleMonitor.getStatsRecorder(),
                                                      TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(),
                                                      null, null, false, NoOpScheduledExecutor.get(),
                                                      SecurityConfigDefaults.TLS_PROTOCOL_VERSION, indexAppendProcessor);
        this.listener.startListening();

        this.streamManager = new MockStreamManager(SCOPE, LISTENING_ADDRESS, segmentStorePort);
        this.streamManager.createScope(SCOPE);

        this.kvtManager = new MockKVTManager(this.streamManager);
        super.startUp();
    }

    @Override
    protected void shutDown() {
        super.shutDown();

        if (this.listener != null) {
            this.listener.close();
            this.listener = null;
        }

        if (this.autoScaleMonitor != null) {
            this.autoScaleMonitor.close();
            this.autoScaleMonitor = null;
        }

        if (this.streamManager != null) {
            this.streamManager.close();
            this.streamManager = null;
        }
    }

    @Override
    protected StreamManager getStreamManager() {
        return this.streamManager;
    }

    @Override
    protected EventStreamClientFactory getClientFactory() {
        return this.streamManager.getClientFactory();
    }

    @Override
    protected KeyValueTableManager getKVTManager() {
        return this.kvtManager;
    }

    @Override
    protected KeyValueTableFactory getKVTFactory() {
        return this.kvtManager.getClientFactory();
    }

    @Override
    protected String getControllerUrl() {
        throw new UnsupportedOperationException("getControllerUrl is not supported for Mock implementations.");
    }

    @Override
    public boolean isFeatureSupported(Feature feature) {
        // This uses MockStreamManager, which only supports Create and Append for Streams.
        // Also the MockStreamSegmentStore does not support any other features as well.
        return feature == Feature.CreateStream
                || feature == Feature.Append
                || feature == Feature.Tables;
    }

    protected StreamSegmentStore getStreamSegmentStore() {
        return new MockStreamSegmentStore();
    }

    protected TableStore getTableStore() {
        return new MockTableStore();
    }

    //endregion

    //region MockStreamSegmentStore

    private class MockStreamSegmentStore implements StreamSegmentStore {
        @GuardedBy("lock")
        private final Map<String, Long> segments = new HashMap<>();
        @GuardedBy("lock")
        private final Map<String, Map<AttributeId, Long>> attributes = new HashMap<>();
        private final Object lock = new Object();

        @Override
        public CompletableFuture<Void> createStreamSegment(String streamSegmentName, SegmentType segmentType,
                                                           Collection<AttributeUpdate> attributes, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.lock) {
                    if (this.segments.put(streamSegmentName, 0L) == null) {
                        this.attributes.put(streamSegmentName, new ConcurrentHashMap<>());
                    } else {
                        throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Long> append(String streamSegmentName, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    long offset = this.segments.getOrDefault(streamSegmentName, -1L);
                    if (offset >= 0) {
                        if (attributeUpdates != null) {
                            val segmentAttributes = this.attributes.get(streamSegmentName);
                            attributeUpdates.forEach(au -> segmentAttributes.put(au.getAttributeId(), au.getValue()));
                        }
                        this.segments.put(streamSegmentName, offset + data.getLength());
                        return offset;
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            return append(streamSegmentName, data, attributeUpdates, timeout);
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    long length = this.segments.getOrDefault(streamSegmentName, -1L);
                    if (length >= 0) {
                        return StreamSegmentInformation.builder().name(streamSegmentName)
                                .length(length)
                                .attributes(new HashMap<>(this.attributes.get(streamSegmentName))).build();
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Void> updateAttributes(String streamSegmentName, AttributeUpdateCollection attributeUpdates, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.lock) {
                    val segmentAttributes = this.attributes.get(streamSegmentName);
                    if (attributeUpdates != null) {
                        attributeUpdates.forEach(au -> segmentAttributes.put(au.getAttributeId(), au.getValue()));
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Map<AttributeId, Long>> getAttributes(String streamSegmentName, Collection<AttributeId> attributeIds, boolean cache, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    val segmentAttributes = this.attributes.get(streamSegmentName);
                    if (segmentAttributes != null) {
                        return new HashMap<>(segmentAttributes);
                    } else {
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Void> flushToStorage(int containerId, Duration timeout) {
            throw new UnsupportedOperationException("flushToStorage");
        }

        @Override
        public CompletableFuture<List<ExtendedChunkInfo>> getExtendedChunkInfo(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("getExtendedChunkInfo");
        }

        @Override
        public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
            throw new UnsupportedOperationException("read");
        }

        @Override
        public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String target, String source, Duration timeout) {
            throw new UnsupportedOperationException("mergeStreamSegment");
        }

        @Override
        public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String target, String source,
                                                                              AttributeUpdateCollection attributeUpdates,
                                                                              Duration timeout) {
            throw new UnsupportedOperationException("mergeStreamSegment");
        }

        @Override
        public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("sealStreamSegment");
        }

        @Override
        public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("deleteStreamSegment");
        }

        @Override
        public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
            throw new UnsupportedOperationException("truncateStreamSegment");
        }
    }

    //endregion

    private static class MockKVTManager implements KeyValueTableManager {
        private final MockStreamManager streamManager;
        @Getter
        private final KeyValueTableFactory clientFactory;

        public MockKVTManager(MockStreamManager streamManager) {
            this.streamManager = streamManager;
            this.clientFactory = new KeyValueTableFactoryImpl(streamManager.getScope(), streamManager.getController(), streamManager.getConnectionPool());
        }

        @Override
        public boolean createKeyValueTable(String scopeName, String keyValueTableName, KeyValueTableConfiguration config) {
            NameUtils.validateUserKeyValueTableName(keyValueTableName);
            if (config == null) {
                config = KeyValueTableConfiguration.builder()
                        .partitionCount(1)
                        .build();
            }

            return Futures.getAndHandleExceptions(
                    this.streamManager.getController().createKeyValueTable(scopeName, keyValueTableName, config),
                    RuntimeException::new);
        }

        @Override
        public boolean deleteKeyValueTable(String scopeName, String keyValueTableName) {
            return Futures.getAndHandleExceptions(
                    this.streamManager.getController().deleteKeyValueTable(scopeName, keyValueTableName),
                    RuntimeException::new);
        }

        @Override
        public Iterator<KeyValueTableInfo> listKeyValueTables(String scopeName) {
            throw new UnsupportedOperationException("listKeyValueTables");
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }

    private class MockTableStore implements TableStore {
        @GuardedBy("segments")
        private final Map<String, Map<BufferView, BufferView>> segments = new HashMap<>();
        private final AtomicLong nextVersion = new AtomicLong(0);

        @Override
        public CompletableFuture<Void> createSegment(String segmentName, SegmentType segmentType, TableSegmentConfig config, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.segments) {
                    if (this.segments.containsKey(segmentName)) {
                        throw new CompletionException(new StreamSegmentExistsException(segmentName));
                    } else {
                        this.segments.put(segmentName, new HashMap<>());
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<Void> deleteSegment(String segmentName, boolean mustBeEmpty, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.segments) {
                    if (this.segments.remove(segmentName) == null) {
                        throw new CompletionException(new StreamSegmentNotExistsException(segmentName));
                    }
                }
            }, executor);
        }

        @Override
        public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout) {
            return put(segmentName, entries, WireCommands.NULL_TABLE_SEGMENT_OFFSET, timeout);
        }

        @Override
        public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, long tableSegmentOffset, Duration timeout) {
            if (tableSegmentOffset != WireCommands.NULL_TABLE_SEGMENT_OFFSET) {
                throw new UnsupportedOperationException("updateTableSegment(with offset)");
            }

            return CompletableFuture.supplyAsync(() -> {
                Map<BufferView, BufferView> segmentData;
                synchronized (this.segments) {
                    segmentData = this.segments.getOrDefault(segmentName, null);
                    if (segmentData == null) {
                        throw new CompletionException(new StreamSegmentNotExistsException(segmentName));
                    }
                }

                // Note: this doesn't do conditional checks. We don't need them here.
                val result = new ArrayList<Long>(entries.size());
                val copies = new ArrayList<Map.Entry<BufferView, BufferView>>(entries.size());
                entries.forEach(e -> {
                    copies.add(new AbstractMap.SimpleImmutableEntry<>(new ByteArraySegment(e.getKey().getKey().getCopy()), new ByteArraySegment(e.getValue().getCopy())));
                    result.add(this.nextVersion.getAndIncrement());
                });
                synchronized (segmentData) {
                    copies.forEach(e -> segmentData.put(e.getKey(), e.getValue()));
                }
                return result;
            }, executor);
        }

        @Override
        public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout) {
            return remove(segmentName, keys, WireCommands.NULL_TABLE_SEGMENT_OFFSET, timeout);
        }

        @Override
        public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, long tableSegmentOffset, Duration timeout) {
            if (tableSegmentOffset != WireCommands.NULL_TABLE_SEGMENT_OFFSET) {
                throw new UnsupportedOperationException("remove(with offset)");
            }

            return CompletableFuture.runAsync(() -> {
                Map<BufferView, BufferView> segmentData;
                synchronized (this.segments) {
                    segmentData = this.segments.getOrDefault(segmentName, null);
                    if (segmentData == null) {
                        throw new CompletionException(new StreamSegmentNotExistsException(segmentName));
                    }
                }

                synchronized (segmentData) {
                    // Note: this doesn't do conditional checks. We don't need them here.
                    keys.forEach(k -> segmentData.remove(k.getKey())); // BufferView.equals() checks equality across implementations.
                }
            }, executor);
        }

        @Override
        public CompletableFuture<List<TableEntry>> get(String segmentName, List<BufferView> keys, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                Map<BufferView, BufferView> segmentData;
                synchronized (this.segments) {
                    segmentData = this.segments.getOrDefault(segmentName, null);
                    if (segmentData == null) {
                        throw new CompletionException(new StreamSegmentNotExistsException(segmentName));
                    }
                }

                val values = new ArrayList<BufferView>(keys.size());
                synchronized (segmentData) {
                    // Note: this doesn't do conditional checks. We don't need them here.
                    keys.forEach(k -> values.add(segmentData.getOrDefault(k, null)));
                }

                val result = new ArrayList<TableEntry>(keys.size());
                for (int i = 0; i < keys.size(); i++) {
                    val v = values.get(i);
                    result.add(v == null ? null : TableEntry.unversioned(new ByteArraySegment(keys.get(i).getCopy()), new ByteArraySegment(v.getCopy())));
                }
                return result;
            }, executor);
        }

        @Override
        public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
            throw new UnsupportedOperationException("keyIterator");
        }

        @Override
        public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
            throw new UnsupportedOperationException("entryIterator");
        }

        @Override
        public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryDeltaIterator(String segmentName, long fromPosition, Duration fetchTimeout) {
            throw new UnsupportedOperationException("entryDeltaIterator");
        }

        @Override
        public CompletableFuture<TableSegmentInfo> getInfo(String segmentName, Duration timeout) {
            throw new UnsupportedOperationException("getInfo");
        }
    }
}
