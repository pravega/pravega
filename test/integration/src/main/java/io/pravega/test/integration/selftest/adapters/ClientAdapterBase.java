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

import com.google.common.base.Preconditions;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Put;
import io.pravega.client.tables.Remove;
import io.pravega.client.tables.TableKey;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.store.kvtable.KeyValueTable;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamingException;
import io.pravega.shared.NameUtils;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Store adapter wrapping a real Pravega Client.
 */
abstract class ClientAdapterBase extends StoreAdapter {
    //region Members
    static final String SCOPE = "SelfTest";
    static final ByteArraySerializer SERIALIZER = new ByteArraySerializer();
    static final BufferViewSerializer KVT_SERIALIZER = new BufferViewSerializer();
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
    private final ConcurrentHashMap<String, io.pravega.client.tables.KeyValueTable> keyValueTables;
    private final int writersPerStream;

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
        this.keyValueTables = new ConcurrentHashMap<>();
        this.writersPerStream = testConfig.getClientWritersPerStream() > 0
                ? testConfig.getClientWritersPerStream()
                : Math.max(1, this.testConfig.getProducerCount() / this.testConfig.getStreamCount());

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

    //endregion

    //region Stream Operations

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

            List<EventStreamWriter<byte[]>> writers = new ArrayList<>(this.writersPerStream);
            if (this.streamWriters.putIfAbsent(streamName, writers) == null) {
                for (int i = 0; i < this.writersPerStream; i++) {
                    writers.add(getClientFactory().createEventWriter(streamName, SERIALIZER, WRITER_CONFIG));
                }
            }
            List<TransactionalEventStreamWriter<byte[]>> txnWriters = new ArrayList<>(this.writersPerStream);
            if (this.transactionalWriters.putIfAbsent(streamName, txnWriters) == null) {
                for (int i = 0; i < this.writersPerStream; i++) {
                    txnWriters.add(getClientFactory().createTransactionalEventWriter("writer", streamName, SERIALIZER, WRITER_CONFIG));
                }
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> deleteStream(String streamName, Duration timeout) {
        ensureRunning();
        String parentName = NameUtils.getParentStreamSegmentName(streamName);
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
        String parentName = NameUtils.getParentStreamSegmentName(streamName);
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
    public CompletableFuture<Void> sealStream(String streamName, Duration timeout) {
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
            String txnName = NameUtils.getTransactionNameFromId(parentStream, txnId);
            this.transactionIds.put(txnName, txnId);
            return txnName;
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureRunning();
        String parentStream = NameUtils.getParentStreamSegmentName(transactionName);
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
        String parentStream = NameUtils.getParentStreamSegmentName(transactionName);
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

    //region Table Operations

    @Override
    public CompletableFuture<Void> createTable(String tableName, Duration timeout) {
        ensureRunning();
        return CompletableFuture.runAsync(() -> {
            val config = KeyValueTableConfiguration.builder()
                    .partitionCount(this.testConfig.getSegmentsPerStream())
                    .build();
            if (!getKVTManager().createKeyValueTable(SCOPE, tableName, config)) {
                throw new CompletionException(new StreamingException(String.format("Unable to create KVT '%s'.", tableName)));
            }

            val clientConfig = KeyValueTableClientConfiguration.builder().build();
            val kvt = getKVTFactory().forKeyValueTable(tableName, clientConfig);
            this.keyValueTables.putIfAbsent(tableName, kvt);
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Void> deleteTable(String tableName, Duration timeout) {
        ensureRunning();
        return CompletableFuture.runAsync(() -> {
            val kvt = this.keyValueTables.remove(tableName);
            if (kvt != null) {
                kvt.close();
            }
            if (!getKVTManager().deleteKeyValueTable(SCOPE, tableName)) {
                throw new CompletionException(new StreamingException(String.format("Unable to delete KVT '%s'.", tableName)));
            }
        }, this.testExecutor);
    }

    @Override
    public CompletableFuture<Long> updateTableEntry(String tableName, BufferView key, BufferView value, Long compareVersion, Duration timeout) {
        // NOTE: we do not support conditional updates (these are converted to unconditional updates).
        ensureRunning();
        return getKvt(tableName)
                .update(new Put(new TableKey(ByteBuffer.wrap(key.getCopy())), ByteBuffer.wrap(value.getCopy())))
                .thenApply(v -> v.asImpl().getSegmentVersion());
    }

    @Override
    public CompletableFuture<Void> removeTableEntry(String tableName, BufferView key, Long compareVersion, Duration timeout) {
        // NOTE: we do not support conditional removals (these are converted to unconditional removals).
        ensureRunning();
        return Futures.toVoid(getKvt(tableName).update(new Remove(new TableKey(ByteBuffer.wrap(key.getCopy())))));
    }

    @Override
    public CompletableFuture<List<BufferView>> getTableEntries(String tableName, List<BufferView> keys, Duration timeout) {
        ensureRunning();
        return getKvt(tableName)
                .getAll(keys.stream().map(BufferView::getCopy).map(ByteBuffer::wrap).map(TableKey::new).collect(Collectors.toList()))
                .thenApplyAsync(entries -> entries.stream().map(e -> e == null ? null : new ByteArraySegment(e.getValue())).collect(Collectors.toList()), this.testExecutor);
    }

    @Override
    public CompletableFuture<AsyncIterator<List<Map.Entry<BufferView, BufferView>>>> iterateTableEntries(String tableName, Duration timeout) {
        throw new UnsupportedOperationException("Table Entry Iterator not supported.");
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
     * Gets a reference to the {@link KeyValueTableManager}.
     */
    protected abstract KeyValueTableManager getKVTManager();

    /**
     * Gets a references to the {@link KeyValueTableFactory} used to create {@link KeyValueTable} instances.
     */
    protected abstract KeyValueTableFactory getKVTFactory();

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

    @SneakyThrows(StreamSegmentNotExistsException.class)
    private io.pravega.client.tables.KeyValueTable getKvt(String kvtName) {
        val kvt = this.keyValueTables.getOrDefault(kvtName, null);
        if (kvt == null) {
            throw new StreamSegmentNotExistsException(kvtName);
        }

        return kvt;
    }

    private boolean isTransaction(String streamName, String parentName) {
        return parentName != null && parentName.length() < streamName.length();
    }

    //endregion

    //region BufferViewSerializer

    private static class BufferViewSerializer implements Serializer<BufferView> {
        @Override
        public ByteBuffer serialize(BufferView value) {
            val contents = value.iterateBuffers();
            val first = contents.hasNext() ? contents.next() : null;
            if (first != null && !contents.hasNext()) {
                return first;
            }

            return ByteBuffer.wrap(value.getCopy());
        }

        @Override
        public BufferView deserialize(ByteBuffer serializedValue) {
            byte[] result = new byte[serializedValue.remaining()];
            serializedValue.get(result);
            return new ByteArraySegment(result);
        }
    }

    //endregion
}