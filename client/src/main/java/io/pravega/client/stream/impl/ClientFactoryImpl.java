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
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactory;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.state.impl.RevisionedStreamClientImpl;
import io.pravega.client.state.impl.StateSynchronizerImpl;
import io.pravega.client.state.impl.UpdateOrInitSerializer;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.AccessOperation;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.ExecutorServiceHelpers.newScheduledThreadPool;

@Slf4j
public final class ClientFactoryImpl extends AbstractClientFactoryImpl implements EventStreamClientFactory, SynchronizerClientFactory {


    private final SegmentInputStreamFactory inFactory;
    private final SegmentOutputStreamFactory outFactory;
    private final ConditionalOutputStreamFactory condFactory;
    private final SegmentMetadataClientFactory metaFactory;
    private final ClientConfig clientConfig;

    private final ScheduledExecutorService watermarkReaderThreads = newScheduledThreadPool(getThreadPoolSize(), "WatermarkReader");

    /**
     * Creates a new instance of ClientFactory class.
     * Note: Controller is closed when {@link ClientFactoryImpl#close()} is invoked.
     *
     * @param scope             The scope string.
     * @param controller        The reference to Controller.
     * @param config            The client config.
     */
    public ClientFactoryImpl(String scope, Controller controller, ClientConfig config) {
        super(scope, controller, new ConnectionPoolImpl(config, new SocketConnectionFactoryImpl(config)));
        this.inFactory = new SegmentInputStreamFactoryImpl(controller, connectionPool);
        this.outFactory = new SegmentOutputStreamFactoryImpl(controller, connectionPool);
        this.condFactory = new ConditionalOutputStreamFactoryImpl(controller, connectionPool);
        this.metaFactory = new SegmentMetadataClientFactoryImpl(controller, connectionPool);
        this.clientConfig = config;
    }

    /**
     * Creates a new instance of the ClientFactory class.
     * Note: ConnectionFactory  and Controller is closed when {@link ClientFactoryImpl#close()} is invoked.
     *
     * @param scope             The scope string.
     * @param controller        The reference to Controller.
     * @param connectionFactory The reference to Connection Factory impl.
     */
    @VisibleForTesting
    public ClientFactoryImpl(String scope, Controller controller, ConnectionFactory connectionFactory) {
        this(scope, controller, new ConnectionPoolImpl(ClientConfig.builder().build(), connectionFactory));
    }

    /**
     * Creates a new instance of the ClientFactory class.
     * Note: ConnectionFactory  and Controller is closed when {@link ClientFactoryImpl#close()} is invoked.
     *
     * @param scope             The scope string.
     * @param controller        The reference to Controller.
     * @param config            The client config.
     * @param connectionFactory The reference to Connection Factory impl.
     */
    @VisibleForTesting
    public ClientFactoryImpl(String scope, Controller controller, ClientConfig config, ConnectionFactory connectionFactory) {
        this(scope, controller, new ConnectionPoolImpl(config, connectionFactory));
    }
    
    /**
     * Creates a new instance of the ClientFactory class. Note: ConnectionFactory and Controller is
     * closed when {@link ClientFactoryImpl#close()} is invoked.
     *
     * @param scope The scope string.
     * @param controller The reference to Controller.
     * @param pool The connection pool
     */
    @VisibleForTesting
    public ClientFactoryImpl(String scope, Controller controller, ConnectionPool pool) {
        super(scope, controller, pool);
        this.inFactory = new SegmentInputStreamFactoryImpl(controller, connectionPool);
        this.outFactory = new SegmentOutputStreamFactoryImpl(controller, connectionPool);
        this.condFactory = new ConditionalOutputStreamFactoryImpl(controller, connectionPool);
        this.metaFactory = new SegmentMetadataClientFactoryImpl(controller, connectionPool);
        this.clientConfig = ClientConfig.builder().build();
    }

    @VisibleForTesting
    public ClientFactoryImpl(String scope, Controller controller, ConnectionPool connectionPool,
            SegmentInputStreamFactory inFactory, SegmentOutputStreamFactory outFactory,
            ConditionalOutputStreamFactory condFactory, SegmentMetadataClientFactory metaFactory) {
        super(scope, controller, connectionPool);
        Preconditions.checkNotNull(inFactory);
        Preconditions.checkNotNull(outFactory);
        Preconditions.checkNotNull(condFactory);
        Preconditions.checkNotNull(metaFactory);
        this.inFactory = inFactory;
        this.outFactory = outFactory;
        this.condFactory = condFactory;
        this.metaFactory = metaFactory;
        this.clientConfig = ClientConfig.builder().build();
    }

    @Override
    public <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config) {
        return createEventWriter(UUID.randomUUID().toString(), streamName, s, config);
    }

    @Override
    public <T> EventStreamWriter<T> createEventWriter(String writerId, String streamName, Serializer<T> s,
                                                      EventWriterConfig config) {
        NameUtils.validateWriterId(writerId);
        log.info("Creating writer: {} for stream: {} with configuration: {}", writerId, streamName, config);
        Stream stream = new StreamImpl(scope, streamName);
        ExecutorService retransmitPool = ExecutorServiceHelpers.getShrinkingExecutor(1, 100,
                "ScalingRetransmission-" + stream.getScopedName());
        try {
            return new EventStreamWriterImpl<T>(stream, writerId, controller, outFactory, s, config, retransmitPool, connectionPool.getInternalExecutor(), connectionPool);
        } catch (Throwable ex) {
            // Make sure we shut down the pool if we can't use it.
            ExecutorServiceHelpers.shutdown(retransmitPool);
            throw ex;
        }
    }

    @Override
    public <T> TransactionalEventStreamWriter<T> createTransactionalEventWriter(String writerId, String streamName,
                                                                                Serializer<T> s,
                                                                                EventWriterConfig config) {
        NameUtils.validateWriterId(writerId);
        log.info("Creating transactional writer:{} for stream: {} with configuration: {}", writerId, streamName, config);
        Stream stream = new StreamImpl(scope, streamName);
        return new TransactionalEventStreamWriterImpl<T>(stream, writerId, controller, outFactory, s, config, connectionPool.getInternalExecutor());
    }

    @Override
    public <T> TransactionalEventStreamWriter<T> createTransactionalEventWriter(String streamName, Serializer<T> s, EventWriterConfig config) {
        return createTransactionalEventWriter(UUID.randomUUID().toString(), streamName, s, config);
    }

    @Override
    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s,
                                                 ReaderConfig config) {
        log.info("Creating reader: {} under readerGroup: {} with configuration: {}", readerId, readerGroup, config);
        return createReader(readerId, readerGroup, s, config, System::nanoTime, System::currentTimeMillis);
    }

    @VisibleForTesting
    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s, ReaderConfig config,
                                          Supplier<Long> nanoTime, Supplier<Long> milliTime) {
        NameUtils.validateReaderId(readerId);
        log.info("Creating reader: {} under readerGroup: {} with configuration: {}", readerId, readerGroup, config);
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> sync = createStateSynchronizer(
                NameUtils.getStreamForReaderGroup(readerGroup),
                new ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer(),
                new ReaderGroupManagerImpl.ReaderGroupStateInitSerializer(),
                synchronizerConfig);
        ReaderGroupStateManager stateManager = new ReaderGroupStateManager(scope, readerGroup, readerId, sync, controller, nanoTime);
        stateManager.initializeReader(config.getInitialAllocationDelay());
        Builder<Stream, WatermarkReaderImpl> watermarkReaders = ImmutableMap.builder();
        if (!config.isDisableTimeWindows()) {
            for (Stream stream : stateManager.getStreams()) {
                String streamName = NameUtils.getMarkStreamForStream(stream.getStreamName());
                val client = createRevisionedStreamClient(getSegmentForRevisionedClient(stream.getScope(), streamName),
                                                          new WatermarkSerializer(),
                                                          SynchronizerConfig.builder().readBufferSize(4096).build());
                watermarkReaders.put(stream, new WatermarkReaderImpl(stream, client, watermarkReaderThreads));
            }
        }
        return new EventStreamReaderImpl<T>(inFactory, metaFactory, s, stateManager, new Orderer(),
                milliTime, config, watermarkReaders.build(), controller);
    }
    
    @Override
    public <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
                                                                      SynchronizerConfig config) {
        log.info("Creating revisioned stream client for stream: {} with synchronizer configuration: {}", streamName, config);
        return createRevisionedStreamClient(getSegmentForRevisionedClient(scope, streamName), serializer, config);
    }

    private <T> RevisionedStreamClient<T> createRevisionedStreamClient(Segment segment, Serializer<T> serializer,
                                                                       SynchronizerConfig config) {
        EventSegmentReader in = inFactory.createEventReaderForSegment(segment, config.getReadBufferSize());
        DelegationTokenProvider delegationTokenProvider = DelegationTokenProviderFactory.create(controller, segment,
                AccessOperation.READ_WRITE);
        ConditionalOutputStream cond = condFactory.createConditionalOutputStream(segment, delegationTokenProvider, config.getEventWriterConfig());
        SegmentMetadataClient meta = metaFactory.createSegmentMetadataClient(segment, delegationTokenProvider);
        return new RevisionedStreamClientImpl<>(segment, in, outFactory, cond, meta, serializer, config.getEventWriterConfig(), delegationTokenProvider, clientConfig);
    }

    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> StateSynchronizer<StateT>
        createStateSynchronizer(String streamName,
                                Serializer<UpdateT> updateSerializer,
                                Serializer<InitT> initialSerializer,
                                SynchronizerConfig config) {
        log.info("Creating state synchronizer with stream: {} and configuration: {}", streamName, config);
        val serializer = new UpdateOrInitSerializer<>(updateSerializer, initialSerializer);
        val segment = getSegmentForRevisionedClient(scope, streamName);
        return new StateSynchronizerImpl<StateT>(segment, createRevisionedStreamClient(segment, serializer, config));
    }

    private Segment getSegmentForRevisionedClient(String scope, String streamName) {
        // This validates if the stream exists and returns zero segments if the stream is sealed.
        StreamSegments currentSegments = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, streamName), InvalidStreamException::new);
        if ( currentSegments == null || currentSegments.getSegments().size() == 0) {
            throw new InvalidStreamException("Stream does not exist: " + streamName);
        }
        return currentSegments.getSegmentForKey(0.0);
    }

    @Override
    public void close() {
        // wait for default timeout duration before forcibly terminating the watermarkReader threads.
        ExecutorServiceHelpers.shutdown(watermarkReaderThreads);
        connectionPool.close();
        controller.close();
    }

    private int getThreadPoolSize() {
        String configuredThreads = System.getProperty("pravega.client.internal.threadpool.size", null);
        if (configuredThreads != null) {
            return Integer.parseInt(configuredThreads);
        }
        return Runtime.getRuntime().availableProcessors();
    }

}