/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ByteStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.batch.impl.BatchClientFactoryImpl;
import io.pravega.client.byteStream.impl.ByteStreamClientImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.security.auth.DelegationTokenProxy;
import io.pravega.client.security.auth.DelegationTokenProxyImpl;
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
import io.pravega.shared.segment.StreamSegmentNameUtils;

import static io.pravega.common.concurrent.ExecutorServiceHelpers.newScheduledThreadPool;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientFactoryImpl implements ClientFactory, EventStreamClientFactory, SynchronizerClientFactory {

    private final String scope;
    private final Controller controller;
    private final SegmentInputStreamFactory inFactory;
    private final SegmentOutputStreamFactory outFactory;
    private final ConditionalOutputStreamFactory condFactory;
    private final SegmentMetadataClientFactory metaFactory;
    private final ConnectionFactory connectionFactory;
    private final ScheduledExecutorService watermarkReaderThreads = newScheduledThreadPool(getThreadPoolSize(), "WatermarkReader");

    /**
     * Creates a new instance of ClientFactory class.
     * Note: Controller is closed when {@link ClientFactoryImpl#close()} is invoked.
     *
     * @param scope             The scope string.
     * @param controller        The reference to Controller.
     */
    public ClientFactoryImpl(String scope, Controller controller) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(controller);
        this.scope = scope;
        this.controller = controller;
        this.connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        this.inFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        this.outFactory = new SegmentOutputStreamFactoryImpl(controller, connectionFactory);
        this.condFactory = new ConditionalOutputStreamFactoryImpl(controller, connectionFactory);
        this.metaFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
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
        this(scope, controller, connectionFactory, new SegmentInputStreamFactoryImpl(controller, connectionFactory),
                new SegmentOutputStreamFactoryImpl(controller, connectionFactory),
                new ConditionalOutputStreamFactoryImpl(controller, connectionFactory),
                new SegmentMetadataClientFactoryImpl(controller, connectionFactory));
    }

    @VisibleForTesting
    public ClientFactoryImpl(String scope, Controller controller, ConnectionFactory connectionFactory,
            SegmentInputStreamFactory inFactory, SegmentOutputStreamFactory outFactory,
            ConditionalOutputStreamFactory condFactory, SegmentMetadataClientFactory metaFactory) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(inFactory);
        Preconditions.checkNotNull(outFactory);
        Preconditions.checkNotNull(condFactory);
        Preconditions.checkNotNull(metaFactory);
        this.scope = scope;
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.inFactory = inFactory;
        this.outFactory = outFactory;
        this.condFactory = condFactory;
        this.metaFactory = metaFactory;
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config) {
        return createEventWriter(UUID.randomUUID().toString(), streamName, s, config);
    }

    @Override
    public <T> EventStreamWriter<T> createEventWriter(String writerId, String streamName, Serializer<T> s,
                                                      EventWriterConfig config) {
        log.info("Creating writer for stream: {} with configuration: {}", streamName, config);
        Stream stream = new StreamImpl(scope, streamName);
        ThreadPoolExecutor retransmitPool = ExecutorServiceHelpers.getShrinkingExecutor(1, 100, "ScalingRetransmition-"
                + stream.getScopedName());
        return new EventStreamWriterImpl<T>(stream, writerId, controller, outFactory, s, config, retransmitPool, connectionFactory.getInternalExecutor());
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T> TransactionalEventStreamWriter<T> createTransactionalEventWriter(String writerId, String streamName,
                                                                                Serializer<T> s,
                                                                                EventWriterConfig config) {
        log.info("Creating transactional writer for stream: {} with configuration: {}", streamName, config);
        Stream stream = new StreamImpl(scope, streamName);
        return new TransactionalEventStreamWriterImpl<T>(stream, writerId, controller, outFactory, s, config, connectionFactory.getInternalExecutor());
    }

    @Override
    @SuppressWarnings("deprecation")
    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s,
                                                 ReaderConfig config) {
        log.info("Creating reader: {} under readerGroup: {} with configuration: {}", readerId, readerGroup, config);
        return createReader(readerId, readerGroup, s, config, System::nanoTime, System::currentTimeMillis);
    }

    @VisibleForTesting
    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s, ReaderConfig config,
                                          Supplier<Long> nanoTime, Supplier<Long> milliTime) {
        log.info("Creating reader: {} under readerGroup: {} with configuration: {}", readerId, readerGroup, config);
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> sync = createStateSynchronizer(
                NameUtils.getStreamForReaderGroup(readerGroup),
                new ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer(),
                new ReaderGroupManagerImpl.ReaderGroupStateInitSerializer(),
                synchronizerConfig);
        ReaderGroupStateManager stateManager = new ReaderGroupStateManager(readerId, sync, controller, nanoTime);
        stateManager.initializeReader(config.getInitialAllocationDelay());
        Builder<Stream, WatermarkReaderImpl> watermarkReaders = ImmutableMap.builder();
        if (!config.isDisableTimeWindows()) {
            for (Stream stream : stateManager.getStreams()) {
                String streamName = StreamSegmentNameUtils.getMarkForStream(stream.getStreamName());
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
    @SuppressWarnings("deprecation")
    public <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
                                                                      SynchronizerConfig config) {
        log.info("Creating revisioned stream client for stream: {} with synchronizer configuration: {}", streamName, config);
        return createRevisionedStreamClient(getSegmentForRevisionedClient(scope, streamName), serializer, config);
    }

    private <T> RevisionedStreamClient<T> createRevisionedStreamClient(Segment segment, Serializer<T> serializer,
                                                                       SynchronizerConfig config) {
        EventSegmentReader in = inFactory.createEventReaderForSegment(segment, config.getReadBufferSize());
        String delegationToken = Futures.getAndHandleExceptions(controller.getOrRefreshDelegationTokenFor(segment.getScope(),
                                                                                                          segment.getStreamName()), RuntimeException::new);
        ConditionalOutputStream cond = condFactory.createConditionalOutputStream(segment, delegationToken, config.getEventWriterConfig());

        DelegationTokenProxy delegationTokenProxy = new DelegationTokenProxyImpl(delegationToken, controller,
                segment.getScope(), segment.getStreamName());
        SegmentMetadataClient meta = metaFactory.createSegmentMetadataClient(segment, delegationTokenProxy);
        return new RevisionedStreamClientImpl<>(segment, in, outFactory, cond, meta, serializer, config.getEventWriterConfig(), delegationToken);
    }

    @Override
    @SuppressWarnings("deprecation")
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

    /**
     * Create a new batch client. A batch client can be used to perform bulk unordered reads without
     * the need to create a reader group.
     *
     * Please note this is an experimental API.
     *
     * @return A batch client
     * @deprecated Use {@link BatchClientFactory#withScope(String, ClientConfig)}
     */
    @Override
    @Deprecated
    public BatchClientFactoryImpl createBatchClient() {
        return new BatchClientFactoryImpl(controller, connectionFactory);
    }
    
    /**
     * Creates a new ByteStreamClient. The byteStreamClient can create readers and writers that work
     * on a stream of bytes. The stream must be pre-created with a single fixed segment. Sharing a
     * stream between the byte stream API and the Event stream readers/writers will CORRUPT YOUR
     * DATA in an unrecoverable way.
     * 
     * @return A byteStreamClient
     * @deprecated Use {@link ByteStreamClientFactory#withScope(String, ClientConfig)}
     */
    @Override
    @Deprecated
    public ByteStreamClientImpl createByteStreamClient() {
        return new ByteStreamClientImpl(scope, controller, connectionFactory);
    }

    @Override
    public void close() {
        controller.close();
        connectionFactory.close();
    }

    private int getThreadPoolSize() {
        String configuredThreads = System.getProperty("pravega.client.internal.threadpool.size", null);
        if (configuredThreads != null) {
            return Integer.parseInt(configuredThreads);
        }
        return Runtime.getRuntime().availableProcessors();
    }

}
