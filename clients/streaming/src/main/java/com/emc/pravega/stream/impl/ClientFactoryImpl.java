/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.RevisionedStreamClient;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.state.impl.CorruptedStateException;
import com.emc.pravega.state.impl.RevisionedStreamClientImpl;
import com.emc.pravega.state.impl.StateSynchronizerImpl;
import com.emc.pravega.state.impl.UpdateOrInitSerializer;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.IdempotentEventStreamWriter;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentInputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentInputStreamFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.NotImplementedException;

import lombok.val;

public class ClientFactoryImpl implements ClientFactory {

    private final String scope;
    private final Controller controller;
    private final SegmentInputStreamFactory inFactory;
    private final SegmentOutputStreamFactory outFactory;
    private final ConnectionFactory connectionFactory;

    /**
     * Creates a new instance of ClientFactory class.
     *
     * @param scope         The scope string.
     * @param controllerUri The Controller URI.
     */
    public ClientFactoryImpl(String scope, URI controllerUri) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(controllerUri);
        this.scope = scope;
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
        connectionFactory = new ConnectionFactoryImpl(false);
        this.inFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        this.outFactory = new SegmentOutputStreamFactoryImpl(controller, connectionFactory);
    }

    /**
     * Creates a new instance of the ClientFactory class.
     *
     * @param scope             The scope string.
     * @param controller        The reference to Controller.
     * @param connectionFactory The reference to Connection Factory impl.
     */
    @VisibleForTesting
    public ClientFactoryImpl(String scope, Controller controller, ConnectionFactory connectionFactory) {
        this(scope, controller, connectionFactory, new SegmentInputStreamFactoryImpl(controller, connectionFactory),
                new SegmentOutputStreamFactoryImpl(controller, connectionFactory));
    }

    @VisibleForTesting
    public ClientFactoryImpl(String scope, Controller controller, ConnectionFactory connectionFactory,
            SegmentInputStreamFactory inFactory, SegmentOutputStreamFactory outFactory) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(inFactory);
        Preconditions.checkNotNull(outFactory);
        this.scope = scope;
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.inFactory = inFactory;
        this.outFactory = outFactory;
    }


    @Override
    public <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config) {
        Stream stream = new StreamImpl(scope, streamName);
        EventRouter router = new EventRouter(stream, controller);
        return new EventStreamWriterImpl<T>(stream, controller, outFactory, router, s, config);
    }

    @Override
    public <T> IdempotentEventStreamWriter<T> createIdempotentEventWriter(String streamName, Serializer<T> s,
            EventWriterConfig config) {
        throw new NotImplementedException();
    }

    @Override
    public <T> EventStreamReader<T> createReader(String stream, Serializer<T> s, ReaderConfig config,
            Position startingPosition) {
        throw new NotImplementedException();
    }

    @Override
    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s,
            ReaderConfig config) {
        SynchronizerConfig synchronizerConfig = new SynchronizerConfig(null, null);
        StateSynchronizer<ReaderGroupState> sync = createStateSynchronizer(readerGroup,
                                                                           new JavaSerializer<>(),
                                                                           new JavaSerializer<>(),
                                                                           synchronizerConfig);
        ReaderGroupStateManager stateManager = new ReaderGroupStateManager(readerId,
                sync,
                controller,
                System::nanoTime);
        stateManager.initializeReader();
        return new EventStreamReaderImpl<T>(inFactory,
                                      s,
                                      stateManager,
                                      new RoundRobinOrderer(),
                                      System::currentTimeMillis,
                                      config);
    }

    private static class RoundRobinOrderer implements Orderer {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public SegmentEventReader nextSegment(List<SegmentEventReader> segments) {
            int count = counter.incrementAndGet();
            return segments.get(count % segments.size());
        }
    }

    @Override
    public <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
            SynchronizerConfig config) {
        Segment segment = new Segment(scope, streamName, 0);
        SegmentInputStream in = inFactory.createInputStreamForSegment(segment, config.getInputConfig());
        SegmentOutputStream out;
        try {
            out = outFactory.createOutputStreamForSegment(segment, config.getOutputConfig());
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Attempted to create synchronizer on sealed segment", e);
        }
        return new RevisionedStreamClientImpl<>(segment, in, out, serializer);
    }

    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>>
            StateSynchronizer<StateT> createStateSynchronizer(String streamName,
                    Serializer<UpdateT> updateSerializer, Serializer<InitT> initialSerializer,
                    SynchronizerConfig config) {
        Segment segment = new Segment(scope, streamName, 0);
        val serializer = new UpdateOrInitSerializer<>(updateSerializer, initialSerializer);
        return new StateSynchronizerImpl<StateT>(segment,
                createRevisionedStreamClient(streamName, serializer, config));
    }

    @Override
    public void close() {
        connectionFactory.close();
    }

}
