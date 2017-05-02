/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.stream.impl;

import io.pravega.ClientFactory;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.NameUtils;
import io.pravega.state.InitialUpdate;
import io.pravega.state.Revisioned;
import io.pravega.state.RevisionedStreamClient;
import io.pravega.state.StateSynchronizer;
import io.pravega.state.SynchronizerConfig;
import io.pravega.state.Update;
import io.pravega.state.impl.RevisionedStreamClientImpl;
import io.pravega.state.impl.StateSynchronizerImpl;
import io.pravega.state.impl.UpdateOrInitSerializer;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.IdempotentEventStreamWriter;
import io.pravega.stream.InvalidStreamException;
import io.pravega.stream.Position;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.Segment;
import io.pravega.stream.Serializer;
import io.pravega.stream.Stream;
import io.pravega.stream.impl.netty.ConnectionFactory;
import io.pravega.stream.impl.netty.ConnectionFactoryImpl;
import io.pravega.stream.impl.segment.SegmentInputStream;
import io.pravega.stream.impl.segment.SegmentInputStreamFactory;
import io.pravega.stream.impl.segment.SegmentInputStreamFactoryImpl;
import io.pravega.stream.impl.segment.SegmentOutputStream;
import io.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import io.pravega.stream.impl.segment.SegmentOutputStreamFactoryImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.function.Supplier;

import lombok.val;
import org.apache.commons.lang.NotImplementedException;

public class ClientFactoryImpl implements ClientFactory {

    private final String scope;
    private final Controller controller;
    private final SegmentInputStreamFactory inFactory;
    private final SegmentOutputStreamFactory outFactory;
    private final ConnectionFactory connectionFactory;

    /**
     * Creates a new instance of ClientFactory class.
     *
     * @param scope             The scope string.
     * @param controller        The reference to Controller.
     */
    public ClientFactoryImpl(String scope, Controller controller) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(controller);
        this.scope = scope;
        this.controller = controller;
        this.connectionFactory = new ConnectionFactoryImpl(false);
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
        return new EventStreamWriterImpl<T>(stream, controller, outFactory, s, config);
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
        return createReader(readerId, readerGroup, s, config, System::nanoTime, System::currentTimeMillis);
    }

    @VisibleForTesting
    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s, ReaderConfig config,
                                          Supplier<Long> nanoTime, Supplier<Long> milliTime) {
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> sync = createStateSynchronizer(
                NameUtils.getStreamForReaderGroup(readerGroup),
                new JavaSerializer<>(),
                new JavaSerializer<>(),
                synchronizerConfig);
        ReaderGroupStateManager stateManager = new ReaderGroupStateManager(readerId, sync, controller, nanoTime);
        stateManager.initializeReader();
        return new EventStreamReaderImpl<T>(inFactory, s, stateManager, new Orderer(), milliTime, config);
    }
    
    @Override
    public <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
                                                                      SynchronizerConfig config) {
        Segment segment = new Segment(scope, streamName, 0);
        SegmentInputStream in = inFactory.createInputStreamForSegment(segment);
        SegmentOutputStream out = outFactory.createOutputStreamForSegment(segment);
        return new RevisionedStreamClientImpl<>(segment, in, out, serializer);
    }

    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> StateSynchronizer<StateT>
        createStateSynchronizer(String streamName,
                                Serializer<UpdateT> updateSerializer,
                                Serializer<InitT> initialSerializer,
                                SynchronizerConfig config) {
        Segment segment = new Segment(scope, streamName, 0);
        if (!FutureHelpers.getAndHandleExceptions(controller.isSegmentOpen(segment), InvalidStreamException::new)) {
            throw new InvalidStreamException("Segment does not exist: " + segment);
        }
        val serializer = new UpdateOrInitSerializer<>(updateSerializer, initialSerializer);
        return new StateSynchronizerImpl<StateT>(segment, createRevisionedStreamClient(streamName, serializer, config));
    }

    @Override
    public void close() {
        connectionFactory.close();
    }

}
