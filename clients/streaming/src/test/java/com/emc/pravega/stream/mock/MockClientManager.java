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
package com.emc.pravega.stream.mock;

import com.emc.pravega.ClientManager;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.ConsumerConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ClientManagerImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;

import java.util.Collections;

public class MockClientManager implements ClientManager {

    private String scope;
    private final ClientManager impl;
    private MockStreamManager streamManager;

    public MockClientManager(String scope, String endpoint, int port) {
        this.scope = scope;
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        MockController controller = new MockController(endpoint, port, connectionFactory);
        streamManager = new MockStreamManager(scope, controller);
        impl = new ClientManagerImpl(scope, controller, connectionFactory, streamManager);
    }
    
    public MockClientManager(String scope, Controller controller) {
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        impl = new ClientManagerImpl(scope, controller, connectionFactory, new MockStreamManager(scope, controller));
    }

    @Override
    public <T> Producer<T> createProducer(String streamName, Serializer<T> s, ProducerConfig config) {
        return impl.createProducer(streamName, s, config);
    }

    @Override
    public <T> Consumer<T> createConsumer(String streamName, Serializer<T> s, ConsumerConfig config,
            Position startingPosition) {
        return impl.createConsumer(streamName, s, config, startingPosition);
    }

    @Override
    public <T> Consumer<T> createConsumer(String consumerId, String consumerGroup, Serializer<T> s,
            ConsumerConfig config) {
        return impl.createConsumer(consumerId, consumerGroup, s, config);
    }

    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> Synchronizer<StateT, UpdateT, InitT> createSynchronizer(
            String streamName, Serializer<UpdateT> updateSerializer, Serializer<InitT> initialSerializer,
            SynchronizerConfig config) {
        return impl.createSynchronizer(streamName, updateSerializer, initialSerializer, config);
    }
    
    public void createStream(String streamName, StreamConfiguration config) {
        streamManager.createStream(streamName, config);
    }

    public Position getInitialPosition(String stream) {
        return new PositionImpl(Collections.singletonMap(new Segment(scope, stream, 0), 0L), Collections.emptyMap());
    }

}
