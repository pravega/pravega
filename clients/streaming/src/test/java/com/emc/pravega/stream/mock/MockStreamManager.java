/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.ScalingPolicy.Type;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamManager;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.PositionImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class MockStreamManager implements StreamManager {

    private final String scope;
    private final ConcurrentHashMap<String, Stream> created = new ConcurrentHashMap<>();
    private final ConnectionFactoryImpl connectionFactory;
    private final Controller controller;

    public MockStreamManager(String scope, String endpoint, int port) {
        this.scope = scope;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.controller = new MockController(endpoint, port, connectionFactory);
    }

    public MockStreamManager(String scope, Controller controller) {
        this.scope = scope;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.controller = controller;
    }

    @Override
    public Stream createStream(String streamName, StreamConfiguration config) {
        if (config == null) {
            config = new StreamConfigurationImpl(scope, streamName, new ScalingPolicy(Type.FIXED_NUM_SEGMENTS, 0, 0, 1));
        }
        Stream stream = createStreamHelper(streamName, config);
        return stream;
    }

    @Override
    public void alterStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        FutureHelpers.getAndHandleExceptions(controller
                        .createStream(new StreamConfigurationImpl(scope, streamName, config.getScalingPolicy())),
                RuntimeException::new);
        Stream stream = new StreamImpl(scope, streamName, config, controller, connectionFactory);
        created.put(streamName, stream);
        return stream;
    }

    @Override
    public Stream getStream(String streamName) {
        return created.get(streamName);
    }

    @Override
    public void close() {

    }

    public Position getInitialPosition(String stream) {
        return new PositionImpl(Collections.singletonMap(new Segment(scope, stream, 0), 0L), Collections.emptyMap());
    }
}
