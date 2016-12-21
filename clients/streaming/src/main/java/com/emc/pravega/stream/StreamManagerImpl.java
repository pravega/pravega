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
package com.emc.pravega.stream;

import com.emc.pravega.StreamManager;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamImpl;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.NotImplementedException;

/**
 * A stream manager. Used to bootstrap the client.
 */
public class StreamManagerImpl implements StreamManager {

    private final String scope;
    static private final ConcurrentHashMap<String, Stream> created = new ConcurrentHashMap<>();
    private final ControllerImpl controller;

    public StreamManagerImpl(String scope, URI controllerUri) {
        this.scope = scope;
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
    }

    @Override
    public Stream createStream(String streamName, StreamConfiguration config) {
        Stream stream = createStreamHelper(streamName, config);
        return stream;
    }

    @Override
    public void alterStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        FutureHelpers.getAndHandleExceptions(controller.createStream(new StreamConfigurationImpl(scope, streamName,
                        config.getScalingPolicy())),
                RuntimeException::new);

        Stream stream = new StreamImpl(scope, streamName, config);
        created.put(streamName, stream);
        return stream;
    }

    @Override
    public Stream getStream(String streamName) {
        return created.get(streamName);
    }

    @Override
    public void close() throws Exception {

    }
    
    @Override
    public ConsumerGroup createConsumerGroup(String groupName, ConsumerGroupConfig config, List<String> streams) {
        throw new NotImplementedException();
    }
    
    @Override
    public ConsumerGroup updateConsumerGroup(String groupName, ConsumerGroupConfig config, List<String> streamNames) {
        throw new NotImplementedException();
    }

    @Override
    public ConsumerGroup getConsumerGroup(String groupName) {
        throw new NotImplementedException();
    }

    @Override
    public void deleteConsumerGroup(ConsumerGroup group) {
        throw new NotImplementedException();
    }

    @Override
    public void deleteStream(Stream toDelete) {
        throw new NotImplementedException();
    }

}
