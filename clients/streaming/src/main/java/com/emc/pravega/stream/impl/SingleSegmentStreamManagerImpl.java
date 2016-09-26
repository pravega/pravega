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
package com.emc.pravega.stream.impl;

import java.util.concurrent.ConcurrentHashMap;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamManager;

/**
 * A StreamManager for the special case where the streams it creates will only ever be composed of one segment.
 */
public class SingleSegmentStreamManagerImpl implements StreamManager {

    private final String scope;
    private final ConcurrentHashMap<String, Stream> created = new ConcurrentHashMap<>();
    private final ControllerApi.Admin apiAdmin;
    private final ControllerApi.Producer apiProducer;
    private final ControllerApi.Consumer apiConsumer;

    public SingleSegmentStreamManagerImpl(ControllerApi.Admin apiAdmin, ControllerApi.Producer apiProducer, ControllerApi.Consumer apiConsumer, String scope) {
        this.scope = scope;
        this.apiAdmin = apiAdmin;
        this.apiProducer = apiProducer;
        this.apiConsumer = apiConsumer;
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
        FutureHelpers.getAndHandleExceptions(
                apiAdmin.createStream(new StreamConfigurationImpl(streamName,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0, 0, 1))),
                RuntimeException::new);


        Stream stream = new SingleSegmentStreamImpl(scope, streamName, config, apiAdmin, apiProducer, apiConsumer);
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
}
