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

import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamManager;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.segment.impl.SegmentManagerImpl;

/**
 * A StreamManager for the special case where the streams it creates will only ever be composed of one segment.
 */
public class SingleSegmentStreamManagerImpl implements StreamManager {

    private final SegmentManagerImpl segmentManager;
    private final String scope;
    private final ConcurrentHashMap<String, Stream> created = new ConcurrentHashMap<>();
    private final ConnectionFactory clientCF;

    public SingleSegmentStreamManagerImpl(String endpoint, int port, String scope) {
        this.scope = scope;
        this.clientCF = new ConnectionFactoryImpl(false, port);
        this.segmentManager = new SegmentManagerImpl(endpoint, clientCF);
    }

    @Override
    public Stream createStream(String streamName, StreamConfiguration config) {
        boolean existed = created.containsKey(streamName);
        Stream stream = createStreamHelper(streamName, config);
        if (!existed) {
            segmentManager.createSegment(stream.getLatestSegments().getSegments().get(0).getQualifiedName());
        }
        return stream;
    }

    @Override
    public void alterStream(String streamName, StreamConfiguration config) {
        createStreamHelper(streamName, config);
    }

    private Stream createStreamHelper(String streamName, StreamConfiguration config) {
        Stream stream = new SingleSegmentStreamImpl(scope, streamName, config, segmentManager);
        created.put(streamName, stream);
        return stream;
    }

    @Override
    public Stream getStream(String streamName) {
        return created.get(streamName);
    }

    @Override
    public void close() {
        clientCF.close();
    }

}
