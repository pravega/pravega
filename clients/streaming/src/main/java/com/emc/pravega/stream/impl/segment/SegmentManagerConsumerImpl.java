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
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.SegmentUri;
import com.emc.pravega.stream.impl.StreamController;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ExecutionException;

@Slf4j
@VisibleForTesting
public class SegmentManagerConsumerImpl implements SegmentManagerConsumer, StreamController {

    private final String stream;
    private final ControllerApi.Consumer apiConsumer;

    public SegmentManagerConsumerImpl(String stream, ControllerApi.Consumer apiConsumer) {
        this.stream = stream;
        this.apiConsumer = apiConsumer;
    }

    @Override
    public SegmentInputStream openSegmentForReading(String name, SegmentInputConfiguration config) {
        AsyncSegmentInputStreamImpl result = new AsyncSegmentInputStreamImpl(this, new ConnectionFactoryImpl(false), name);
        try {
            Exceptions.handleInterrupted(() -> result.getConnection().get());
        } catch (ExecutionException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return new SegmentInputStreamImpl(result, 0);
    }

    @Override
    public SegmentUri getEndpointForSegment(String segment) {
        return FutureHelpers.getAndHandleExceptions(apiConsumer.getURI(stream, getSegmentIdFromName(segment)), RuntimeException::new);
    }

    private SegmentId getSegmentIdFromName(String segment) {
        // TODO: Shivesh
        return null;
    }
}
