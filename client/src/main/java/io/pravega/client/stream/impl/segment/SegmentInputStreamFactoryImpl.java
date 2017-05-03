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
package io.pravega.client.stream.impl.segment;

import java.util.concurrent.ExecutionException;

import io.pravega.client.stream.Segment;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.netty.ConnectionFactory;
import io.pravega.common.Exceptions;
import com.google.common.annotations.VisibleForTesting;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@VisibleForTesting
@RequiredArgsConstructor
public class SegmentInputStreamFactoryImpl implements SegmentInputStreamFactory {

    private final Controller controller;
    private final ConnectionFactory cf;
    
    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment) {
        return createInputStreamForSegment(segment, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, int bufferSize) {
    AsyncSegmentInputStreamImpl result = new AsyncSegmentInputStreamImpl(controller, cf, segment);
        try {
            Exceptions.handleInterrupted(() -> result.getConnection().get());
        } catch (ExecutionException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return new SegmentInputStreamImpl(result, 0, bufferSize);
    }
}
