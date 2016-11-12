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

import java.util.concurrent.ExecutionException;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.Controller;
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
    public SegmentInputStream createInputStreamForSegment(Segment segment, SegmentInputConfiguration config) {
        AsyncSegmentInputStreamImpl result = new AsyncSegmentInputStreamImpl(controller, cf,
                segment.getQualifiedName());
        try {
            Exceptions.handleInterrupted(() -> result.getConnection().get());
        } catch (ExecutionException e) {
            log.warn("Initial connection attempt failure. Suppressing.", e);
        }
        return new SegmentInputStreamImpl(result, 0);
    }
}
