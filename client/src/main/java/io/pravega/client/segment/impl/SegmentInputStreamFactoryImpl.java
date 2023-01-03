/**
 * Copyright Pravega Authors.
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
package io.pravega.client.segment.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.common.MathHelpers;
import io.pravega.shared.security.auth.AccessOperation;
import java.util.concurrent.Semaphore;
import lombok.RequiredArgsConstructor;

@VisibleForTesting
@RequiredArgsConstructor
public class SegmentInputStreamFactoryImpl implements SegmentInputStreamFactory {

    private final Controller controller;
    private final ConnectionPool cp;

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment) {
        return createEventReaderForSegment(segment, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, long startOffset, long endOffset) {
        return getEventSegmentReader(segment, null, startOffset,  endOffset, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, int bufferSize) {
        return getEventSegmentReader(segment, null, 0, Long.MAX_VALUE, bufferSize);
    }
    
    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, int bufferSize, Semaphore hasData, long endOffset) {
        return getEventSegmentReader(segment, hasData, 0, endOffset, bufferSize);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, long startOffset, int lengthToRead) {
        return getEventSegmentReader(segment, null, startOffset, startOffset + lengthToRead, lengthToRead);
    }

    private EventSegmentReader getEventSegmentReader(Segment segment, Semaphore hasData, long startOffset, long endOffset, int bufferSize) {
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(controller, segment, AccessOperation.READ);
        tokenProvider.retrieveToken();
        AsyncSegmentInputStreamImpl async = new AsyncSegmentInputStreamImpl(controller, cp, segment, tokenProvider, hasData);
        async.getConnection();                      //Sanity enforcement
        bufferSize = MathHelpers.minMax(bufferSize, SegmentInputStreamImpl.MIN_BUFFER_SIZE, SegmentInputStreamImpl.MAX_BUFFER_SIZE);
        return getEventSegmentReader(async, startOffset, endOffset, bufferSize);
    }

    @VisibleForTesting
    static EventSegmentReaderImpl getEventSegmentReader(AsyncSegmentInputStream async, long startOffset,
                                                                  long endOffset, int bufferSize) {
        return new EventSegmentReaderImpl(new SegmentInputStreamImpl(async, startOffset, endOffset, bufferSize));
    }

    @VisibleForTesting
    static EventSegmentReaderImpl getEventSegmentReader(AsyncSegmentInputStream async, long startOffset) {
        return new EventSegmentReaderImpl(new SegmentInputStreamImpl(async, startOffset));
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, DelegationTokenProvider tokenProvider) {
        return createInputStreamForSegment(segment, tokenProvider, 0);
    }

    @Override
    public SegmentInputStream createInputStreamForSegment(Segment segment, DelegationTokenProvider tokenProvider, long startOffset) {
        AsyncSegmentInputStreamImpl async = new AsyncSegmentInputStreamImpl(controller, cp, segment, tokenProvider, null);
        async.getConnection();
        return new SegmentInputStreamImpl(async, startOffset);
    }
}
