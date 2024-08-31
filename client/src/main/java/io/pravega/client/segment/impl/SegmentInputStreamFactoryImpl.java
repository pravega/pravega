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
import io.pravega.common.util.SimpleCache;
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.AccessOperation;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@VisibleForTesting
public class SegmentInputStreamFactoryImpl implements SegmentInputStreamFactory {
    private static final Logger log = LoggerFactory.getLogger(SegmentInputStreamFactoryImpl.class);
    private static final int CACHE_MAX_SIZE = 100;
    private static final int EXPIRATION_TIME_MILLIS = 600000;
    private final Controller controller;
    private final ConnectionPool cp;
    private SimpleCache<String, DelegationTokenProvider> tokenProviderCache;

    public SegmentInputStreamFactoryImpl(Controller controller, ConnectionPool cp) {
        this.controller = controller;
        this.cp = cp;
        this.tokenProviderCache = new SimpleCache<>(CACHE_MAX_SIZE,
                Duration.ofMillis(EXPIRATION_TIME_MILLIS), (k, v) -> log.info("key: {} is evicted from tokenProviderCache.", k));
    }

    @VisibleForTesting
    public SegmentInputStreamFactoryImpl(Controller controller, ConnectionPool cp, SimpleCache<String, DelegationTokenProvider> tokenProviderCache) {
        this.controller = controller;
        this.cp = cp;
        this.tokenProviderCache = tokenProviderCache;
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment) {
        return createEventReaderForSegment(segment, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
    }

    @Override
    public EventSegmentReader createEventReaderForSegment(Segment segment, long startOffset, long endOffset) {
        return getEventSegmentReader(segment, null, startOffset, endOffset, SegmentInputStreamImpl.DEFAULT_BUFFER_SIZE);
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
        DelegationTokenProvider tokenProvider = getDelegationTokenProvider(segment.getScope(), segment.getStreamName());
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

    DelegationTokenProvider getDelegationTokenProvider(final String scope, final String streamName) {
        final String scopedStreamName = NameUtils.getScopedStreamName(scope, streamName);
        log.debug("get delegation token provider for scope: {}, stream: {}, scopedStreamName: {}", scope,
                streamName, scopedStreamName);
        DelegationTokenProvider tokenProvider = tokenProviderCache.get(scopedStreamName);
        if (Objects.isNull(tokenProvider)) {
            log.debug("getDelegationTokenProvider cache doesn't get hit.");
            tokenProvider = createDelegationTokenProvider(scope, streamName);
            tokenProviderCache.put(scopedStreamName, tokenProvider);
        }
        tokenProvider.retrieveToken();

        return tokenProvider;
    }

    private DelegationTokenProvider createDelegationTokenProvider(final String scope, final String streamName) {
        return DelegationTokenProviderFactory.create(controller, scope, streamName, AccessOperation.READ);
    }
}
