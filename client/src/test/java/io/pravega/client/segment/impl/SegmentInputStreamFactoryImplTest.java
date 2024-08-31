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

import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.EmptyTokenProviderImpl;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.util.SimpleCache;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;

import java.time.Duration;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SegmentInputStreamFactoryImplTest {
    @Mock
    private Controller controller;
    @Mock
    private ConnectionPool cp;
    @Mock
    private ClientConnection connection;
    @Mock
    private ScheduledExecutorService executor;

    @Before
    public void setup() {
        when(controller.getEndpointForSegment(anyString()))
                .thenReturn(CompletableFuture.completedFuture(new PravegaNodeUri("localhost", 9090)));
        when(cp.getClientConnection(any(Flow.class), any(PravegaNodeUri.class), any(ReplyProcessor.class)))
                .thenReturn(CompletableFuture.completedFuture(connection));
        when(cp.getInternalExecutor()).thenReturn(executor);
    }

    @Test
    public void createInputStreamForSegment() {
        SegmentInputStreamFactoryImpl factory = new SegmentInputStreamFactoryImpl(controller, cp);
        SegmentInputStream segmentInputStream = factory
                .createInputStreamForSegment(Segment.fromScopedName("scope/stream/0"), new EmptyTokenProviderImpl());
        assertEquals(0, segmentInputStream.getOffset());
    }

    @Test
    public void testCreateInputStreamForSegmentWithOffset() {
        SegmentInputStreamFactoryImpl factory = new SegmentInputStreamFactoryImpl(controller, cp);
        SegmentInputStream segmentInputStream = factory
                .createInputStreamForSegment(Segment
                        .fromScopedName("scope/stream/0"), new EmptyTokenProviderImpl(), 100);
        assertEquals(100, segmentInputStream.getOffset());
        assertEquals(Segment.fromScopedName("scope/stream/0"), segmentInputStream.getSegmentId());

    }

    @Test
    public void testCreateInputStreamForSegmentWithOffsetAndBufferSize() {
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController mockController = new MockController("localhost", -1, connectionFactory, false);
        SegmentInputStreamFactoryImpl streamFactory = new SegmentInputStreamFactoryImpl(mockController, connectionFactory);
        @Cleanup
        EventSegmentReader reader = streamFactory.createEventReaderForSegment(Segment.fromScopedName("scope/stream/0"), 100L, 10);
        assertEquals(100, reader.getOffset());
        assertEquals(Segment.fromScopedName("scope/stream/0"), reader.getSegmentId());

    }

    @Test(timeout = 10000)
    public void testGetDelegationTokenProviderWithCacheHitAndNotExpire() {
        final String testScope = "testScope";
        final String testStream = "testStream";
        val expirationTimeNanos = 100;
        val evictions = new HashMap<String, DelegationTokenProvider>();
        Supplier<Long> currentTime = new AtomicLong(expirationTimeNanos - 1)::get;
        SimpleCache<String, DelegationTokenProvider> simpleCache = new SimpleCache<>(10, Duration.ofMillis(100), evictions::put,
                currentTime);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController mockController = new MockController("localhost", -1, connectionFactory, false);
        SegmentInputStreamFactoryImpl inputStreamFactory = spy(new SegmentInputStreamFactoryImpl(mockController, connectionFactory, simpleCache));

        DelegationTokenProvider mockDelegationTokenProvider = mock(DelegationTokenProvider.class);
        final String testScopedStreamName = NameUtils.getScopedStreamName(testScope, testStream);
        simpleCache.put(testScopedStreamName, mockDelegationTokenProvider);

        CompletableFuture<String> tokenFuture = new CompletableFuture<>();
        tokenFuture.complete("testToken");
        doReturn(tokenFuture).when(mockDelegationTokenProvider).retrieveToken();
        DelegationTokenProvider delegationTokenProvider = inputStreamFactory.getDelegationTokenProvider(testScope, testStream);
        DelegationTokenProvider newDelegationTokenProvider = inputStreamFactory.getDelegationTokenProvider(testScope, "testStream2");

        Assert.assertSame(delegationTokenProvider, mockDelegationTokenProvider);
        Assert.assertEquals(simpleCache.size(), 2);
        Assert.assertTrue(Objects.nonNull(newDelegationTokenProvider));
    }

    @Test(timeout = 10000)
    public void testGetDelegationTokenProviderWithCacheHitButExpire() {
        final String testScope = "testScope";
        final String testStream = "testStream";
        int expirationTimeNanos = 100;
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController mockController = new MockController("localhost", -1, connectionFactory, false);
        val evictions = new HashMap<String, DelegationTokenProvider>();
        Supplier<Long> currentTime = new AtomicLong(expirationTimeNanos + 1)::get;
        SimpleCache<String, DelegationTokenProvider> simpleCache = new SimpleCache<>(2, Duration.ofNanos(expirationTimeNanos), evictions::put,
                currentTime);
        SegmentInputStreamFactoryImpl inputStreamFactory = spy(new SegmentInputStreamFactoryImpl(mockController, connectionFactory, simpleCache));
        DelegationTokenProvider mockDelegationTokenProvider = mock(DelegationTokenProvider.class);
        final String testScopedStream = NameUtils.getScopedStreamName(testScope, testStream);
        simpleCache.put(testScopedStream, mockDelegationTokenProvider);

        CompletableFuture<String> tokenFuture = new CompletableFuture<>();
        tokenFuture.complete("testToken");
        DelegationTokenProvider mockNewDelegationTokenProvider = mock(DelegationTokenProvider.class);
        doReturn(mockNewDelegationTokenProvider).when(inputStreamFactory).getDelegationTokenProvider(anyString(), anyString());
        doReturn(tokenFuture).when(mockNewDelegationTokenProvider).retrieveToken();
        DelegationTokenProvider delegationTokenProvider =  inputStreamFactory.getDelegationTokenProvider(testScope, testStream);

        Assert.assertNotSame(delegationTokenProvider, mockDelegationTokenProvider);
    }
}