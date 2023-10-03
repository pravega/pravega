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
package io.pravega.segmentstore.server.host.stat;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.SimpleCache;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.NonNull;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AutoScaleProcessorTest extends ThreadPooledTestSuite {

    private static final String SCOPE = "scope";
    private static final String STREAM1 = "stream1";
    private static final String STREAM2 = "stream2";
    private static final String STREAM3 = "stream3";
    private static final String STREAM4 = "stream4";
    private boolean authEnabled = false;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Test (timeout = 10000)
    public void writerCreationTest() throws Exception {
        EventStreamClientFactory clientFactory = mock(EventStreamClientFactory.class);
        CompletableFuture<Void> createWriterLatch = new CompletableFuture<>();
        doAnswer(x -> {
            createWriterLatch.complete(null);
            throw new RuntimeException();
        }).when(clientFactory).createEventWriter(any(), any(), any());

        TestAutoScaleProcessor failingWriterProcessor = new TestAutoScaleProcessor(
                AutoScalerConfig.builder().with(AutoScalerConfig.CONTROLLER_URI, "tcp://localhost:9090").build(),
                clientFactory,
                executorService());
        String segmentStreamName = "scope/myStreamSegment/0.#epoch.0";
        failingWriterProcessor.notifyCreated(segmentStreamName);
        assertFalse(failingWriterProcessor.isInitializeStarted());
        AtomicReference<EventStreamWriter<AutoScaleEvent>> w = new AtomicReference<>();

        AssertExtensions.assertThrows("Bootstrap should not be initiated until isInitializeStarted is true",
                () -> failingWriterProcessor.bootstrapOnce(clientFactory, w),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);

        // report but since the cooldown time hasnt elapsed, no scale event should be attempted. So no writer should be initialized yet. 
        failingWriterProcessor.report(segmentStreamName, 1, 0L, 10.0, 10.0, 10.0, 10.0);
        assertFalse(failingWriterProcessor.isInitializeStarted());

        failingWriterProcessor.setTimeMillis(20 * 60000L);
        failingWriterProcessor.report(segmentStreamName, 1, 0L, 10.0, 10.0, 10.0, 10.0);
        // the above should initiate the bootstrap.
        assertTrue(failingWriterProcessor.isInitializeStarted());

        // since we are throwing on writer creation, wait until the writer is invoked once at least
        createWriterLatch.join();

        // now close the processor. The writer future should get cancelled.
        failingWriterProcessor.close();
        assertTrue(failingWriterProcessor.getWriterFuture().isCancelled());

        // create new processor and let the writer get created 
        TestAutoScaleProcessor processor = new TestAutoScaleProcessor(
                AutoScalerConfig.builder().with(AutoScalerConfig.CONTROLLER_URI, "tcp://localhost:9090").build(),
                clientFactory,
                executorService());

        LinkedBlockingQueue<AutoScaleEvent> queue = new LinkedBlockingQueue<>();
        EventStreamWriter<AutoScaleEvent> writerMock = createWriter(queue::add);
        doAnswer(x -> writerMock).when(clientFactory).createEventWriter(any(), any(), any());

        processor.notifyCreated(segmentStreamName);

        // report a low rate to trigger a scale down 
        processor.setTimeMillis(21 * 60000L);
        processor.report(segmentStreamName, 10, 0L, 1.0, 1.0, 1.0, 1.0);
        assertTrue(processor.isInitializeStarted());

        AssertExtensions.assertEventuallyEquals(writerMock, () -> processor.getWriterFuture().join(), 10000L);
        AutoScaleEvent event = queue.take();
        assertEquals(event.getDirection(), AutoScaleEvent.DOWN);

        processor.close();

        // create third writer, this time supply the writer directly
        EventStreamWriter<AutoScaleEvent> writer = spy(createWriter(e -> { }));

        // verify that when writer is set, we are able to get the processor initialized
        TestAutoScaleProcessor processor2 = new TestAutoScaleProcessor(writer,
                AutoScalerConfig.builder().with(AutoScalerConfig.CONTROLLER_URI, "tcp://localhost:9090").build(),
                executorService());

        processor2.notifyCreated(segmentStreamName);
        assertFalse(processor2.isInitializeStarted());
        processor2.setTimeMillis(20 * 60000L);
        processor2.report(segmentStreamName, 1, 0L, 10.0, 10.0, 10.0, 10.0);
        // the above should create a writer future. 
        assertTrue(processor2.isInitializeStarted());

        assertTrue(Futures.isSuccessful(processor2.getWriterFuture()));
        processor2.close();
        verify(writer, times(1)).close();
    }

    @Test (timeout = 10000)
    public void scaleTest() {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<Void> result2 = new CompletableFuture<>();
        CompletableFuture<Void> result3 = new CompletableFuture<>();
        CompletableFuture<Void> result4 = new CompletableFuture<>();
        EventStreamWriter<AutoScaleEvent> writer = createWriter(event -> {
            if (event.getScope().equals(SCOPE) &&
                    event.getStream().equals(STREAM1) &&
                    event.getDirection() == AutoScaleEvent.UP) {
                result.complete(null);
            }

            if (event.getScope().equals(SCOPE) &&
                    event.getStream().equals(STREAM2) &&
                    event.getDirection() == AutoScaleEvent.DOWN) {
                result2.complete(null);
            }

            if (event.getScope().equals(SCOPE) &&
                    event.getStream().equals(STREAM3) &&
                    event.getDirection() == AutoScaleEvent.DOWN) {
                result3.complete(null);
            }

            if (event.getScope().equals(SCOPE) &&
                    event.getStream().equals(STREAM4) &&
                    event.getDirection() == AutoScaleEvent.UP &&
                    event.getNumOfSplits() == 2) {
                result4.complete(null);
            }
        });
        @Cleanup
        AutoScaleProcessor monitor = new AutoScaleProcessor(writer,
                AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                        .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                        .with(AutoScalerConfig.AUTH_ENABLED, authEnabled)
                        .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 1)
                        .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 1).build(),
                executorService());

        String streamSegmentName1 = NameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM1, 0L);
        String streamSegmentName2 = NameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM2, 0L);
        String streamSegmentName3 = NameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM3, 0L);
        String streamSegmentName4 = NameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM4, 0L);
        monitor.notifyCreated(streamSegmentName1);
        monitor.notifyCreated(streamSegmentName2);
        monitor.notifyCreated(streamSegmentName3);
        monitor.notifyCreated(streamSegmentName4);

        long twentyminutesback = System.currentTimeMillis() - Duration.ofMinutes(20).toMillis();
        monitor.put(streamSegmentName1, new ImmutablePair<>(twentyminutesback, twentyminutesback));
        monitor.put(streamSegmentName3, new ImmutablePair<>(twentyminutesback, twentyminutesback));

        monitor.report(streamSegmentName1, 10, twentyminutesback, 1001, 500, 200, 200);

        monitor.report(streamSegmentName3, 10, twentyminutesback, 0.0, 0.0, 0.0, 0.0);

        monitor.report(streamSegmentName4, 10, twentyminutesback, 0.0, 0.0, 10.10, 0.0);

        monitor.notifySealed(streamSegmentName1);
        assertTrue(Futures.await(result));
        assertTrue(Futures.await(result2));
        assertTrue(Futures.await(result3));
        assertTrue(Futures.await(result4));
    }

    @Test(timeout = 10000)
    public void testCacheExpiry() {
        CompletableFuture<Void> scaleDownFuture = new CompletableFuture<>();

        AutoScaleProcessor monitor = new AutoScaleProcessor(createWriter(event -> {
            if (event.getDirection() == AutoScaleEvent.DOWN) {
                scaleDownFuture.complete(null);
            } else {
                scaleDownFuture.completeExceptionally(new RuntimeException());
            }
        }), AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 1)
                .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 1).build(),
                executorService());
        String streamSegmentName1 = NameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM1, 0L);
        monitor.notifyCreated(streamSegmentName1);

        assertTrue(Futures.await(scaleDownFuture));

        assertNull(monitor.get(streamSegmentName1));
    }

    @Test
    public void testHasTlsEnabledThrowsExceptionWhenInputIsNull() {
        AssertExtensions.assertThrows("Accepted null argument",
                () -> AutoScaleProcessor.hasTlsEnabled(null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testHasTlsEnabledReturnsFalseWhenSchemeIsNull() {
        assertFalse(AutoScaleProcessor.hasTlsEnabled(URI.create("//localhost:9090")));
    }

    @Test
    public void testHasTlsEnabledReturnsFalseWhenSchemeIsTlsIncompatible() {
        assertFalse(AutoScaleProcessor.hasTlsEnabled(URI.create("tcp://localhost:9090")));

        assertFalse(AutoScaleProcessor.hasTlsEnabled(URI.create("pravega://localhost:9090")));

        assertFalse(AutoScaleProcessor.hasTlsEnabled(URI.create("unsupported://localhost:9090")));
    }

    @Test
    public void testHasTlsEnabledReturnsTrueWhenSchemeIsTlsCompatible() {
        assertTrue(AutoScaleProcessor.hasTlsEnabled(URI.create("tls://localhost:9090")));

        assertTrue(AutoScaleProcessor.hasTlsEnabled(URI.create("pravegas://localhost:9090")));
    }

    @Test
    public void testPrepareClientConfigForTlsDisabledInput() {
        AutoScalerConfig config = AutoScalerConfig.builder()
                .with(AutoScalerConfig.CONTROLLER_URI, "tcp://localhost:9090")
                .with(AutoScalerConfig.TLS_ENABLED, false)
                .build();
        ClientConfig objectUnderTest = AutoScaleProcessor.prepareClientConfig(config);

        assertFalse(objectUnderTest.isEnableTls());
        assertEquals("tcp://localhost:9090", objectUnderTest.getControllerURI().toString());
    }

    @Test
    public void testPrepareClientConfigForMixedTlsInput() {
        // Notice that we are setting non-TLS scheme for the Controller, but enabling TLS for the auto scaler.
        AutoScalerConfig config = AutoScalerConfig.builder()
                .with(AutoScalerConfig.CONTROLLER_URI, "tcp://localhost:9090")
                .with(AutoScalerConfig.TLS_ENABLED, true)
                .with(AutoScalerConfig.TLS_CERT_FILE, SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .with(AutoScalerConfig.VALIDATE_HOSTNAME, false)
                .build();
        ClientConfig objectUnderTest = AutoScaleProcessor.prepareClientConfig(config);

        assertFalse(objectUnderTest.isEnableTls());
        assertFalse(objectUnderTest.isEnableTlsToController());
        assertTrue(objectUnderTest.isEnableTlsToSegmentStore());
    }

    @Test
    public void testPrepareClientConfigWhenTlsIsEnabled() {
        // Notice that we are setting non-TLS scheme for the Controller, but enabling TLS for the auto scaler.
        AutoScalerConfig config = AutoScalerConfig.builder()
                .with(AutoScalerConfig.CONTROLLER_URI, "tls://localhost:9090")
                .with(AutoScalerConfig.TLS_ENABLED, true)
                .with(AutoScalerConfig.TLS_CERT_FILE, SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME)
                .with(AutoScalerConfig.VALIDATE_HOSTNAME, false)
                .build();
        ClientConfig objectUnderTest = AutoScaleProcessor.prepareClientConfig(config);

        assertTrue(objectUnderTest.isEnableTls());
        assertTrue(objectUnderTest.isEnableTlsToController());
        assertTrue(objectUnderTest.isEnableTlsToSegmentStore());
    }


    private EventStreamWriter<AutoScaleEvent> createWriter(Consumer<AutoScaleEvent> consumer) {
        return new EventStreamWriter<AutoScaleEvent>() {
            @Override
            public CompletableFuture<Void> writeEvent(AutoScaleEvent event) {
                return CompletableFuture.<Void>completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> writeEvent(String routingKey, AutoScaleEvent event) {
                consumer.accept(event);
                return CompletableFuture.<Void>completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> writeEvents(String routingKey, List<AutoScaleEvent> events) {
                throw new NotImplementedException("mock doesnt require this");
            }

            @Override
            public EventWriterConfig getConfig() {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public void close() {

            }

            @Override
            public void noteTime(long timestamp) {

            }
        };
    }

    private static class TestAutoScaleProcessor extends AutoScaleProcessor {
        private AtomicLong timeMillis = new AtomicLong();

        TestAutoScaleProcessor(@NonNull EventStreamWriter<AutoScaleEvent> writer,
                               @NonNull AutoScalerConfig configuration, @NonNull ScheduledExecutorService executor) {
            super(writer, configuration, executor);
        }

        TestAutoScaleProcessor(@NonNull AutoScalerConfig configuration, EventStreamClientFactory clientFactory, @NonNull ScheduledExecutorService executor) {
            super(configuration, clientFactory, executor);
        }

        TestAutoScaleProcessor(@NonNull AutoScalerConfig configuration, EventStreamClientFactory clientFactory,
                               @NonNull ScheduledExecutorService executor,
                               SimpleCache<String, Pair<Long, Long>> testSimpleCache) {
            super(configuration, clientFactory, executor, testSimpleCache);
        }

        @Override
        protected long getTimeMillis() {
            return timeMillis.get();
        }

        protected void setTimeMillis(long time) {
            timeMillis.set(time);
        }
    }

    @Test
    public void testSteadyStateExpiry() {
        HashMap<String, Pair<Long, Long>> map = new HashMap<>();
        HashMap<String, Long> lastAccessedTime = new HashMap<>();
        List<String> evicted = new ArrayList<>();
        @SuppressWarnings("unchecked")
        SimpleCache<String, Pair<Long, Long>> simpleCache = mock(SimpleCache.class);
        AtomicLong clock = new AtomicLong(0L);
        Function<Void, Void> cleanup = m -> {
            for (Map.Entry<String, Long> e : lastAccessedTime.entrySet()) {
                if (e.getValue() < clock.get()) {
                    lastAccessedTime.remove(e.getKey());
                    map.remove(e.getKey());
                    evicted.add(e.getKey());
                }
            }
            // remove all that should have expired.
            return null;
        };
        doAnswer(x -> {
            cleanup.apply(null);
            return map.get(x.getArgument(0));
        }).when(simpleCache).get(anyString());
        doAnswer(x -> {
            cleanup.apply(null);
            map.put(x.getArgument(0), x.getArgument(1));
            return map.get(x.getArgument(0));
        }).when(simpleCache).put(anyString(), any());
        doAnswer(x -> cleanup.apply(null)).when(simpleCache).cleanUp();

        AutoScalerConfig config = AutoScalerConfig.builder()
                .with(AutoScalerConfig.CONTROLLER_URI, "tcp://localhost:9090")
                .with(AutoScalerConfig.TLS_ENABLED, false)
                .build();
        ClientConfig objectUnderTest = AutoScaleProcessor.prepareClientConfig(config);
        @Cleanup
        EventStreamClientFactory eventStreamClientFactory = EventStreamClientFactory.withScope(SCOPE, objectUnderTest);

        @Cleanup
        TestAutoScaleProcessor monitor = new TestAutoScaleProcessor(
                AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                        .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                        .with(AutoScalerConfig.AUTH_ENABLED, authEnabled)
                        .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 150)
                        .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 60).build(), eventStreamClientFactory,
                executorService(), simpleCache);
        String streamSegmentName1 = NameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM1, 0L);
        monitor.setTimeMillis(0L);
        clock.set(0L);
        monitor.notifyCreated(streamSegmentName1);
        monitor.put(streamSegmentName1, new ImmutablePair<>(5L, 5L));
        monitor.setTimeMillis(30 * 1000L);
        clock.set(30L);
        monitor.report(streamSegmentName1, 10L, 0L, 10D, 10D, 10D, 10D);
        monitor.setTimeMillis(80 * 1000L);
        clock.set(80L);
        simpleCache.cleanUp();
        assertNotNull(monitor.get(streamSegmentName1));
        assertNotNull(simpleCache.get(streamSegmentName1));
        assertTrue(evicted.isEmpty());

        AssertExtensions.assertThrows("NPE should be thrown",
                () -> new AutoScaleProcessor(null, config, executorService()),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("NPE should be thrown",
                () -> new AutoScaleProcessor(null, eventStreamClientFactory, executorService()),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("NPE should be thrown",
                () -> new AutoScaleProcessor(AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                        .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                        .with(AutoScalerConfig.AUTH_ENABLED, authEnabled)
                        .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 150)
                        .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 60).build(), eventStreamClientFactory, null),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("NPE should be thrown",
                () -> new AutoScaleProcessor(AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                        .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                        .with(AutoScalerConfig.AUTH_ENABLED, authEnabled)
                        .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 150)
                        .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 60).build(), eventStreamClientFactory, null, simpleCache),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("NPE should be thrown",
                () -> new AutoScaleProcessor(null, eventStreamClientFactory, executorService(), simpleCache),
                e -> e instanceof NullPointerException);

        monitor.notifySealed(streamSegmentName1);
    }
}