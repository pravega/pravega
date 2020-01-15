/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.stat;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

        AutoScaleProcessor monitor = new AutoScaleProcessor(writer,
                AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                        .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0)
                        .with(AutoScalerConfig.AUTH_ENABLED, authEnabled)
                        .with(AutoScalerConfig.CACHE_CLEANUP_IN_SECONDS, 1)
                        .with(AutoScalerConfig.CACHE_EXPIRY_IN_SECONDS, 1).build(),
                executorService());

        String streamSegmentName1 = StreamSegmentNameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM1, 0L);
        String streamSegmentName2 = StreamSegmentNameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM2, 0L);
        String streamSegmentName3 = StreamSegmentNameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM3, 0L);
        String streamSegmentName4 = StreamSegmentNameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM4, 0L);
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
        String streamSegmentName1 = StreamSegmentNameUtils.getQualifiedStreamSegmentName(SCOPE, STREAM1, 0L);
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
}