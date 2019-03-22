/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ScopeTest {
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    //@Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
    }

    //@After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 30000)
    public void testScale() throws Exception {
        final String scope = "test";
        final String streamName1 = "test1";
        final String streamName2 = "test2";
        final String streamName3 = "test3";
        final Map<String, Integer> foundCount = new HashMap<>();
        foundCount.put(streamName1, 0);
        foundCount.put(streamName2, 0);
        foundCount.put(streamName3, 0);
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();

        @Cleanup
        Controller controller = controllerWrapper.getController();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://localhost"))
                                                                                    .build());

        controllerWrapper.getControllerService().createScope(scope).get();
        controller.createStream(scope, streamName1, config).get();
        controller.createStream(scope, streamName2, config).get();
        controller.createStream(scope, streamName3, config).get();

        StreamManager manager = new StreamManagerImpl(controller, connectionFactory);

        Iterator<Stream> iterator = manager.listStreams(scope);
        assertTrue(iterator.hasNext());
        Stream next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertTrue(iterator.hasNext());
        next = iterator.next();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        assertFalse(iterator.hasNext());

        assertTrue(foundCount.entrySet().stream().allMatch(x -> x.getValue() == 1));

        AsyncIterator<Stream> asyncIterator = controller.listStreams(scope);
        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        foundCount.computeIfPresent(next.getStreamName(), (x, y) -> ++y);

        next = asyncIterator.getNext().join();
        assertNull(next);

        assertTrue(foundCount.entrySet().stream().allMatch(x -> x.getValue() == 2));
    }

    public static final int NUM_SEGMENTS = 10000;
    public static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(5);

    @Test
    public void scalability2() {
        for(int xi = 0; xi < 1; xi++) {
            String scope = "scope";
            String streamName = "stream17" + new Random().nextInt();

            ClientConfig clientConfig = ClientConfig.builder()
                                                    .controllerURI(URI.create("tcp://127.0.0.1:9090"))
                                                    .build();
            ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder()
                                                                               .clientConfig(clientConfig)
                                                                               .build(), EXECUTOR);

            controller.createScope(scope).join();
            StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(NUM_SEGMENTS)).build();
            controller.createStream(scope, streamName, config).join();

            int numSegments = config.getScalingPolicy().getMinNumSegments();
            int scalesToPerform = 10;

            // manually scale the stream SCALES_TO_PERFORM times
            Stream stream = new StreamImpl(scope, streamName);
            AtomicInteger counter = new AtomicInteger(0);
            List<List<Segment>> listOfEpochs = new LinkedList<>();

            CompletableFuture<Void> scaleFuture = Futures.loop(() -> counter.incrementAndGet() <= scalesToPerform,
                    () -> controller.getCurrentSegments(scope, streamName)
                                    .thenCompose(segments -> {
                                        ArrayList<Segment> sorted = Lists.newArrayList(segments.getSegments().stream()
                                                                                               .sorted(Comparator.comparingInt(x ->
                                                                                                       StreamSegmentNameUtils.getSegmentNumber(x.getSegmentId()) % numSegments))
                                                                                               .collect(Collectors.toList()));
                                        listOfEpochs.add(sorted);
                                        Pair<List<Long>, Map<Double, Double>> scaleInput = getScaleInput(sorted);
                                        List<Long> segmentsToSeal = scaleInput.getKey();
                                        Map<Double, Double> newRanges = scaleInput.getValue();

                                        return controller.scaleStream(stream, segmentsToSeal, newRanges, executor)
                                                         .getFuture()
                                                         .thenAccept(scaleStatus -> {
                                                             log.info("scale stream for epoch {} completed with status {}", counter.get(), scaleStatus);
                                                             assert scaleStatus;
                                                         });
                                    }), executor);

            scaleFuture
                    .thenCompose(r -> {
                        // try SCALES_TO_PERFORM randomly generated stream cuts and truncate stream at those 
                        // stream cuts. 
                        List<AtomicInteger> indexes = new LinkedList<>();
                        Random rand = new Random();
                        for (int i = 0; i < numSegments; i++) {
                            indexes.add(new AtomicInteger(1));
                        }
                        return Futures.loop(() -> indexes.stream().allMatch(x -> x.get() < scalesToPerform - 1), () -> {
                            // randomly generate a stream cut. 
                            // Note: From epoch 1 till epoch SCALES_TO_PERFORM each epoch is made up of 10k segments
                            // and the range is statically partitioned evenly. 
                            // So a random, correct streamcut would be choosing numSegments disjoint segments from numSegments random epochs. 
                            Map<Segment, Long> map = new HashMap<>();
                            for (int i = 0; i < numSegments; i++) {
                                AtomicInteger index = indexes.get(i);
                                index.set(index.get() + rand.nextInt(scalesToPerform - index.get()));
                                map.put(listOfEpochs.get(index.get()).get(i), 0L);
                            }

                            StreamCut cut = new StreamCutImpl(stream, map);
                            log.info("truncating stream at {}", map);
                            return controller.truncateStream(scope, streamName, cut).
                                    thenCompose(truncated -> {
                                        log.info("stream truncated successfully at {}", cut);
                                        assert truncated;
                                        // we will just validate that a non empty value is returned. 
                                        return controller.getSuccessors(cut)
                                                         .thenAccept(successors -> {
                                                             log.info("Successors for streamcut {} are {}", cut, successors);
                                                         });
                                    });
                        }, executor);
                    }).join();
        }
    }

    AtomicInteger counter = new AtomicInteger(0);
    Pair<List<Long>, Map<Double, Double>> getScaleInput(ArrayList<Segment> sorted) {
        int i = counter.incrementAndGet();
        List<Long> segmentsToSeal = sorted.stream()
                                          .filter(x -> i - 1 == StreamSegmentNameUtils.getSegmentNumber(x.getSegmentId()) % NUM_SEGMENTS)
                                          .map(Segment::getSegmentId).collect(Collectors.toList());
        Map<Double, Double> newRanges = new HashMap<>();
        double delta = 1.0 / NUM_SEGMENTS;
        newRanges.put(delta * (i - 1), delta * counter.get());

        return new ImmutablePair<>(segmentsToSeal, newRanges);
    }

    Pair<List<Long>, Map<Double, Double>> getScaleInput2(ArrayList<Segment> sortedCurrentSegments) {
        return new ImmutablePair<>(getSegmentsToSeal(sortedCurrentSegments), getNewRanges());
    }

    private List<Long> getSegmentsToSeal(ArrayList<Segment> sorted) {
        return sorted.stream()
                     .map(Segment::getSegmentId).collect(Collectors.toList());
    }

    private Map<Double, Double> getNewRanges() {
        Map<Double, Double> newRanges = new HashMap<>();
        double delta = 1.0 / NUM_SEGMENTS;
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            double low = delta * i;
            double high = i == NUM_SEGMENTS - 1 ? 1.0 : delta * (i + 1);

            newRanges.put(low, high);
        }
        return newRanges;
    }

}

