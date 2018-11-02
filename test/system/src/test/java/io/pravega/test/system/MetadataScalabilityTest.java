/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import com.google.common.collect.Lists;
import io.pravega.client.ClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@RunWith(SystemTestRunner.class)
public class MetadataScalabilityTest extends AbstractScaleTests {
    private static final String STREAM_NAME = "metadataScalability";

    private static final StreamConfiguration CONFIG = StreamConfiguration.builder().scope(SCOPE)
                                                                         .streamName(STREAM_NAME).scalingPolicy(ScalingPolicy.fixed(5)).build();
    private static final int TOTAL_NUMBER_OF_SCALES_TO_PERFORM = 1010;
    //The execution time for @Before + @After + @Test methods should be less than 10 mins. Else the test will timeout.
    @Rule
    public Timeout globalTimeout = Timeout.seconds(30 * 60);

    private final ScheduledExecutorService scaleExecutorService = Executors.newScheduledThreadPool(5);

    @Environment
    public static void initialize() {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    /**
     * Invoke the createStream method, ensure we are able to create stream.
     *
     * @throws InterruptedException if interrupted
     * @throws URISyntaxException   If URI is invalid
     * @throws ExecutionException   if error in create stream
     */
    @Before
    public void setup() throws InterruptedException, ExecutionException {

        //create a scope
        Controller controller = getController();
        executorService = ExecutorServiceHelpers.newScheduledThreadPool(5, "Scalability-main");
        Boolean createScopeStatus = controller.createScope(SCOPE).get();
        log.debug("create scope status {}", createScopeStatus);

        //create a stream
        Boolean createStreamStatus = controller.createStream(CONFIG).get();
        log.debug("create stream status for scale up stream {}", createStreamStatus);
    }

    @After
    public void tearDown() {
        getClientFactory().close();
        getConnectionFactory().close();
        getController().close();
        ExecutorServiceHelpers.shutdown(executorService, scaleExecutorService);
    }

    @Test
    public void scalability() {
        testState = new TestState(false);

        ClientFactory clientFactory = getClientFactory();
        ControllerImpl controller = getController();
        createWriters(clientFactory, 6, SCOPE, STREAM_NAME);
        
        Map<Double, Double> newRanges = new HashMap<>();
        newRanges.put(0.0, 0.2);
        newRanges.put(0.2, 0.4);
        newRanges.put(0.4, 0.6);
        newRanges.put(0.6, 0.8);
        newRanges.put(0.8, 1.0);
        // manually scale the stream TOTAL_NUMBER_OF_SCALES_TO_PERFORM times
        Stream stream = new StreamImpl(SCOPE, STREAM_NAME);
        AtomicInteger counter = new AtomicInteger(0);
        List<List<Segment>> listOfEpochs = new LinkedList<>();
        
        CompletableFuture<Void> scaleFuture = Futures.loop(() -> counter.incrementAndGet() <= TOTAL_NUMBER_OF_SCALES_TO_PERFORM,
                () -> controller.getCurrentSegments(SCOPE, STREAM_NAME)
                                .thenCompose(segments -> {
                                    listOfEpochs.add(Lists.newArrayList(segments.getSegments()));
                                    List<Long> sealedSegments = segments.getSegments().stream()
                                                                        .map(Segment::getSegmentId).collect(Collectors.toList());
                                    return controller.scaleStream(stream, sealedSegments, newRanges, executorService)
                                                                    .getFuture()
                                            .thenAccept(scaleStatus -> {
                                                assert scaleStatus;
                                                log.debug("scale stream status for epoch {} completed", counter.get());
                                            });
                                }), executorService);

        CompletableFuture<Void> result = scaleFuture
                .thenCompose(r -> {
                    // try TOTAL_NUMBER_OF_SCALES_TO_PERFORM randomly generated stream cuts and truncate stream at those 
                    // stream cuts. 
                    AtomicInteger index1 = new AtomicInteger(1);
                    AtomicInteger index2 = new AtomicInteger(1);
                    AtomicInteger index3 = new AtomicInteger(1);
                    AtomicInteger index4 = new AtomicInteger(1);
                    AtomicInteger index5 = new AtomicInteger(1);
                    return Futures.loop(() -> counter.decrementAndGet() > 0, () -> {
                        // randomly generate a stream cut. 
                        // Note: From epoch 1 till epoch TOTAL_NUMBER_OF_SCALES_TO_PERFORM each epoch is made up of 5 segments
                        // and the range is statically partitioned evenly. 
                        // So a random, correct streamcut would be choosing 5 disjoint segments from 5 random epochs. 
                        Map<Segment, Long> map = new HashMap<>();
                        index1.set(index1.get() + new Random().nextInt(TOTAL_NUMBER_OF_SCALES_TO_PERFORM + 1 - index1.get()));
                        index2.set(index2.get() + new Random().nextInt(TOTAL_NUMBER_OF_SCALES_TO_PERFORM + 1 - index2.get()));
                        index3.set(index3.get() + new Random().nextInt(TOTAL_NUMBER_OF_SCALES_TO_PERFORM + 1 - index3.get()));
                        index4.set(index4.get() + new Random().nextInt(TOTAL_NUMBER_OF_SCALES_TO_PERFORM + 1 - index4.get()));
                        index5.set(index5.get() + new Random().nextInt(TOTAL_NUMBER_OF_SCALES_TO_PERFORM + 1 - index5.get()));
                        map.put(listOfEpochs.get(index1.get()).get(0), 0L);
                        map.put(listOfEpochs.get(index2.get()).get(1), 0L);
                        map.put(listOfEpochs.get(index3.get()).get(2), 0L);
                        map.put(listOfEpochs.get(index4.get()).get(3), 0L);
                        map.put(listOfEpochs.get(index5.get()).get(4), 0L);

                        StreamCut cut = new StreamCutImpl(stream, map);
                        return controller.truncateStream(SCOPE, STREAM_NAME, cut).
                                thenCompose(truncated -> {
                                    // we will just validate that a non empty value is returned.  
                                    assert truncated;
                                    return controller.getSuccessors(cut)
                                            .thenAccept(successors -> {
                                                log.debug("Successors for streamcut {} are {}", cut, successors);
                                            });
                                });
                    }, executorService);
                });
        
        Futures.await(result);
    }
}
