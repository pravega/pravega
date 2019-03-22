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
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@RunWith(SystemTestRunner.class)
public abstract class MetadataScalabilityTest extends AbstractScaleTests {
    private final String streamName = getStreamName();

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
        Boolean createStreamStatus = controller.createStream(SCOPE, getStreamName(), getStreamConfig()).get();
        log.debug("create stream status for scale up stream {}", createStreamStatus);
    }

    @After
    public void tearDown() {
        getClientFactory().close();
        getConnectionFactory().close();
        getController().close();
        ExecutorServiceHelpers.shutdown(executorService, scaleExecutorService);
    }

    abstract StreamConfiguration getStreamConfig();

    abstract String getStreamName();

    abstract int getScalesToPerform();

    abstract Pair<List<Long>, Map<Double, Double>> getScaleInput(ArrayList<Segment> sortedCurrentSegments);

    List<List<Segment>>  scale(ControllerImpl controller) {
        int numSegments = getStreamConfig().getScalingPolicy().getMinNumSegments();
        int scalesToPerform = getScalesToPerform();

        // manually scale the stream SCALES_TO_PERFORM times
        Stream stream = new StreamImpl(SCOPE, getStreamName());
        AtomicInteger counter = new AtomicInteger(0);
        List<List<Segment>> listOfEpochs = new LinkedList<>();

        CompletableFuture<Void> scaleFuture = Futures.loop(() -> counter.incrementAndGet() <= scalesToPerform,
                () -> controller.getCurrentSegments(SCOPE, streamName)
                                .thenCompose(segments -> {
                                    ArrayList<Segment> sorted = Lists.newArrayList(segments.getSegments().stream()
                                                                                           .sorted(Comparator.comparingInt(x ->
                                                                                                   StreamSegmentNameUtils.getSegmentNumber(x.getSegmentId()) % numSegments))
                                                                                           .collect(Collectors.toList()));
                                    listOfEpochs.add(sorted);
                                    // note: with SCALES_TO_PERFORM < numSegments, we can use the segment number as the index
                                    // into the range map
                                    Pair<List<Long>, Map<Double, Double>> scaleInput = getScaleInput(sorted);
                                    List<Long> segmentsToSeal = scaleInput.getKey();
                                    Map<Double, Double> newRanges = scaleInput.getValue();

                                    return controller.scaleStream(stream, segmentsToSeal, newRanges, executorService)
                                                     .getFuture()
                                                     .thenAccept(scaleStatus -> {
                                                         log.info("scale stream for epoch {} completed with status {}", counter.get(), scaleStatus);
                                                         assert scaleStatus;
                                                     });
                                }), executorService);

        scaleFuture.join();
        
        return listOfEpochs;
    }
     
     void truncation(ControllerImpl controller, List<List<Segment>> listOfEpochs) {
         int numSegments = getStreamConfig().getScalingPolicy().getMinNumSegments();
         int scalesToPerform = getScalesToPerform();
         Stream stream = new StreamImpl(SCOPE, getStreamName());

         // try SCALES_TO_PERFORM randomly generated stream cuts and truncate stream at those 
         // stream cuts. 
         List<AtomicInteger> indexes = new LinkedList<>();
         Random rand = new Random();
         for (int i = 0; i < numSegments; i++) {
             indexes.add(new AtomicInteger(1));
         }
         Futures.loop(() -> indexes.stream().allMatch(x -> x.get() < scalesToPerform - 1), () -> {
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
             return controller.truncateStream(SCOPE, streamName, cut).
                     thenCompose(truncated -> {
                         log.info("stream truncated successfully at {}", cut);
                         assert truncated;
                         // we will just validate that a non empty value is returned. 
                         return controller.getSuccessors(cut)
                                          .thenAccept(successors -> {
                                              log.info("Successors for streamcut {} are {}", cut, successors);
                                          });
                     });
         }, executorService).join();
     }
    
    void sealAndDeleteStream(ControllerImpl controller) {
        controller.sealStream(SCOPE, streamName).join();
        controller.deleteStream(SCOPE, streamName).join();
    }
}
