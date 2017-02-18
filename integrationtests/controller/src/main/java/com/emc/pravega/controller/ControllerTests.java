/**
 * Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller;


import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.TxnStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.StreamSegments;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Simple controller stress tests to calculate the time
 * taken for given number of  concurrent calls.
 * Controller Uri,number of calls to each api are user configurable
 * To run the tests:
 * 1../gradlew build
 * 2. cd integrationtests/controller/build/distributions
 * 3. untar controller.tar
 * 4. ./controller/bin/controller -controller <controller uri>  -createstream <createstreamcallcount> .
 */
@Slf4j
public class ControllerTests {

    private static String controllerUri = "http://127.0.0.1:9090";
    private static int createStreamCallCount = 100;
    private static int alterStreamCallCount = 100;
    private static int sealStreamCallCount = 100;
    private static int scaleStreamCallCount = 100;
    private static int getPositionsCallCount = 100;
    private static int getCurrentSegmentsCallCount = 100;
    private static int createTransactionCallCount = 100;
    private static int commitTransactionCallCount = 100;
    private static int dropTransactionCallCount = 100;
    private static long startTime;
    private static long endTime;
    private static long timeTaken;
    private static ArrayList<CompletableFuture<CreateStreamStatus>> createStatusList = new ArrayList<>();
    private static ArrayList<CompletableFuture<UpdateStreamStatus>> alterStatusList = new ArrayList<>();
    private static ArrayList<CompletableFuture<UpdateStreamStatus>> sealStatusList = new ArrayList<>();
    private static ArrayList<CompletableFuture<ScaleResponse>> scaleStatusList = new ArrayList<>();
    private static ArrayList<CompletableFuture<List<PositionInternal>>> getPositionsList = new ArrayList<>();
    private static ArrayList<CompletableFuture<StreamSegments>> getCurrentSegmentsList = new ArrayList<>();
    private static ArrayList<CompletableFuture<UUID>> createTransactionList = new ArrayList<>();
    private static ArrayList<CompletableFuture<TxnStatus>> commitTransactionList = new ArrayList<>();
    private static ArrayList<CompletableFuture<TxnStatus>> dropTransactionList = new ArrayList<>();

    public static void main(String[] args) {
        parseCmdLine(args);
        System.exit(0);
    }

    private static void createStream() {

        ExecutorService createExecutor = Executors.newFixedThreadPool(createStreamCallCount);
        log.debug("\nCalling Create Stream  {} times.The controller endpoint is {}", createStreamCallCount, controllerUri);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < createStreamCallCount; i++) {
            try {
                ControllerImpl controller = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new CreateStream(controller, i);
                createExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("invalid controller uri {}", uri);
                System.exit(0);
            }
        }
        createExecutor.shutdown();
        try {
            createExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of create executor {} ", e);
        }
        CompletableFuture<Void> createAll = FutureHelpers.allOf(createStatusList);
        createAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;

        log.debug("Time taken for {} create stream calls = {} milliseconds ", createStreamCallCount, timeTaken);

        createStatusList.forEach(createStreamStatusCompletableFuture -> {
            try {
                log.debug("Status of each create stream call {}", createStreamStatusCompletableFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on create stream status {}", e);
            }
        });
    }

    private static void alterStream() {

        ExecutorService alterExecutor = Executors.newFixedThreadPool(alterStreamCallCount);
        log.debug("\nCalling Alter Stream {} times.The controller endpoint is {}", alterStreamCallCount, controllerUri);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < alterStreamCallCount; i++) {
            try {
                ControllerImpl controller = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new AlterStream(controller, i);
                alterExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
        }

        alterExecutor.shutdown();
        try {
            alterExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of alter executor {}", e);
        }

        CompletableFuture<Void> alterAll = FutureHelpers.allOf(alterStatusList);
        alterAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;

        log.debug("Time taken for {} alter stream calls = {} milliseconds", alterStreamCallCount, timeTaken);

        alterStatusList.forEach(alterStreamStatusCompletableFuture -> {
            try {
                log.debug("Status of each alter stream call {}", alterStreamStatusCompletableFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on alter stream status {}", e);
            }
        });

        alterExecutor.shutdown();
    }

    private static void sealStream() {

        ExecutorService sealExecutor = Executors.newFixedThreadPool(sealStreamCallCount);
        log.debug("\nCalling Seal Stream {} times.The controller endpoint is {}", sealStreamCallCount, controllerUri);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < sealStreamCallCount; i++) {
            try {
                ControllerImpl controller4 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new SealStream(controller4, i);
                sealExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
        }

        sealExecutor.shutdown();
        try {
            sealExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of seal executor {}", e);
        }

        CompletableFuture<Void> sealAll = FutureHelpers.allOf(sealStatusList);
        sealAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        log.debug("Time taken for {} seal stream calls = {} milliseconds", sealStreamCallCount, timeTaken);

        sealStatusList.forEach(sealStreamStatusCompletableFuture -> {
            try {
                log.debug("Status of each seal stream call {}", sealStreamStatusCompletableFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on seal stream status {}", e);
            }
        });
    }

    private static void scaleStream() {

        ExecutorService scaleExecutor = Executors.newFixedThreadPool(scaleStreamCallCount);
        log.debug("\nCalling Scale Stream {} times.The controller endpoint is {}", scaleStreamCallCount, controllerUri);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < scaleStreamCallCount; i++) {
            try {
                ControllerImpl controller4 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new ScaleStream(controller4, i);
                scaleExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
        }
        scaleExecutor.shutdown();
        try {
            scaleExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of scale executor {}", e);
        }

        CompletableFuture<Void> scaleAll = FutureHelpers.allOf(scaleStatusList);
        scaleAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;

        log.debug("Time taken for {} scale stream calls = {} milliseconds", scaleStreamCallCount, timeTaken);
        scaleStatusList.forEach(scaleResponseCompletableFuture -> {
            try {
                log.debug("Status of each scale stream call {}", scaleResponseCompletableFuture.get().getStatus());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on scale stream status {}", e);
            }
        });

    }

    private static void getPositions() {

        ExecutorService getPositionsExecutor = Executors.newFixedThreadPool(getPositionsCallCount);
        log.debug("\nCalling Get Positions {} times.The controller endpoint is {}", getPositionsCallCount, controllerUri);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < getPositionsCallCount; i++) {
            try {
                ControllerImpl controller4 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new GetPositions(controller4, i);
                getPositionsExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
        }
        getPositionsExecutor.shutdown();
        try {
            getPositionsExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of getpositions executor {}", e);
        }
        CompletableFuture<Void> getPositionsAll = FutureHelpers.allOf(getPositionsList);
        getPositionsAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        log.debug("Time taken for {} getpositions calls = {} milliseconds", getPositionsCallCount, timeTaken);

        getPositionsList.forEach(getPositionsCompletableFuture -> {
            try {
                log.debug("Status of each getposition call {}", getPositionsCompletableFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on getposition status {}", e);
            }
        });
    }

    private static void getCurrentSegments() {

        ExecutorService getCurrentSegmentsExecutor = Executors.newFixedThreadPool(getCurrentSegmentsCallCount);
        log.debug("\nCalling Get Current Segments {} times.The controller endpoint is {}", getCurrentSegmentsCallCount, controllerUri);

        startTime = System.currentTimeMillis();
        for (int i = 0; i < getCurrentSegmentsCallCount; i++) {
            try {
                ControllerImpl controller4 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new GetCurrentSegments(controller4, i);
                getCurrentSegmentsExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
        }
        getCurrentSegmentsExecutor.shutdown();
        try {
            getCurrentSegmentsExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of getcurrentsegments executor {}", e);
        }

        CompletableFuture<Void> getCurrentSegmentsAll = FutureHelpers.allOf(getCurrentSegmentsList);
        getCurrentSegmentsAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        log.debug("Time taken for {} getcurrentsegments calls = {} milliseconds", getCurrentSegmentsCallCount, timeTaken);

        getCurrentSegmentsList.forEach(getCurrentSegmentsCompletableFuture -> {
            try {
                log.debug("Status of each get current segments call {}", getCurrentSegmentsCompletableFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on get current segments status {}", e);
            }
        });

    }

    private static void createTransaction() {

        ExecutorService createTransactionExecutor = Executors.newFixedThreadPool(createTransactionCallCount);
        log.debug("\nCalling create transaction {} times.The controller endpoint is {}", createTransactionCallCount, controllerUri);

        startTime = System.currentTimeMillis();
        for (int i = 0; i < createTransactionCallCount; i++) {
            try {
                ControllerImpl controller4 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new CreateTransaction(controller4, i);
                createTransactionExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
        }
        createTransactionExecutor.shutdown();
        try {
            createTransactionExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of create transaction executor {}", e);
        }

        CompletableFuture<Void> createTransAll = FutureHelpers.allOf(createTransactionList);
        createTransAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        log.debug("Time taken for {} create transaction calls = {} milliseconds", createTransactionCallCount, timeTaken);

        createTransactionList.forEach(createTransCompletableFuture -> {
            try {
                log.debug("Status of each create transaction call {}", createTransCompletableFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on create transaction status  {}", e);
            }
        });

    }

    private static void commitTransaction() {

        ExecutorService commitTransactionExecutor = Executors.newFixedThreadPool(commitTransactionCallCount);

        log.debug("\nCalling commit transaction {} times.The controller endpoint is {}", commitTransactionCallCount, controllerUri);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < commitTransactionCallCount; i++) {
            try {
                ControllerImpl controller4 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new CommitTransaction(controller4, i);
                commitTransactionExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
        }
        commitTransactionExecutor.shutdown();
        try {
            commitTransactionExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of commit transaction executor {}", e);
        }

        CompletableFuture<Void> commitTransAll = FutureHelpers.allOf(commitTransactionList);
        commitTransAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        log.debug("Time taken for {} commit transaction calls = {} milliseconds", commitTransactionCallCount, timeTaken);
        commitTransactionList.forEach(commitTransCompletableFuture -> {
            try {
                log.debug("Status of each commit transaction call {}", commitTransCompletableFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on commit transaction status {}", e);
            }
        });

    }

    private static void dropTransaction() {

        ExecutorService dropTransactionExecutor = Executors.newFixedThreadPool(dropTransactionCallCount);

        log.debug("\nCalling Drop transaction {} times.The controller endpoint is {}", dropTransactionCallCount, controllerUri);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < dropTransactionCallCount; i++) {
            try {
                ControllerImpl controller4 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new DropTransaction(controller4, i);
                dropTransactionExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
        }
        dropTransactionExecutor.shutdown();
        try {
            dropTransactionExecutor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Error in await termination of drop transaction executor {}", e);
        }

        CompletableFuture<Void> dropTransAll = FutureHelpers.allOf(dropTransactionList);
        dropTransAll.join();
        endTime = System.currentTimeMillis();
        timeTaken = endTime - startTime;
        log.debug("Time taken for {} drop transaction calls = {} milliseconds", dropTransactionCallCount, timeTaken);
        dropTransactionList.forEach(dropTransCompletableFuture -> {
            try {
                log.debug("Status of each drop  transaction call {}", dropTransCompletableFuture.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Error in doing a get on drop transaction status {}", e);
            }
        });

    }

    private static void parseCmdLine(String[] args) {

        Options options = new Options();
        options.addOption("controller", true, "controller URI");
        options.addOption("createstream", true, "number of create stream calls");
        options.addOption("alterstream", true, "number of alter stream calls");
        options.addOption("sealstream", true, "number of seal stream calls");
        options.addOption("scalestream", true, "number of scale stream calls");
        options.addOption("getpositions", true, "number of getposition calls");
        options.addOption("getcurrentsegments", true, "number of getcurrentsegment calls");
        options.addOption("createtransaction", true, "number of createtransaction calls");
        options.addOption("committransaction", true, "number of committransaction calls");
        options.addOption("droptransaction", true, "number of droptransaction calls");
        options.addOption("help", false, "Help message");

        CommandLineParser parser = new BasicParser();

        try {
            CommandLine commandline = parser.parse(options, args);
            if (commandline.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("controller tests", options);
                System.exit(0);
            } else {
                if (commandline.hasOption("controller")) {
                    controllerUri = commandline.getOptionValue("controller");
                }
                if (commandline.hasOption("createstream")) {
                    createStreamCallCount = Integer.parseInt(commandline.getOptionValue("createstream"));
                    createStream();
                }
                if (commandline.hasOption("alterstream")) {
                    alterStreamCallCount = Integer.parseInt(commandline.getOptionValue("alterstream"));
                    alterStream();
                }
                if (commandline.hasOption("sealstream")) {
                    sealStreamCallCount = Integer.parseInt(commandline.getOptionValue("sealstream"));
                    sealStream();
                }
                if (commandline.hasOption("scalestream")) {
                    scaleStreamCallCount = Integer.parseInt(commandline.getOptionValue("scalestream"));
                    scaleStream();
                }
                if (commandline.hasOption("getpositions")) {
                    getPositionsCallCount = Integer.parseInt(commandline.getOptionValue("getpositions"));
                    getPositions();
                }
                if (commandline.hasOption("getcurrentsegments")) {
                    getCurrentSegmentsCallCount = Integer.parseInt(commandline.getOptionValue("getcurrentsegments"));
                    getCurrentSegments();
                }
                if (commandline.hasOption("createtransaction")) {
                    createTransactionCallCount = Integer.parseInt(commandline.getOptionValue("createtransaction"));
                    createTransaction();
                }
                if (commandline.hasOption("committransaction")) {
                    commitTransactionCallCount = Integer.parseInt(commandline.getOptionValue("committransaction"));
                    commitTransaction();
                }
                if (commandline.hasOption("droptransaction")) {
                    dropTransactionCallCount = Integer.parseInt(commandline.getOptionValue("droptransaction"));
                    dropTransaction();
                }
            }
        } catch (Exception nfe) {
            log.error("Invalid arguments. Starting with default values {}", nfe);
            System.exit(0);
        }
    }

    private static class CreateStream implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String scope = "scopeCreate";
        private String streamName = "streamCreate";

        CreateStream(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
        }

        public void run() {

            scope = scope + i;
            streamName = streamName + i;
            StreamConfiguration config = new StreamConfigurationImpl(scope, streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));

            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);
            createStatusList.add(createStreamStatus);
        }
    }

    private static class AlterStream implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String scope = "scopeAlter";
        private String streamName = "streamAlter";

        AlterStream(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
        }

        public void run() {

            scope = scope + i;
            streamName = streamName + i;
            StreamConfiguration config = new StreamConfigurationImpl(scope, streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));

            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);
            ControllerImpl controllerAlter = null;
            try {
                controllerAlter = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                config = new StreamConfigurationImpl(scope, streamName,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2 + i + 1, 2));

                CompletableFuture<UpdateStreamStatus> updateStatus = controllerAlter.alterStream(config);
                alterStatusList.add(updateStatus);
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }

        }
    }

    private static class SealStream implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String scope = "scopeSeal";
        private String streamName = "streamSeal";

        SealStream(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
        }

        public void run() {

            scope = scope + i;
            streamName = streamName + i;
            StreamConfiguration config = new StreamConfigurationImpl(scope, streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));

            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);
            ControllerImpl controllerSeal = null;
            try {
                controllerSeal = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            CompletableFuture<UpdateStreamStatus> sealStreamStatus = controllerSeal.sealStream(scope, streamName);
            sealStatusList.add(sealStreamStatus);
        }
    }

    private static class ScaleStream implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String streamName = "streamScale";
        private String scope = "scopeScale";

        ScaleStream(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
        }


        // scale stream: split one segment into two
        public void run() {

            scope = scope + i;
            streamName = streamName + i;

            StreamConfiguration config = new StreamConfigurationImpl(scope, streamName,
                           new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0L, 0, 1));

            Stream stream = new StreamImpl(scope, streamName, config);
            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);
            Map<Double, Double> map = new HashMap<>();
            map.put(0.0, 0.5);
            map.put(0.5, 1.0);
            ControllerImpl controllerScale = null;
            try {
                controllerScale = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            CompletableFuture<ScaleResponse> scaleResponse = controllerScale.scaleStream(stream, Collections.singletonList(0), map);
            scaleStatusList.add(scaleResponse);
        }
    }

    private static class GetPositions implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String streamName = "streamGetPos";
        private String scope = "scopeGetPos";

        private final int count = 10;

        GetPositions(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
        }

        public void run() {

            scope = scope + i;
            streamName = streamName + i;

            StreamConfiguration config = new StreamConfigurationImpl(scope, streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 2L, 2, 2));

            Stream stream = new StreamImpl(scope, streamName, config);
            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);
            ControllerImpl controllerGetPos = null;
            try {
                controllerGetPos = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            CompletableFuture<List<PositionInternal>> getPositions = controllerGetPos.getPositions(stream, System.currentTimeMillis(), count);
            getPositionsList.add(getPositions);

        }
    }

    private static class GetCurrentSegments implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String scope = "scopeGetCurSeg";
        private String streamName = "streamGetCurSeg";

        GetCurrentSegments(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
        }

        public void run() {

            scope = scope + i;
            streamName = streamName + i;

            StreamConfiguration config = new StreamConfigurationImpl(scope, streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 2L, 2, 2));

            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);
            ControllerImpl controllerGetCurSeg = null;
            try {
                controllerGetCurSeg = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            CompletableFuture<StreamSegments> getActiveSegments = controllerGetCurSeg.getCurrentSegments(scope, streamName);
            getCurrentSegmentsList.add(getActiveSegments);
        }
    }

    private static class CreateTransaction implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String streamName = "streamCreateTxn";
        private String scope = "scopeCreateTxn";

        CreateTransaction(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;

        }

        public void run() {

            scope = scope + i;
            streamName = streamName + i;

            StreamConfiguration config = new StreamConfigurationImpl(scope, streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 2L, 2, 1));

            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);

            Stream stream = new StreamImpl(scope, streamName, config);
            //create transaction
            ControllerImpl controllerCreateTxn = null;
            try {
                controllerCreateTxn = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            CompletableFuture<UUID> txIdFuture = controllerCreateTxn.createTransaction(stream, 60000);
            createTransactionList.add(txIdFuture);
        }
    }

    private static class CommitTransaction implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String streamName = "streamCommitTxn";
        private String scope = "scopeCommitTxn";

        CommitTransaction(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
        }

        public void run() {

            scope = scope + i;
            streamName = streamName + i;

            StreamConfiguration config =  new StreamConfigurationImpl(scope, streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0L, 0, 1));

            Stream stream = new StreamImpl(scope, streamName, config);

            log.debug("Creating stream {},{} for commit transaction", scope, streamName);

            CompletableFuture<CreateStreamStatus> createStatus = controller.createStream(config);

            //create transaction
            ControllerImpl controllercommitTxn1 = null;
            try {
                controllercommitTxn1 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            UUID txId = FutureHelpers.getAndHandleExceptions(controllercommitTxn1.createTransaction(stream, 60000), RuntimeException::new);
            //commit transaction
            ControllerImpl controllercommitTxn2 = null;
            try {
                controllercommitTxn2 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            CompletableFuture<TxnStatus> commitTransaction = controllercommitTxn2.commitTransaction(stream, txId);
            commitTransactionList.add(commitTransaction);
        }
    }

    private static class DropTransaction implements Runnable {

        private final ControllerImpl controller;
        private final int i;
        private String streamName = "streamDropTxn";
        private String scope = "scopeDropTxn";

        DropTransaction(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
        }

        public void run() {

            scope = scope + i;
            streamName = streamName + i;

            StreamConfiguration config = new StreamConfigurationImpl(scope, streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0L, 0, 1));

            Stream stream = new StreamImpl(scope, streamName, config);

            log.debug("Creating stream {},{} for drop transaction", scope, streamName);

            CompletableFuture<CreateStreamStatus> createStatus = controller.createStream(config);

            //create transaction
            ControllerImpl controllerDropTxn1 = null;
            try {
                controllerDropTxn1 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            UUID txId = FutureHelpers.getAndHandleExceptions(controllerDropTxn1.createTransaction(stream, 60000), RuntimeException::new);
            //drop transaction
            ControllerImpl controllerDropTxn2 = null;
            try {
                controllerDropTxn2 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
            } catch (URISyntaxException uri) {
                log.error("Invalid controller uri {}", uri);
                System.exit(0);
            }
            CompletableFuture<TxnStatus> dropTransaction = controllerDropTxn2.dropTransaction(stream, txId);
            dropTransactionList.add(dropTransaction);

        }
    }

}
