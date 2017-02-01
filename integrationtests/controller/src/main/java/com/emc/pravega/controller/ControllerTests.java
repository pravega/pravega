/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the                                       +
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
package com.emc.pravega.controller.stresstest;


import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Slf4j
public class ControllerTests {

    private static String controllerUri = "http://127.0.0.1:9090";
    private static int createStreamCallCount = 2;
    private static int alterStreamCallCount = 2;
    private static int sealStreamCallCount = 2;
    private static long starttime;
    private static long endtime;
    private static long timetaken;
    private static ArrayList<CompletableFuture<CreateStreamStatus>> createStatusList = new ArrayList<>();
    private static ArrayList<CompletableFuture<UpdateStreamStatus>> alterStatusList = new ArrayList<>();
    private static ArrayList<CompletableFuture<UpdateStreamStatus>> sealStatusList = new ArrayList<>();


    public static void main(String[] args) throws Exception {
        parseCmdLine(args);
        createStream();
        alterStream();
        sealStream();
        System.exit(0);
    }

    private static void createStream() {
        ExecutorService createExecutor = Executors.newFixedThreadPool(createStreamCallCount);
        System.out.println("\n Calling create stream  " + createStreamCallCount + " times. " + " The controller endpoint is" + controllerUri);
        starttime = System.currentTimeMillis();
        for (int i = 0; i < createStreamCallCount; i++) {
            try {
                ControllerImpl controller = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new CreateStream(controller, i);
                createExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                System.out.println(uri);
            }
        }
        createExecutor.shutdown();
        try {
            createExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        endtime = System.currentTimeMillis();
        timetaken = endtime - starttime;
        System.out.println("time taken for" + createStreamCallCount + "create stream calls =" + timetaken);
        CompletableFuture<Void> createAll = FutureHelpers.allOf(createStatusList);
        createAll.join();
        createStatusList.forEach(createStreamStatusCompletableFuture -> {
            try {
                System.out.println("get status of each create stream call" + createStreamStatusCompletableFuture.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private static void alterStream() {

        ExecutorService alterExecutor = Executors.newFixedThreadPool(alterStreamCallCount);
        String scope = "scope";
        String streamName = "streamName";
        StreamConfiguration config =
                new StreamConfigurationImpl(scope,
                        streamName,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));
        ControllerImpl controller1 = null;
        try {
            controller1 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        CompletableFuture<CreateStreamStatus> createStream = controller1.createStream(config);
        try {
            System.out.println("create stream status" + createStream.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("\n calling alter stream  " + alterStreamCallCount + " times " + " the controller endpoint is" + controllerUri);
        starttime = System.currentTimeMillis();
        for (int i = 0; i < alterStreamCallCount; i++) {
            try {
                ControllerImpl controller2 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable1 = new AlterStream(controller2, scope, streamName, i);
                alterExecutor.execute(runnable1);
            } catch (URISyntaxException uri) {
                System.out.println(uri);
            }
        }

        alterExecutor.shutdown();
        try {
            alterExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        endtime = System.currentTimeMillis();
        timetaken = endtime - starttime;
        System.out.println("time taken for" + alterStreamCallCount + "alter stream calls =" + timetaken);
        CompletableFuture<Void> alterAll = FutureHelpers.allOf(alterStatusList);
        alterAll.join();
        try {
            System.out.println("contents" + alterAll.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("is empty" + alterStatusList.isEmpty());
        alterStatusList.forEach(alterStreamStatusCompletableFuture -> {
            try {
                System.out.println("get status of each alter stream call" + alterStreamStatusCompletableFuture.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private static void sealStream() {

        ExecutorService sealExecutor = Executors.newFixedThreadPool(sealStreamCallCount);
        String scope = "sealscope";
        String streamName = "sealstreamName";
        StreamConfiguration config =
                new StreamConfigurationImpl(scope,
                        streamName,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));
        ControllerImpl controller3 = null;
        try {
            controller3 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        CompletableFuture<CreateStreamStatus> createStream = controller3.createStream(config);
        try {
            System.out.println("create stream status" + createStream.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("\n calling seal stream  " + sealStreamCallCount + " times " + " the controller endpoint is" + controllerUri);
        starttime = System.currentTimeMillis();
        for (int i = 0; i < sealStreamCallCount; i++) {
            try {
                ControllerImpl controller4 = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new SealStream(controller4, scope, streamName);
                sealExecutor.execute(runnable);
            } catch (URISyntaxException uri) {
                System.out.println(uri);
            }
        }

        sealExecutor.shutdown();
        try {
            sealExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        endtime = System.currentTimeMillis();
        timetaken = endtime - starttime;
        System.out.println("time taken for" + sealStreamCallCount + "alter stream calls =" + timetaken);
        CompletableFuture<Void> sealAll = FutureHelpers.allOf(sealStatusList);
        sealAll.join();
        try {
            System.out.println("contents" + sealAll.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("is empty" + sealStatusList.isEmpty());
        sealStatusList.forEach(sealStreamStatusCompletableFuture -> {
            try {
                System.out.println("get status of each alter stream call" + sealStreamStatusCompletableFuture.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private static void parseCmdLine(String[] args) {

        Options options = new Options();
        options.addOption("controller", true, "controller URI");
        options.addOption("createstream", true, "number of create stream calls");
        options.addOption("alterstream", true, "number of alter stream calls");
        options.addOption("sealstream", true, "number of seal stream calls");
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
                }
                if (commandline.hasOption("alterstream")) {
                    alterStreamCallCount = Integer.parseInt(commandline.getOptionValue("alterstream"));
                }
                if (commandline.hasOption("sealstream")) {
                    alterStreamCallCount = Integer.parseInt(commandline.getOptionValue("sealstream"));
                }
            }
        } catch (Exception nfe) {
            System.out.println("Invalid arguments. Starting with default values");
            nfe.printStackTrace();
            System.exit(0);
        }
    }

    private static class CreateStream implements Runnable {

        private ControllerImpl controller;
        private int i;
        private String scope;
        private String streamName;

        CreateStream(ControllerImpl controller, int i) {
            this.controller = controller;
            this.i = i;
            this.scope = "scope" + i;
            this.streamName = "streamName" + i;
        }


        public void run() {
            StreamConfiguration config =
                    new StreamConfigurationImpl(scope,
                            streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));

            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);
            createStatusList.add(createStreamStatus);

        }
    }

    private static class AlterStream implements Runnable {

        private ControllerImpl controller;
        private int i;
        private String scope;
        private String streamName;

        AlterStream(ControllerImpl controller, String scope, String streamName, int i) {
            this.controller = controller;
            this.i = i;
            this.scope = scope;
            this.streamName = streamName;
        }


        public void run() {
            final StreamConfiguration config =
                    new StreamConfigurationImpl(scope,
                            streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2 + i + 1, 2));

            CompletableFuture<UpdateStreamStatus> alterStreamStatus = controller.alterStream(config);
            alterStatusList.add(alterStreamStatus);

        }
    }

    private static class SealStream implements Runnable {

        private ControllerImpl controller;
        private int i;
        private String scope;
        private String streamName;

        SealStream(ControllerImpl controller, String scope, String streamName) {
            this.controller = controller;
            this.scope = scope;
            this.streamName = streamName;
        }

        public void run() {
            CompletableFuture<UpdateStreamStatus> sealStreamStatus = controller.sealStream(scope, streamName);
            alterStatusList.add(sealStreamStatus);

        }
    }
}





