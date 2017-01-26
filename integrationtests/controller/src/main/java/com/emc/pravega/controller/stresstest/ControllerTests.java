/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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
    private static int createStreamCallCount = 50;
    private static long starttime;
    private static long endtime;
    private static long timetaken;
    private static ArrayList<CompletableFuture<CreateStreamStatus>> createStatusList = new ArrayList<CompletableFuture<CreateStreamStatus>>();

    public static void main(String[] args) throws Exception {

        parseCmdLine(args);
        ExecutorService executor = Executors.newFixedThreadPool(createStreamCallCount);
        System.out.println("\n calling create stream  " + createStreamCallCount + " times " + " the controller endpoint is" + controllerUri);
        starttime = System.currentTimeMillis();
        for (int i = 0; i < createStreamCallCount; i++) {
            try {
                ControllerImpl controller = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
                Runnable runnable = new CreateStream(controller, i);
                executor.execute(runnable);
            } catch (URISyntaxException uri) {
                System.out.println(uri);
            }
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        endtime = System.currentTimeMillis();
        timetaken = endtime - starttime;
        System.out.println("time taken for" + createStreamCallCount + "number of create stream calls =" + timetaken);
        CompletableFuture<Void> all = FutureHelpers.allOf(createStatusList);
        all.join();
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

    private static void parseCmdLine(String[] args) {

        Options options = new Options();
        options.addOption("controller", true, "controller URI");
        options.addOption("createstream", true, "number of create stream calls");
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
        private long starttime;
        private long endtime;
        private long timetaken;
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
            starttime = System.currentTimeMillis();
            System.out.println("create stream config" + i + config);
            CompletableFuture<CreateStreamStatus> createStreamStatus = controller.createStream(config);
            createStatusList.add(createStreamStatus);

        }
    }

}





