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
package com.emc.pravega.demo;


import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

/**
 * Created by root on 1/11/17.
 */

public class ControllerTests {
    private  static ControllerImpl controller;
    private  static String controllerUri = "http://10.249.250.154:9090";
    private static int createStreamCallCount = 100;

    public static void main(String[] args)  {
        parseCmdLine(args);
        try {
            controller = new ControllerImpl(new URI(controllerUri).getHost(), new URI(controllerUri).getPort());
        } catch (URISyntaxException uri) {
            uri.printStackTrace();
            System.exit(1);
        }

        System.out.println("\n create stream called "+createStreamCallCount+ " times "+" the controller endpoint is"+controllerUri);
         new Create().start();
         System.out.println("create stream calls successful");
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
        }  catch (Exception nfe) {
            System.out.println("Invalid arguments. Starting with default values");
            nfe.printStackTrace();
        }
    }

    private static class Create extends  Thread {

        public void run() {
            final String scope1 = "scope1";
            final String streamName1 = "stream1";
            final StreamConfiguration config1 =
                    new StreamConfigurationImpl(scope1,
                            streamName1,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));
            //createStream calls
            CompletableFuture<CreateStreamStatus>[] createStatus = new CompletableFuture[createStreamCallCount];
            for (int i = 0; i <  createStreamCallCount; i++) {
                System.out.println("create stream called"+i+"th time");
                try {
                    createStatus[i] = controller.createStream(config1);
                } catch (IllegalStateException ise) {
                    System.out.println(ise);
                }
            }
            try {
                CompletableFuture.allOf(createStatus);
            }  catch (NullPointerException npe) {
                  System.out.println(npe);
            }
        }
    }

}






