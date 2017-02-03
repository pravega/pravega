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

import com.emc.pravega.StreamManager;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamManagerImpl;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.common.util.ZipKinTracer;
import lombok.Cleanup;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;




/**
 * Sample app will simulate sensors that measure temperatures of Wind Turbines Gearbox.
 * Data format is in comma separated format as following: {TimeStamp, Sensor Id, Location, TempValue }.
 *
 */
public class TurbineHeatSensor {


    private static Stream stream;
    private static PerfStats produceStats, consumeStats;
    private static String controllerUri = "http://10.249.250.154:9090";
    private static int messageSize = 100;
    private static String streamName = StartLocalService.STREAM_NAME;
    private static ClientFactoryImpl factory = null;
    private static boolean onlyWrite = true;
    private static boolean blocking = false;
    // How many producers should we run concurrently
    private static int producerCount = 20;
    // How many events each producer has to produce per seconds
    private static int eventsPerSec = 40;
    // How long it needs to run
    private static int runtimeSec = 10;
    // Should producers use Transaction or not
    private static boolean isTransaction = false;


    public static void main(String[] args) throws Exception {

        // Place names where wind farms are located
        String[] locations = {"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
                "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
                "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi",
                "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
                "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
                "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
                "West Virginia", "Wisconsin", "Wyoming", "Montgomery", "Juneau", "Phoenix", "Little Rock",
                "Sacramento", "Denver", "Hartford", "Dover", "Tallahassee", "Atlanta", "Honolulu", "Boise",
                "Springfield", "Indianapolis", "Des Moines", "Topeka", "Frankfort", "Baton Rouge", "Augusta",
                "Annapolis", "Boston", "Lansing", "St. Paul", "Jackson", "Jefferson City", "Helena", "Lincoln",
                "Carson City", "Concord", "Trenton", "Santa Fe", "Albany", "Raleigh", "Bismarck", "Columbus",
                "Oklahoma City", "Salem", "Harrisburg", "Providence", "Columbia", "Pierre", "Nashville", "Austin",
                "Salt Lake City", "Montpelier", "Richmond", "Olympia", "Charleston", "Madison", "Cheyenne"};

        parseCmdLine(args);

        System.out.println("\nTurbineHeatSensor is running "+ producerCount + " simulators each ingesting " +
                eventsPerSec + " temperature data per second for " + runtimeSec + " seconds " +
                (isTransaction ? "via transactional mode" : " via non-transactional mode. The controller end point " +
                        "is " + controllerUri));

        // Initialize executor
        @Cleanup("shutdown")
        ExecutorService executor = Executors.newFixedThreadPool(producerCount + 10);

        try {
            @Cleanup
            StreamManager streamManager = null;
            streamManager = new StreamManagerImpl(StartLocalService.SCOPE, new URI(controllerUri));

            stream = streamManager.createStream(streamName,
                    new StreamConfigurationImpl("hi", streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 5,
                                    producerCount)));
        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(1);
        }

        produceStats = new PerfStats(producerCount * eventsPerSec * runtimeSec, 200);

        if ( !onlyWrite ) {
            consumeStats = new PerfStats(producerCount * eventsPerSec * runtimeSec, 2);
            SensorReader reader = new SensorReader(producerCount * eventsPerSec * runtimeSec);
            executor.execute(reader);
        }
        /* Create producerCount number of threads to simulate sensors. */
        for (int i = 0; i < producerCount; i++) {
            factory = new ClientFactoryImpl("hi", new URI(controllerUri));

            TemperatureSensors worker;
            if ( isTransaction ) {
                worker = new TransactionTemperatureSensors(i, locations[i % locations.length], eventsPerSec, runtimeSec,
                                isTransaction, factory);
            } else {
                worker = new TemperatureSensors(i, locations[i % locations.length], eventsPerSec, runtimeSec,
                        isTransaction, factory);
            }
            executor.execute(worker);

        }

        executor.shutdown();
        // Wait until all threads are finished.
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("\nFinished all producers");
        produceStats.printAll();
        produceStats.printTotal();
        if ( !onlyWrite ) {
            consumeStats.printTotal();
        }
        ZipKinTracer.getTracer().close();
        System.exit(0);
    }

    private static void parseCmdLine(String[] args) {
        // create Options object
        Options options = new Options();

        options.addOption("controller", true, "controller URI");
        options.addOption("producers", true, "number of producers");
        options.addOption("eventspersec", true, "number events per sec");
        options.addOption("runtime", true, "number of seconds the code runs");
        options.addOption("transaction", true, "Producers use transactions or not");
        options.addOption("size", true, "Size of each message");
        options.addOption("stream", true, "Stream name");
        options.addOption("writeonly", true, "Just produce vs read after produce");
        options.addOption("blocking", true, "Block for each ack");
        options.addOption("zipkin", true, "Enable zipkin trace");

        options.addOption("help", false, "Help message");

        CommandLineParser parser = new BasicParser();
        try {

            CommandLine commandline = parser.parse(options, args);
            // Since it is command line sample producer, user inputs will be accepted from console
            if (commandline.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("integrationstests", options);
                System.exit(0);
            } else {

                if (commandline.hasOption("controller")) {
                    controllerUri = commandline.getOptionValue("controller");
                }

                if (commandline.hasOption("producers")) {
                    producerCount = Integer.parseInt(commandline.getOptionValue("producers"));
                }

                if (commandline.hasOption("eventspersec")) {
                    eventsPerSec = Integer.parseInt(commandline.getOptionValue("eventspersec"));
                }

                if (commandline.hasOption("runtime")) {
                    runtimeSec = Integer.parseInt(commandline.getOptionValue("runtime"));
                }

                if (commandline.hasOption("transaction")) {
                    isTransaction = Boolean.parseBoolean(commandline.getOptionValue("transaction"));
                }

                if (commandline.hasOption("size")) {
                    messageSize = Integer.parseInt(commandline.getOptionValue("size"));
                }

                if (commandline.hasOption("stream")) {
                    streamName = commandline.getOptionValue("stream");
                }

                if (commandline.hasOption("writeonly")) {
                    onlyWrite = Boolean.parseBoolean(commandline.getOptionValue("writeonly"));
                }
                if (commandline.hasOption("blocking")) {
                    blocking = Boolean.parseBoolean(commandline.getOptionValue("blocking"));
                }

                if (commandline.hasOption("zipkin")) {
                    boolean zipkin = Boolean.parseBoolean(commandline.getOptionValue("zipkin"));
                    ZipKinTracer.enableZipkin(zipkin);
                    if ( zipkin ) {
                        ZipKinTracer.setZipkinEndpoint("http://10.249.250.151:9411/api/v1/spans");
                    }
                }
            }
        } catch (Exception nfe) {
            System.out.println("Invalid arguments. Starting with default values");
            nfe.printStackTrace();
        }
    }

    /**
     * A Sensor simulator class that generates dummy value as temperature measurement and ingests to specified stream.
     */

    private static class TemperatureSensors implements Runnable {

        final EventStreamWriter<String> producer;
        private final int producerId;
        private final String city;
        private final int eventsPerSec;
        private final int secondsToRun;
        private final boolean isTransaction;

        TemperatureSensors(int sensorId, String city, int eventsPerSec, int secondsToRun, boolean isTransaction,
                           ClientFactoryImpl factory) {
            this.producerId = sensorId;
            this.city = city;
            this.eventsPerSec = eventsPerSec;
            this.secondsToRun = secondsToRun;
            this.isTransaction = isTransaction;
            this.producer = factory.createEventWriter(streamName, new JavaSerializer<>(),
                    new EventWriterConfig(null));

        }

        /**
         * This function will be executed in a loop and time behavior is measured.
         * @return A function which takes String key and data and returns a future object.
         */
        BiFunction<String, String, Future> sendFunction() {
            return  ( key, data) -> producer.writeEvent(key, data);
        }

        /**
         * Executes the given method over the producer with configured settings.
         * @param fn The function to execute.
         */
        void runLoop(BiFunction<String, String, Future> fn) {

            Future<Void> retFuture = null;
            for (int i = 0; i < secondsToRun; i++) {
                int currentEventsPerSec = 0;

                long loopStartTime = System.currentTimeMillis();
                while ( currentEventsPerSec < eventsPerSec) {
                    currentEventsPerSec++;

                    // Construct event payload
                    String val = System.currentTimeMillis() + ", " + producerId + ", " + city + ", " + (int) (Math.random() * 200);
                    String payload = String.format("%-" + messageSize + "s", val);
                    // event ingestion
                    long now = System.currentTimeMillis();
                    retFuture = produceStats.runAndRecordTime(() -> {
                                    return (CompletableFuture<Void>) fn.apply(Integer.toString(producerId),
                                            payload);
                                },
                                now,
                                payload.length());
                        //If it is a blocking call, wait for the ack
                        if ( blocking ) {
                            try {
                                retFuture.get();
                            } catch (InterruptedException  | ExecutionException e) {
                                e.printStackTrace();
                            }
                        }

                }
                long timeSpent = System.currentTimeMillis() - loopStartTime;
                // wait for next event
                try {
                    //There is no need for sleep for blocking calls.
                    if ( !blocking ) {
                        if ( timeSpent < 1000) {
                            Thread.sleep((1000 - timeSpent) / 1000 );
                        }
                    }
                } catch (InterruptedException e) {
                    // log exception
                    System.exit(1);
                }
            }
            producer.flush();
            //producer.close();
            try {
                //Wait for the last packet to get acked
                retFuture.get();
            } catch (InterruptedException | ExecutionException e ) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            runLoop(sendFunction());
        }
    }


    private static class TransactionTemperatureSensors extends TemperatureSensors {

        private final Transaction<String> transaction;

        TransactionTemperatureSensors(int sensorId, String city, int eventsPerSec, int secondsToRun, boolean
                isTransaction, ClientFactoryImpl factory) {
            super(sensorId, city, eventsPerSec, secondsToRun, isTransaction, factory);
            transaction = producer.beginTxn(60000);
        }

        BiFunction<String, String, Future> sendFunction() {
            return  ( key, data) -> {
                try {
                    transaction.writeEvent(key, data);
                } catch (TxnFailedException e) {
                    System.out.println("Publish to transaction failed");
                    e.printStackTrace();
                }
                return CompletableFuture.completedFuture(null);
            };
        }
    }

    /**
     * A Sensor reader class that reads the temperative data
     */
    private static class SensorReader implements Runnable {
        private int totalEvents;

        public SensorReader(int totalEvents) {
            this.totalEvents = totalEvents;
        }

        @Override
        public void run() {
            @Cleanup
            EventStreamReader<String> reader = factory.createReader(streamName,
                    new JavaSerializer<>(), new ReaderConfig(), null);

            do {
                final EventRead<String> result = reader.readNextEvent(0);
                produceStats.runAndRecordTime(() -> {
                    return CompletableFuture.completedFuture(null);
                }, Long.parseLong(result.getEvent()), 100);
            } while ( totalEvents-- > 0 );
        }
    }


}
