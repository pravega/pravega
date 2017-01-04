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
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamManagerImpl;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import lombok.Cleanup;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import javax.annotation.concurrent.GuardedBy;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;




/**
 * Sample app will simulate sensors that measure temperatures of Wind Turbines Gearbox.
 * Data format is in comma separated format as following: {TimeStamp, Sensor Id, Location, TempValue }.
 *
 */
public class TurbineHeatSensor {


    private static final int NUM_SEGMENTS = 5;
    private static Stream stream;
    private static Stats stats;
    private static String controllerUri = "http://10.249.250.154:9090";
    private static int messageSize = 100;
    private static String streamName = StartLocalService.STREAM_NAME;
    private static ClientFactoryImpl factory;

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

        // How many producers should we run concurrently
        int producerCount = 20;
        // How many events each producer has to produce per seconds
        int eventsPerSec = 40;
        // How long it needs to run
        int runtimeSec = 10;
        // Should producers use Transaction or not
        boolean isTransaction = false;

        // create Options object
        Options options = new Options();

        options.addOption("controller", true, "controller URI");
        options.addOption("producers", true, "number of producers");
        options.addOption("eventspersec", true, "number events per sec");
        options.addOption("runtime", true, "number of seconds the code runs");
        options.addOption("transaction", true, "Producers use transactions or not");
        options.addOption("size", true, "Size of each message");
        options.addOption("stream", true, "Stream name");

        options.addOption("help", false, "Help message");

        CommandLineParser parser = new BasicParser();

        CommandLine commandline = parser.parse(options, args);
        // Since it is command line sample producer, user inputs will be accepted from console
        if (commandline.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("integrationstests", options);
            System.exit(0);
        } else {

            try {

                if (commandline.hasOption("controller")) {
                    controllerUri = commandline.getOptionValue("controller");
                }

                if (commandline.hasOption("producers")) {
                    producerCount = Integer.parseInt(commandline.getOptionValue("producers"));
                }

                if (commandline.hasOption("eventspersec")) {
                    eventsPerSec = Integer.parseInt(commandline.getOptionValue("eventspersec"));
                }

                if (commandline.hasOption("runtimesec")) {
                    producerCount = Integer.parseInt(commandline.getOptionValue("runtimesec"));
                }

                if (commandline.hasOption("transaction")) {
                    producerCount = Integer.parseInt(commandline.getOptionValue("transaction"));
                }

                if (commandline.hasOption("size")) {
                    messageSize = Integer.parseInt(commandline.getOptionValue("size"));
                }

                if (commandline.hasOption("stream")) {
                    streamName = commandline.getOptionValue("stream");
                }

            } catch (Exception nfe) {
                System.out.println("Invalid arguments. Starting with default values");
                nfe.printStackTrace();
            }
        }

        System.out.println("\nTurbineHeatSensor is running "+ producerCount + " simulators each ingesting " +
                eventsPerSec + " temperature data per second for " + runtimeSec + " seconds " +
                (isTransaction ? "via transactional mode" : " via non-transactional mode. The controller end point " +
                        "is " + controllerUri));

        // Initialize executor
        ExecutorService executor = Executors.newFixedThreadPool(20);

        try {
            @Cleanup
            StreamManager streamManager = null;
            streamManager = new StreamManagerImpl(StartLocalService.SCOPE, new URI(controllerUri));

            stream = streamManager.createStream(streamName,
                    new StreamConfigurationImpl("hi", streamName,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 5,
                                    NUM_SEGMENTS)));
            factory = new ClientFactoryImpl("hi", new URI(controllerUri));

            stats = new Stats(producerCount * eventsPerSec * runtimeSec, 2);

        } catch (URISyntaxException e) {
            e.printStackTrace();
            System.exit(1);
        }
        /* Create producerCount number of threads to simulate sensors. */
        for (int i = 0; i < producerCount; i++) {
            EventStreamWriter<String> producer = factory.createEventWriter(streamName, new JavaSerializer<>(),
                    new EventWriterConfig(null));
            TemperatureSensors worker = new TemperatureSensors(i, locations[i % locations.length], eventsPerSec,
                    runtimeSec, isTransaction, producer);
            executor.execute(worker);
        }
        executor.shutdown();
        // Wait until all threads are finished.
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println("\nFinished all producers");
        stats.printTotal();
        System.exit(0);
    }

    /**
     * A Sensor simulator class that generates dummy value as temperature measurement and ingests to specified stream.
     */

    private static class TemperatureSensors implements Runnable {

        private final EventStreamWriter<String> producer;
        private int producerId = 0;
        private String city = "";
        private int eventsPerSec = 0;
        private int secondsToRun = 0;
        private boolean isTransaction = false;

        TemperatureSensors(int sensorId, String city, int eventsPerSec, int secondsToRun, boolean isTransaction,
                           EventStreamWriter<String> producer) {
            this.producerId = sensorId;
            this.city = city;
            this.eventsPerSec = eventsPerSec;
            this.secondsToRun = secondsToRun;
            this.isTransaction = isTransaction;
            this.producer = producer;
        }

        @Override
        public void run() {

            Transaction<String> transaction = null;

            if (isTransaction) {
                transaction = producer.beginTxn(60000);
            }

            Future<Void> retFuture = null;
            for (int i = 0; i < secondsToRun; i++) {
                int currentEventsPerSec = 0;

                while ( currentEventsPerSec < eventsPerSec) {
                    currentEventsPerSec++;

                    // wait for next event
                    try {
                        Thread.sleep(1000 / eventsPerSec);
                    } catch (InterruptedException e) {
                        // log exception
                    }

                    // Construct event payload
                    String val = System.currentTimeMillis() + ", " + producerId + ", " + city + ", " +
                            (int) (Math.random() * 200);
                    String payload = String.format("%-" + messageSize + "s", val);
                    // event ingestion
                    if (isTransaction) {
                        try {
                            transaction.writeEvent(city, payload);
                        } catch (TxnFailedException e) {
                            System.out.println("Publish to transaction failed");
                            e.printStackTrace();
                            break;
                        }
                    } else {
                        retFuture = stats.runAndRecordTime( () -> {
                            return (CompletableFuture<Void>) producer.writeEvent(city, payload);
                        }, payload.length());

                    }
                }
            }
            producer.flush();
            try {
                //Wait for the last packet to get acked
                retFuture.get();
            } catch (InterruptedException e ) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            if (isTransaction) {
                try {
                    transaction.commit();
                } catch (TxnFailedException e) {
                    System.out.println("Transaction commit failed");
                    e.printStackTrace();
                }
            }
        }
    }


    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        @GuardedBy("this")
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.index = 0;
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public synchronized void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.5f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.5f MB/sec), %.2f ms avg latency, %.2f ms max " +
                            "latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }

        public CompletableFuture<Void> runAndRecordTime(Supplier<CompletableFuture<Void>> fn, int length) {
            long now = System.currentTimeMillis();
            int iter = this.iteration++;
            return fn.get().thenAccept( (lmn) -> {
                stats.record(iter, (int) (System.currentTimeMillis() - now), length,
                        System.currentTimeMillis());
            });

        }
    }
}