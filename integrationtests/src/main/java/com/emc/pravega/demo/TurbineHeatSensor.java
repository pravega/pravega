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

import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamManager;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamManagerImpl;
import lombok.Cleanup;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static java.lang.System.*;

/**
 * Created by bayar on 11/3/2016.
 * Sample app will simulate sensors that measure temperatures of Wind Turbines Gearbox
 * Data format is in comma separated format as following TimeStamp, Sensor Id, Location, TempValue }
 *
 */
public class TurbineHeatSensor {


    private static Stream stream;
    private static Producer<String> producer;
    private static Stats stats;

    public static void main(String[] args) throws Exception {

        // Place names where wind farms are located
        String[] locations = {"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming", "Montgomery", "Juneau", "Phoenix", "Little Rock", "Sacramento", "Denver", "Hartford", "Dover", "Tallahassee", "Atlanta", "Honolulu", "Boise", "Springfield", "Indianapolis", "Des Moines", "Topeka", "Frankfort", "Baton Rouge", "Augusta", "Annapolis", "Boston", "Lansing", "St. Paul", "Jackson", "Jefferson City", "Helena", "Lincoln", "Carson City", "Concord", "Trenton", "Santa Fe", "Albany", "Raleigh", "Bismarck", "Columbus", "Oklahoma City", "Salem", "Harrisburg", "Providence", "Columbia", "Pierre", "Nashville", "Austin", "Salt Lake City", "Montpelier", "Richmond", "Olympia", "Charleston", "Madison", "Cheyenne"};

        // How many producers should we run concurrently
        int producerCount = 20;
        // How many events each producer has to produce per seconds
        int eventsPerSec = 40;
        // How long it needs to run
        int runtimeSec = 10;
        // Should producers use Transaction or not
        boolean isTransaction = false;

        // Since it is command line sample producer, user inputs will be accepted from console
        if (args.length != 4 || args[0].equals("help")) {
            out.println("TurbineHeatSensor producerCount eventsPerSec runtimeSec isTransaction");
            out.println("TurbineHeatSensor 10 100 20 1");
        } else {

            try {
                // Parse the string argument into an integer value.
                producerCount = Integer.parseInt(args[0]);
                eventsPerSec = Integer.parseInt(args[1]);
                runtimeSec = Integer.parseInt(args[2]);
                isTransaction = Boolean.parseBoolean(args[3]);
            } catch (Exception nfe) {
                // The first argument isn't a valid integer.  Print
                // an error message, then exit with an error code.
                out.println("Arguments must be valid.");
            }
        }

        out.println("\nTurbineHeatSensor is running "+producerCount+" simulators each ingesting "+eventsPerSec+" temperature data per second for "+runtimeSec+" seconds " + (isTransaction ? "via transactional mode" : " via non-transactional mode"));

        // Initialize executor
        ExecutorService executor = Executors.newFixedThreadPool(producerCount);

        try {
            @Cleanup
            StreamManager streamManager = null;
            streamManager = new StreamManagerImpl(StartLocalService.SCOPE, new URI("http://10.249.250.154:9090"));

            stream = streamManager.createStream(StartLocalService.STREAM_NAME,
                    new StreamConfigurationImpl("hi", StartLocalService.STREAM_NAME,
                            new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 5, 5)));
            producer = stream.createProducer(new JavaSerializer<>(),
                    new ProducerConfig(null));

            stats = new Stats(producerCount * eventsPerSec * runtimeSec, 2);

        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        // create producerCount number of threads to simulate sensors
        for (int i = 0; i < producerCount; i++) {
            TemperatureSensors worker = new TemperatureSensors(i, locations[i % locations.length], eventsPerSec, runtimeSec, isTransaction);
            executor.execute(worker);
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {
            // wait
        }

        out.println("\nFinished all producers");
        stats.printTotal();
        exit(0);
    }

    /**
     * A Sensor simulator thread that generates dummy value as temperature measurement and ingests to specified stream
     */

    private static class TemperatureSensors implements Runnable {

        private int producerId = 0;
        private String city = "";
        private int eventsPerSec = 0;
        private int secondsToRun = 0;
        private boolean isTransaction = false;

        TemperatureSensors(int sensorId, String city, int eventsPerSec, int secondsToRun, boolean isTransaction) {
            this.producerId = sensorId;
            this.city = city;
            this.eventsPerSec = eventsPerSec;
            this.secondsToRun = secondsToRun;
            this.isTransaction = isTransaction;
        }

        @Override
        public void run() {

            Transaction<String> transaction = null;

            if (isTransaction) {
                transaction = producer.startTransaction(60000);
            }

            Future<Void> retFuture = null;
            for (int i = 0; i < secondsToRun; i++) {
                int currentEventsPerSec = 0;

                //long oneSecondTimer = currentTimeMillis() + 1000;
                while (/*currentTimeMillis() < oneSecondTimer &&*/ currentEventsPerSec < eventsPerSec) {
                    currentEventsPerSec++;

                    // wait for next event
                    try {
                        Thread.sleep(1000 / eventsPerSec);
                    } catch (InterruptedException e) {
                        // log exception
                    }

                    // Construct event payload
                    String payload = currentTimeMillis() + ", " + producerId + ", " + city + ", " + (int) (Math.random() * 200);

                    // event ingestion
                    if (isTransaction) {
                        try {
                            transaction.publish(city, payload);
                            //  transaction.flush();
                        } catch (TxFailedException e) {
                        }
                    } else {

                        retFuture = stats.runAndRecordTime( (p) -> {
                            return (CompletableFuture<Void>) producer.publish(city, payload);
                        }, payload.length());

                        //  producer.flush();
                    }
                }
            }
            producer.flush();
            try {
                retFuture.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            if (isTransaction) {
                try {
                    transaction.commit();
                } catch (TxFailedException e) {
                }
            }
        }
    }


    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
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
            this.start = currentTimeMillis();
            this.windowStart = currentTimeMillis();
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

        public void record(int iter, int latency, int bytes, long time) {
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

        /*public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }*/

        public void printWindow() {
            long ellapsed = currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            out.printf("%d records sent, %.1f records/sec (%.5f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            out.printf("%d records sent, %f records/sec (%.5f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d " +
                            "ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
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

        public CompletableFuture<Void> runAndRecordTime(Function<Void, CompletableFuture<Void>> fn, int length) {
            long now = currentTimeMillis();
            int iter = this.iteration++;
            return fn.apply(null).thenAccept( (lmn) -> {
                stats.record(iter, (int) (System.currentTimeMillis() - now), length,
                        System.currentTimeMillis());
            });

        }
    }
}