package com.emc.pravega.demo;

import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.TxFailedException;
import lombok.Cleanup;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockStreamManager;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by bayar on 11/3/2016.
 * Sample app will simulate sensors that measure temperatures of Wind Turbines Gearbox
 * Data format is in comma separated format as following TimeStamp, Sensor Id, Location, TempValue }
 *
 */
public class TurbineHeatSensor {


    public static void main(String[] args) throws Exception {

        // Place names where wind farms are located
        String[] locations = {"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming", "Montgomery", "Juneau", "Phoenix", "Little Rock", "Sacramento", "Denver", "Hartford", "Dover", "Tallahassee", "Atlanta", "Honolulu", "Boise", "Springfield", "Indianapolis", "Des Moines", "Topeka", "Frankfort", "Baton Rouge", "Augusta", "Annapolis", "Boston", "Lansing", "St. Paul", "Jackson", "Jefferson City", "Helena", "Lincoln", "Carson City", "Concord", "Trenton", "Santa Fe", "Albany", "Raleigh", "Bismarck", "Columbus", "Oklahoma City", "Salem", "Harrisburg", "Providence", "Columbia", "Pierre", "Nashville", "Austin", "Salt Lake City", "Montpelier", "Richmond", "Olympia", "Charleston", "Madison", "Cheyenne"};

        // How many producers should we run concurrently
        int producerCount = 20;
        // How many events each producer has to produce per seconds
        int eventsPerSec = 1000;
        // How long it needs to run
        int runtimeSec = 20;
        // Should producers use Transaction or not
        boolean isTransaction = false;

        // Since it is command line sample producer, user inputs will be accepted from console
        if (args.length != 4 || args[0].equals("help")) {
            System.out.println("TurbineHeatSensor producerCount eventsPerSec runtimeSec isTransaction");
            System.out.println("TurbineHeatSensor 10 100 20 1");
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
                 System.out.println("Arguments must be valid.");
             }
        }

        System.out.println("\nTurbineHeatSensor is running "+producerCount+" simulators each ingesting "+eventsPerSec+" temperature data per second for "+runtimeSec+" seconds " + ((isTransaction) ? "via transactional mode" : " via non-transactional mode"));

        // Initialize executor
        ExecutorService executor = Executors.newFixedThreadPool(producerCount);

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

        System.out.println("\nFinished all producers");
        System.exit(0);
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

            @Cleanup
            MockStreamManager streamManager = new MockStreamManager(StartLocalService.SCOPE,
                    "localhost",
                    StartLocalService.PORT);
            Stream stream = streamManager.createStream(StartLocalService.STREAM_NAME, null);

            @Cleanup
            Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
            Transaction<String> transaction = null;

            if (isTransaction) {
                transaction = producer.startTransaction(60000);
            }

            for (int i = 0; i < secondsToRun; i++) {
                int currentEventsPerSec = 0;

                long oneSecondTimer = System.currentTimeMillis() + 1000;
                while (System.currentTimeMillis() < oneSecondTimer && currentEventsPerSec <= eventsPerSec) {
                    currentEventsPerSec++;

                    // wait for next event
                    try {
                        Thread.sleep(1000 / eventsPerSec);
                    } catch (InterruptedException e) {
                        // log exception
                    }

                    // Construct event payload
                    String payload = System.currentTimeMillis() + ", " + producerId + ", " + city + ", " +  (int) (Math.random() * 200);

                    // event ingestion
                    if (isTransaction) {
                        try {
                            transaction.publish(city, payload);
                            transaction.flush();
                        } catch (TxFailedException e) { }
                    } else {
                        producer.publish(city, payload);
                        producer.flush();
                    }
                }
            }

            if (isTransaction) {
                try {
                    transaction.commit();
                } catch (TxFailedException e) { }
            }

        }
    }
}
