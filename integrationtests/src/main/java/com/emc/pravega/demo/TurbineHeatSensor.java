package com.emc.pravega.demo;

import com.emc.pravega.stream.*;
import lombok.Cleanup;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockStreamManager;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by bayar on 11/3/2016.
 * Sample app will simulate sensors that measure temperatures of Wind Turbines Gearbox
 * Data format is in CSV format as following Sensor Id, Turbine Id, Location, Timestamp, TempValue }
 *
 */
public class TurbineHeatSensor {


    public static void main(String[] args) throws Exception {

        // Place names where wind farms are located
        String[] LOCATIONS = {"Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming",
        "Montgomery","Juneau","Phoenix","Little Rock","Sacramento","Denver","Hartford","Dover","Tallahassee","Atlanta","Honolulu","Boise","Springfield","Indianapolis","Des Moines","Topeka","Frankfort","Baton Rouge","Augusta","Annapolis","Boston","Lansing","St. Paul","Jackson","Jefferson City","Helena","Lincoln","Carson City","Concord","Trenton","Santa Fe","Albany","Raleigh","Bismarck","Columbus","Oklahoma City","Salem","Harrisburg","Providence","Columbia","Pierre","Nashville","Austin","Salt Lake City","Montpelier","Richmond","Olympia","Charleston","Madison","Cheyenne"};

        // default parameters
        int PRODUCER_COUNT = 20;
        int EVENTS_PER_SEC = 1000;
        int RUNTIME_SEC = 20;
        boolean IS_TRANSACTION = false;

        // Since it is command line sample producer, user inputs will be accepted from console
         if(args.length != 4 || args[0] == "help"){
             System.out.println("TurbineHeatSensor PRODUCER_COUNT EVENTS_PER_SEC RUNTIME_SEC IS_TRANSACTION");
             System.out.println("TurbineHeatSensor 10 100 20 1");
         }else {

             try {
                 // Parse the string argument into an integer value.
                 PRODUCER_COUNT = Integer.parseInt(args[0]);
                 EVENTS_PER_SEC = Integer.parseInt(args[1]);
                 RUNTIME_SEC = Integer.parseInt(args[2]);
                 IS_TRANSACTION = Boolean.parseBoolean(args[3]);
             }
             catch (Exception nfe) {
                 // The first argument isn't a valid integer.  Print
                 // an error message, then exit with an error code.
                 System.out.println("Arguments must be valid.");

             }
         }

        System.out.println("\nTurbineHeatSensor is running "+PRODUCER_COUNT+" simulators each ingesting "+EVENTS_PER_SEC+" temperature data per second for "+RUNTIME_SEC+" seconds " + ((IS_TRANSACTION)?"via transactional mode":" via non-transactional mode"));

        // Initialize executor
        ExecutorService executor = Executors.newFixedThreadPool(PRODUCER_COUNT);

        // create PRODUCER_COUNT number of threads to simulate sensors
        for (int i = 0; i < PRODUCER_COUNT; i++) {
            TemperatureSensors worker = new TemperatureSensors(i, LOCATIONS[i%LOCATIONS.length], EVENTS_PER_SEC, RUNTIME_SEC, IS_TRANSACTION);
            executor.execute(worker);
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {
        }

        System.out.println("\nFinished all producers");
        System.exit(0);
    }

    /**
     * A Sensor simulator thread that generates dummy value as temperature measurement and ingests to specified stream
     */

    public static class TemperatureSensors implements Runnable {

        private int producer_id = 0;
        private String city = "";
        private int events_per_sec = 0;
        private int seconds_to_run = 0;
        private boolean is_transaction = false;

        TemperatureSensors(int sensor_id, String city, int events_per_sec, int seconds_to_run, boolean is_transaction) {
            this.producer_id = sensor_id;
            this.city = city;
            this.events_per_sec = events_per_sec;
            this.seconds_to_run = seconds_to_run;
            this.is_transaction = is_transaction;
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

            if(is_transaction){
                transaction = producer.startTransaction(60000);
            }

            for(int i=0; i < seconds_to_run; i++) {
                int current_events_per_sec = 0;

                long one_second_timer = System.currentTimeMillis() + 1000;
                while(System.currentTimeMillis() < one_second_timer && current_events_per_sec <= events_per_sec) {
                    current_events_per_sec++;

                    // wait for next event
                    try {
                        Thread.sleep(1000/events_per_sec);
                    }catch(InterruptedException e){}

                    // Construct event payload
                    String payload = System.currentTimeMillis() + "," + producer_id + "," + city + "," +  (int) (Math.random() * 200);

                    // event ingestion
                    if (is_transaction) {
                        try {
                            transaction.publish(city, payload);
                            transaction.flush();   
                        } catch (TxFailedException e) {}
                    } else {
                        producer.publish(city, payload);
                        producer.flush();
                    }
                }
            }

            if(is_transaction){
                try{
                    transaction.commit();
                }catch(TxFailedException e){}
            }

        }
    }
}
