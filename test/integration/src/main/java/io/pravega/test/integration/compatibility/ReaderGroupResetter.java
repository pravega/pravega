package io.pravega.test.integration.compatibility;


import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ReaderGroupResetter {

    private static final URI CONTROLLER_URI = URI.create("tcp://localhost:9090");
    private static final String SCOPE_NAME = "myScope";
    private static final String STREAM_NAME = "myStream";
    private static final String READER_GROUP_NAME = "myReaderGroup";

    public static void main(String[] args) throws Exception {
        // client configuration and create scope, stream and reader group.
//        ClientConfig clientConfig = ClientConfig.builder().controllerURI(CONTROLLER_URI).build();
//        StreamManager streamManager = StreamManager.create(clientConfig);
//        streamManager.createScope(SCOPE_NAME);
//        StreamConfiguration streamConfig = StreamConfiguration.builder().build();
//        streamManager.createStream(SCOPE_NAME, STREAM_NAME, streamConfig);
//        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE_NAME, clientConfig);
//        readerGroupManager.createReaderGroup(READER_GROUP_NAME, ReaderGroupConfig.builder().stream(Stream.of(SCOPE_NAME, STREAM_NAME)).build());
//        System.out.println("Readergroup is created");

        //  task that each virtual process will run.
        Runnable task = () -> {
            try {
                ClientConfig clientConfig = ClientConfig.builder().controllerURI(CONTROLLER_URI).build();
                @Cleanup
                StreamManager streamManager = StreamManager.create(clientConfig);
                try {
                    streamManager.createScope(SCOPE_NAME);


                }
                catch (Exception e){
                    log.error("create scope error",e);
                }
                //streamManager.createScope(SCOPE_NAME);
                StreamConfiguration streamConfig = StreamConfiguration.builder().build();
                try{

                    streamManager.createStream(SCOPE_NAME, STREAM_NAME, streamConfig);
                }
                catch (Exception e){
                    log.error("create stream error:",e);
                }
//                streamManager.createStream(SCOPE_NAME, STREAM_NAME, streamConfig);
                @Cleanup
                ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE_NAME, clientConfig);
                final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(SCOPE_NAME, STREAM_NAME)).build();
                readerGroupManager.createReaderGroup(READER_GROUP_NAME, readerGroupConfig);// ReaderGroupConfig.builder().stream(Stream.of(SCOPE_NAME, STREAM_NAME)).build());
                log.info("Readergroup is created");
                // Create a new Pravega reader with a unique thread name to isolate it from other readers.
                //System.out.println(Thread.currentThread().getName()+"started tasking");
                @Cleanup
                ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP_NAME);
                final String threadName = Thread.currentThread().getName();

                final ReaderConfig readerConfig = ReaderConfig.builder().build();

                final EventStreamReader<String> reader = EventStreamClientFactory.withScope(SCOPE_NAME, clientConfig)
                        .createReader(threadName, READER_GROUP_NAME, new JavaSerializer<>(), readerConfig);

                // Read some data to simulate additional concurrent activity on the reader group.
                final EventRead<String> event = reader.readNextEvent(1000);
                if (event.getEvent() != null) {
                    System.out.format("Thread %s: Read event %s from reader group.\n", threadName, event.getEvent());
                }

                // Invoke the resetReaderGroup call.
                //System.out.println(Thread.currentThread().getName()+" before reset");
                log.info(String.valueOf(readerGroupConfig));
                reader.close();
                readerGroup.resetReaderGroup(readerGroupConfig);
                //System.out.println(Thread.currentThread().getName()+" after reset");


                // Clean up the reader and the reader group.
                //reader.close();
                //readerGroupManager.deleteReaderGroup(READER_GROUP_NAME);
                //System.out.println(Thread.currentThread().getName()+"ending tasking");
            } catch (Exception e) {
                log.error("This didn't run successfully" ,e);//+ e.getMessage());
                //e.printStackTrace();
            }
        };

        // Spawn multiple virtual processes and run the task in parallel.
        ExecutorService executor = Executors.newFixedThreadPool(6);
        for (int i = 0; i < 100; i++) {
            executor.submit(task);
        }
        executor.shutdown();
    }
}
