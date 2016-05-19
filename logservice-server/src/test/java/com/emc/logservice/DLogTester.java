package com.emc.logservice;

import com.twitter.distributedlog.*;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

import java.net.URI;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by padura on 5/16/16.
 */
public class DLogTester {

    private static final int RecordCount = 10;
    private static final int RecordSize = 100;
    private final byte[] RecordData;

    public DLogTester() {
        Random rnd = new Random();
        RecordData = new byte[RecordSize];
        rnd.nextBytes(RecordData);
    }

    public void run() throws Exception {

        //        ZooKeeperClient zkc = org.apache.bookkeeper.zookeeper.ZooKeeperClient.createConnectedZooKeeperClient("andrei-desktop-ubuntu:2181", 10000);
        //        zkc.create("/ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //        zkc.create("/ledgers/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //DLMetadata.create(new BKDLConfig("andrei-desktop-ubuntu:2181", "/ledgers")).create(URI.create("distributedlog://andrei-desktop-ubuntu:2181/messaging/distributedlog"));
 //       DLMetadata.create(new BKDLConfig("andrei-desktop-ubuntu:2181", "/ledgers")).create(URI.create("distributedlog://127.0.0.1:2181/messaging/distributedlog"));

        URI uri = URI.create("distributedlog://andrei-desktop-ubuntu:2181/messaging/distributedlog");
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setImmediateFlushEnabled(true)
                .setOutputBufferSize(0)
                .setPeriodicFlushFrequencyMilliSeconds(0)
                .setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE)
                .setCreateStreamIfNotExists(true)
                .setAckQuorumSize(1)
                .setEnsembleSize(1)
                .setWriteQuorumSize(1);


        DistributedLogNamespace namespace = null;
        DistributedLogManager dlm = null;
        AsyncLogWriter writer = null;
        ConcurrentHashMap<Integer, Long> latenciesById = new ConcurrentHashMap<>();

        try {
            System.out.println("Opening namespace...");
            namespace = DistributedLogNamespaceBuilder.newBuilder()
                                                      .conf(conf)
                                                      .uri(uri)
                                                      .regionId(DistributedLogConstants.LOCAL_REGION_ID)
                                                      .clientId("console-writer")
                                                      .build();

            System.out.println("Opening log...");
            dlm = namespace.openLog("messaging-stream-1");

            System.out.println("Opening async writer...");
            writer = FutureUtils.result(dlm.openAsyncLogWriter());

            byte[] data = "hello".getBytes();
            LogRecord record = new LogRecord(System.currentTimeMillis(), data);
            Future<DLSN> writeFuture = writer.write(record);

            writeFuture.addEventListener(new FutureEventListener<DLSN>() {
                @Override
                public void onFailure(Throwable cause) {
                    System.err.println(cause);
                }

                @Override
                public void onSuccess(DLSN value) {
                    System.out.println(value);
                }
            });

            writeFuture.get();

            //            System.out.println("Writing entries...");
            //            for (int i = 0; i < RecordCount; i++) {
            //                LogRecord record = new LogRecord(i, RecordData);
            //                latenciesById.put(i, System.nanoTime());
            //                final int txId = i;
            //                System.out.println("Writing record " + i);
            //                Future<DLSN> dlsn = writer.write(record);
            //                dlsn.addEventListener(new FutureEventListener<DLSN>() {
            //                    @Override
            //                    public void onSuccess(DLSN value) {
            //                        latenciesById.put(txId, System.nanoTime() - latenciesById.get(txId));
            //                    }
            //
            //                    @Override
            //                    public void onFailure(Throwable cause) {
            //                        System.err.println(cause);
            //                    }
            //                });
            //                dlsn.get();
            //            }
        }
        finally {
            if (writer != null) {
                System.out.println("Closing writer...");
                FutureUtils.result(writer.asyncClose());
            }

            if (dlm != null) {
                System.out.println("Closing log...");
                FutureUtils.result(dlm.asyncClose());
            }

            if (namespace != null) {
                System.out.println("Closing namespace...");
                namespace.close();
            }
        }

        //        ArrayList<Long> latencies = new ArrayList<>(latenciesById.values());
        //        latencies.sort(Long::compare);
        //
        //        long sum = 0;
        //        for (int i = 0; i < latencies.size(); i++) {
        //            latencies.set(i, latencies.get(i) / 1000 / 1000);
        //            sum += latencies.get(i);
        //        }
        //
        //        System.out.println("Latencies");
        //        System.out.println("Count, Avg, Min, Max, 50%, 90%, 95%, 99%, 99.9%");
        //        System.out.println(String.format("%d, %f, %d, %d, %d, %d, %d, %d, %d",
        //                latencies.size(),
        //                sum * 1.0 / latencies.size(),
        //                latencies.get(0),
        //                latencies.get(latencies.size() - 1),
        //                latencies.get((int) (latencies.size() * 0.5)),
        //                latencies.get((int) (latencies.size() * 0.9)),
        //                latencies.get((int) (latencies.size() * 0.95)),
        //                latencies.get((int) (latencies.size() * 0.99)),
        //                latencies.get((int) (latencies.size() * 0.999))));
    }
}
