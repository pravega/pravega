package com.emc.logservice;

import com.twitter.distributedlog.*;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import com.twitter.distributedlog.namespace.DistributedLogNamespaceBuilder;
import com.twitter.distributedlog.util.FutureUtils;
import com.twitter.util.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by padura on 5/16/16.
 */
public class DLogTester {

    private static final int RecordCount = 100;
    private static final int RecordSize = 100;
    private final byte[] RecordData;

    public DLogTester() {
        Random rnd = new Random();
        RecordData = new byte[RecordSize];
        rnd.nextBytes(RecordData);
    }

    public void run() throws Exception {
        URI uri = URI.create("distributedlog://localhost:7000/messaging/distributedlog");
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
                .setImmediateFlushEnabled(true)
                .setOutputBufferSize(0)
                .setPeriodicFlushFrequencyMilliSeconds(0)
                .setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE)
                .setCreateStreamIfNotExists(true);


        DistributedLogNamespace namespace = null;
        DistributedLogManager dlm = null;
        AsyncLogWriter writer = null;
        ConcurrentHashMap<Long, Long> latenciesById = new ConcurrentHashMap<>();

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

            System.out.println("Writing entries...");
            for (int i = 0; i < RecordCount; i++) {
                final long txId = System.currentTimeMillis();
                LogRecord record = new LogRecord(txId, RecordData);
                latenciesById.put(txId, System.nanoTime());
                Future<DLSN> dlsn = writer.write(record);
                System.out.println("Writing record " + i);
                dlsn.addEventListener(new FutureEventListener<DLSN>() {
                    @Override
                    public void onSuccess(DLSN value) {
                        latenciesById.put(txId, System.nanoTime() - latenciesById.get(txId));
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        System.err.println(cause);
                    }
                });

                dlsn.get();
            }
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

        ArrayList<Long> latencies = new ArrayList<>(latenciesById.values());
        latencies.sort(Long::compare);

        long sum = 0;
        for (int i = 0; i < latencies.size(); i++) {
            latencies.set(i, latencies.get(i) / 1000 / 1000);
            sum += latencies.get(i);
        }

        System.out.println("Latencies");
        System.out.println("Count, Avg, Min, Max, 50%, 90%, 95%, 99%, 99.9%");
        System.out.println(String.format("%d, %f, %d, %d, %d, %d, %d, %d, %d",
                latencies.size(),
                sum * 1.0 / latencies.size(),
                latencies.get(0),
                latencies.get(latencies.size() - 1),
                latencies.get((int) (latencies.size() * 0.5)),
                latencies.get((int) (latencies.size() * 0.9)),
                latencies.get((int) (latencies.size() * 0.95)),
                latencies.get((int) (latencies.size() * 0.99)),
                latencies.get((int) (latencies.size() * 0.999))));
    }
}
