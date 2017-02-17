/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.demo;

import javax.annotation.concurrent.GuardedBy;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

class PerfStats {
    private long start;
    private long windowStart;
    private int[] latencies;
    private int sampling;
    @GuardedBy("lock")
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
    private final Object lock = new Object();

    public PerfStats(long numRecords, int reportingInterval) {
        this.start = System.nanoTime();
        //  this.windowStart = System.nanoTime();
        this.windowStart = 0;
        this.index = 0;
        this.iteration = 0;
        this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
        this.latencies = new int[(int) (numRecords / this.sampling) ];
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
        if (count - windowStart >= reportingInterval) {
            printWindow();
            newWindow(count);
        }
    }

    private void printWindow() {
        long elapsed = System.nanoTime() - windowStart;
        double recsPerSec = 1000000.0 * windowCount / (double) elapsed;
        double mbPerSec = 1000000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
        System.out.printf("%d records sent, %.1f records/sec (%.5f MB/sec), %.1f ms avg latency, %.1f max latency.%n",
                windowCount,
                recsPerSec,
                mbPerSec,
                windowTotalLatency / (double) windowCount,
                (double) windowMaxLatency);
    }

    private void newWindow(long currentNumber) {
        //  this.windowStart = System.nanoTime();
        this.windowStart = currentNumber;
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
    }

    public synchronized void printAll() {
        for (int i = 0; i < latencies.length; i++) {
            System.out.printf("%d %d %n", i, latencies[i]);

        }
    }

    public synchronized void printTotal() {
        long elapsed = System.nanoTime() - start;
        double recsPerSec = 1000000.0 * count / (double) elapsed;
        double mbPerSec = 1000000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
        int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
        System.out.printf("%d records sent, %f records/sec (%.5f MB/sec), %.2f ms avg latency, %.2f ms max " +
                        "latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
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

    public CompletableFuture<Void> runAndRecordTime(Supplier<CompletableFuture<Void>> fn,
                                                    long startTime,
                                                    int length) {
        int iter = incrementIter();
        return fn.get().thenAccept( (lmn) -> {
            record(iter, (int) (System.nanoTime() - startTime), length,
                    System.nanoTime());
        });
    }

    private int incrementIter() {
        synchronized (this.lock) {
            return this.iteration++;
        }
    }
}
