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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

class PerfStats {
    private final int messageSize;
    private long windowStartTime;
    private long start;
    private long windowStart;
    private long[] latencies;
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

    public PerfStats(long numRecords, int reportingInterval, int messageSize) {
        this.start = System.currentTimeMillis();
        this.windowStartTime = System.currentTimeMillis();
        this.windowStart = 0;
        this.index = 0;
        this.iteration = 0;
        this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
        this.latencies = new long[(int) (numRecords / this.sampling)];
        this.index = 0;
        this.maxLatency = 0;
        this.totalLatency = 0;
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
        this.totalLatency = 0;
        this.reportingInterval = reportingInterval;
        this.messageSize = messageSize;
    }

    public synchronized void record(int iter, int latencyMicro, int bytes, long time) {
        this.count++;
        this.bytes += bytes;
        this.totalLatency += latencyMicro;
        this.maxLatency = Math.max(this.maxLatency, latencyMicro);
        this.windowCount++;
        this.windowBytes += bytes;
        this.windowTotalLatency += latencyMicro;
        this.windowMaxLatency = Math.max(windowMaxLatency, latencyMicro);
        if (iter % this.sampling == 0) {
            this.latencies[index] = latencyMicro;
            this.index++;
        }
        /* maybe report the recent perf */
        if (count - windowStart >= reportingInterval) {
            printWindow();
            newWindow(count);
        }
    }

    private void printWindow() {
        long elapsed = System.currentTimeMillis() - windowStartTime;
        double recsPerSec = 1000.0 * windowCount / (double) elapsed;
        double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0 * 1024.0);
        System.out.printf("%d records sent, %.1f records/sec (%.5f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                windowCount, recsPerSec, mbPerSec, windowTotalLatency / ((double) windowCount * 1000.0),
                (double) windowMaxLatency / 1000.0);
    }

    private void newWindow(long currentNumber) {
        this.windowStart = currentNumber;
        this.windowStartTime = System.currentTimeMillis();
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
    }

    public synchronized void printAll() {
        /*
        for (int i = 0; i < latencies.length; i++) {
            System.out.printf("%d %d\n", i, latencies[i]);

        }
        */
    }

    public synchronized void printTotal() {
        long elapsed = System.currentTimeMillis() - start;
        double recsPerSec = 1000.0 * count / (double) elapsed;
        double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
        long[] percs = percentiles(this.latencies, 0.5, 0.95, 0.99, 0.999);
        System.out.printf(
                "%d records sent, %f records/sec (%.5f MB/sec), %.2f ms avg latency, %.2f ms max " + "latency, %.2f " +
                        "ms 50th, %.2f ms 95th, %.2f ms 99th, %.2f ms 99.9th.\n",
                count, recsPerSec, mbPerSec, totalLatency / ((double) count * 1000.0), (double) maxLatency / 1000.0,
                percs[0] / 1000.0, percs[1] / 1000.0, percs[2] / 1000.0, percs[3] / 1000.0);
        System.out.printf(
                " FINAL:, %d, %.5f MB/sec, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
                messageSize, mbPerSec, totalLatency / ((double) count * 1000.0), (double) maxLatency / 1000.0,
                percs[0] / 1000.0, percs[1] / 1000.0, percs[2] / 1000.0, percs[3] / 1000.0);
    }

    private long[] percentiles(long[] latencies, double... percentiles) {
        long size = Math.min(count, latencies.length);
        Arrays.sort(latencies, 0, (int) size);
        long[] values = new long[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            int index = (int) (percentiles[i] * size);
            values[i] = latencies[index];
        }
        return values;
    }

    public CompletableFuture<Void> runAndRecordTime(Supplier<CompletableFuture<Void>> fn, long startTime, int length) {
        int iter = this.iteration++;
        return fn.get().thenAccept((lmn) -> {
            record(iter, (int) (System.currentTimeMillis() - startTime) * 1000, length, System.nanoTime());
        });

    }
}
