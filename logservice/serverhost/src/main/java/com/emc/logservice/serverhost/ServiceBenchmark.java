/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.serverhost;

import ch.qos.logback.classic.LoggerContext;
import com.emc.logservice.common.CloseableIterator;
import com.emc.logservice.common.FutureHelpers;
import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.server.mocks.InMemoryServiceBuilder;
import com.emc.logservice.server.service.ServiceBuilder;
import com.emc.logservice.server.service.ServiceBuilderConfig;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogException;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.storageabstraction.mocks.InMemoryDurableDataLogFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;
import lombok.Cleanup;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Command-line program that benchmarks the StreamSegmentStore.
 */
public class ServiceBenchmark {
    private static final int APPEND_COUNT = 10000;
    private static final int MAX_APPEND_SIZE = 1024 * 1024;
    private static final int[] BURST_SIZES = new int[]{1, 100, 1000, Integer.MAX_VALUE};
    private static final int[] APPEND_SIZES = new int[]{100, 1024, 10 * 1024, 100 * 1024, MAX_APPEND_SIZE};
    private static final int[] SEGMENT_COUNTS = new int[]{1, 5, 10, 20};
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final List<TestInput> APPENDS_ONLY_INPUTS;

    static {
        // Config for Append-Only test
        APPENDS_ONLY_INPUTS = new ArrayList<>();
        for (int segmentCount : SEGMENT_COUNTS) {
            int appendCount = APPEND_COUNT / segmentCount;
            for (int appendSize : APPEND_SIZES) {
                for (int burstSize : BURST_SIZES) {
                    APPENDS_ONLY_INPUTS.add(new TestInput(segmentCount, appendCount, appendSize, burstSize));
                }
            }
        }
    }

    public static void main(String[] args) {
        // Turn off all logging.
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        //context.getLoggerList().get(0).setLevel(Level.INFO);
        context.reset();

        benchmarkAppendsOnly();
        benchmarkReadsOnly();
        benchmarkAppendAndReads();
    }

    //region Only Appends

    private static void benchmarkAppendsOnly() {
        log("AppendOnly", "Start");
        List<TestOutput> results = new ArrayList<>();
        for (TestInput input : APPENDS_ONLY_INPUTS) {
            TestOutput output = benchmarkAppendsOnly(input);
            results.add(output);
        }

        printResults(results);
    }

    private static void printResults(List<TestOutput> results) {
        Collections.sort(results);
        printResultLine("Segments", "Appends", "Append Size", "Burst Size", "TPut(MB/S)", "L.min", "L.max", "L.avg", "L.50%", "L.90%", "L.95%", "L.99%", "L.99.9%");
        for (TestOutput result : results) {
            int appendCount = result.input.segmentCount * result.input.appendsPerSegment;
            double mbps = result.totalAppendLength / 1024.0 / 1024 / (result.totalDurationNanos / 1000.0 / 1000 / 1000);
            printResultLine(
                    result.input.segmentCount,
                    appendCount,
                    result.input.appendSize,
                    result.input.burstSize,
                    mbps,
                    result.latencies.min(),
                    result.latencies.max(),
                    result.latencies.avg(),
                    result.latencies.percentile(50),
                    result.latencies.percentile(90),
                    result.latencies.percentile(95),
                    result.latencies.percentile(99),
                    result.latencies.percentile(99.9));
        }
    }

    private static void printResultLine(Object... args) {
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg instanceof Double) {
                System.out.print(String.format("%.1f", arg));
            } else {
                System.out.print(arg);
            }

            if (i < args.length - 1) {
                System.out.print(", ");
            }
        }
        System.out.println();
    }

    private static TestOutput benchmarkAppendsOnly(TestInput testInput) {
        log("AppendOnly", "Running %s", testInput);

        // TODO: add custom config (i.e., DurableLog auto-inject checkpoint).
        ServiceBuilderConfig serviceBuilderConfig = ServiceBuilderConfig.getDefaultConfig();
        @Cleanup
        //ServiceBuilder serviceBuilder = new NoOpDataLogServiceBuilder(serviceBuilderConfig);
                ServiceBuilder serviceBuilder = new InMemoryServiceBuilder(serviceBuilderConfig);
        serviceBuilder.getContainerManager().initialize(TIMEOUT).join();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        List<String> segmentNames = createStreamSegments(store, testInput.segmentCount);

        // Use a single append buffer for all appends. This makes sure that what we are measuring is actually the
        // time the StreamSegmentStore uses to process the append, not to do extra copying in memory.
        final byte[] appendData = new byte[testInput.appendSize];
        final UUID clientId = UUID.randomUUID();
        final long testStartTime = System.nanoTime();

        // Store the Futures for all appends in the current burst. We will occasionally wait for all of them, based
        // on test input.
        List<CompletableFuture<Void>> burstResults = new ArrayList<>();
        TestOutput result = new TestOutput(testInput, new SeriesStatistic());

        for (int appendIndex = 0; appendIndex < testInput.appendsPerSegment; appendIndex++) {
            // Append context is unique per Append Index (we can share it with multiple StreamSegments).
            AppendContext appendContext = new AppendContext(clientId, appendIndex);
            for (String segmentName : segmentNames) {
                // Do the append.
                final long startTimeNanos = System.nanoTime();
                CompletableFuture<Void> completionFuture = store
                        .append(segmentName, appendData, appendContext, TIMEOUT)
                        .thenRunAsync(() -> {
                            // Record latencies when the future is done.
                            long elapsedNanos = System.nanoTime() - startTimeNanos;
                            synchronized (result) {
                                result.totalAppendLength += appendData.length;
                                result.latencies.add(elapsedNanos / 1000000.0);
                            }
                        });
                burstResults.add(completionFuture);

                // Check to see if we need to wait on the recently added appends.
                if (burstResults.size() >= testInput.burstSize) {
                    FutureHelpers.allOf(burstResults).join();
                    burstResults.clear();
                }
            }
        }

        if (burstResults.size() > 0) {
            FutureHelpers.allOf(burstResults).join();
            burstResults.clear();
        }

        result.totalDurationNanos = System.nanoTime() - testStartTime;
        result.latencies.snapshot();
        return result;
    }

    //endregion

    //region Only Reads

    private static void benchmarkReadsOnly() {
        // TODO: implement.
    }

    //endregion

    //region Appends and Reads

    private static void benchmarkAppendAndReads() {
        // TODO: implement.
    }

    //endregion

    //region Helpers

    private static List<String> createStreamSegments(StreamSegmentStore store, int segmentCount) {
        List<CompletableFuture<Void>> results = new ArrayList<>();
        List<String> result = new ArrayList<>();

        for (int i = 0; i < segmentCount; i++) {
            String name = String.format("StreamSegment_%d", i);
            result.add(name);
            results.add(store.createStreamSegment(name, TIMEOUT));
        }

        FutureHelpers.allOf(results).join();
        return result;
    }

    private static void log(String testName, String messageTemplate, Object... args) {
        System.out.println(testName + ": " + String.format(messageTemplate, args));
    }

    //endregion

    //region TestOutput

    private static class TestOutput implements Comparable<TestOutput> {
        public final TestInput input;
        public long totalAppendLength;
        public long totalDurationNanos;
        public final SeriesStatistic latencies;

        public TestOutput(TestInput input, SeriesStatistic latencies) {
            this.input = input;
            this.latencies = latencies;
        }

        @Override
        public String toString() {
            return String.format("%s, Tput = %.1f, Latencies = %s", this.input, this.totalAppendLength / 1024.0 * 1024 / (this.totalDurationNanos / 1000000000), this.latencies);
        }

        @Override
        public int compareTo(TestOutput testOutput) {
            return this.input.compareTo(testOutput.input);
        }
    }

    //endregion

    //region SeriesStatistic

    private static class SeriesStatistic {
        private final ArrayList<Double> items;
        private double average;
        private boolean isSnapshot;

        public SeriesStatistic() {
            this.items = new ArrayList<>();
        }

        public void add(double value) {
            this.items.add(value);
        }

        public void snapshot() {
            Collections.sort(this.items);

            // Calculate average.
            if (this.items.size() > 0) {
                AtomicDouble sum = new AtomicDouble();
                this.items.forEach(sum::addAndGet);
                this.average = sum.get() / this.items.size();
            }

            this.isSnapshot = true;
        }

        public double min() {
            Preconditions.checkState(this.isSnapshot, "Cannot get a statistic if snapshot() hasn't been run.");
            Preconditions.checkState(this.items.size() > 0, "No elements in the series.");
            return this.items.get(0);
        }

        public double max() {
            Preconditions.checkState(this.isSnapshot, "Cannot get a statistic if snapshot() hasn't been run.");
            Preconditions.checkState(this.items.size() > 0, "No elements in the series.");
            return this.items.get(this.items.size() - 1);
        }

        public double avg() {
            Preconditions.checkState(this.isSnapshot, "Cannot get a statistic if snapshot() hasn't been run.");
            Preconditions.checkState(this.items.size() > 0, "No elements in the series.");
            return this.average;
        }

        public double percentile(double percentile) {
            Preconditions.checkState(this.isSnapshot, "Cannot get a statistic if snapshot() hasn't been run.");
            Preconditions.checkState(this.items.size() > 0, "No elements in the series.");
            Preconditions.checkArgument(percentile >= 0 && percentile <= 100, "Invalid percentile. Must be between 0 and 100 (inclusive).");
            int index = (int) (this.items.size() * percentile / 100);
            return this.items.get(index);
        }

        @Override
        public String toString() {
            if (!this.isSnapshot) {
                return "Snapshot was not run";
            } else if (this.items.size() == 0) {
                return "Empty series.";
            } else {
                return String.format(
                        "Min = %.1f, Max = %.1f, Avg = %.1f, 50%% = %.1f, 90%% = %.1f, 95%% = %.1f, 99%% = %.1f, 99.9%% = %.1f",
                        min(),
                        max(),
                        avg(),
                        percentile(50),
                        percentile(90),
                        percentile(95),
                        percentile(99),
                        percentile(99.9));
            }
        }
    }

    //endregion

    //region TestInput

    private static class TestInput implements Comparable<TestInput> {
        public final int segmentCount;
        public final int appendsPerSegment;
        public final int appendSize;
        public final int burstSize;

        public TestInput(int segmentCount, int appendsPerSegment, int appendSize, int burstSize) {
            this.segmentCount = segmentCount;
            this.appendsPerSegment = appendsPerSegment;
            this.appendSize = appendSize;
            this.burstSize = burstSize;
        }

        @Override
        public String toString() {
            return String.format("SegmentCount = %d, AppendsPerSegment = %d, AppendSize = %d, BurstSize = %d", this.segmentCount, this.appendsPerSegment, this.appendSize, this.burstSize);
        }

        @Override
        public int compareTo(TestInput testInput) {
            int diff = testInput.segmentCount - this.segmentCount;
            if (diff != 0) {
                diff = testInput.appendsPerSegment - this.appendsPerSegment;
            }
            if (diff != 0) {
                diff = testInput.appendSize - this.appendSize;
            }
            if (diff != 0) {
                diff = testInput.burstSize - this.burstSize;
            }

            return diff;
        }
    }

    //endregion

    //region NoOpDataLogServiceBuilder

    private static class NoOpDataLogServiceBuilder extends InMemoryServiceBuilder {

        public NoOpDataLogServiceBuilder(ServiceBuilderConfig config) {
            super(config);
        }

        @Override
        protected DurableDataLogFactory createDataLogFactory() {
            return new InMemoryDurableDataLogFactory();
        }

        private static class NoOpDurableDataLogFactory implements DurableDataLogFactory {

            @Override
            public DurableDataLog createDurableDataLog(String containerId) {
                return new NoOpDurableDataLog();
            }

            @Override
            public void close() {
                // Nothing to do.
            }
        }

        private static class NoOpDurableDataLog implements DurableDataLog {
            private final AtomicLong currentOffset = new AtomicLong();

            @Override
            public void initialize(Duration timeout) throws DurableDataLogException {
                // Nothing to do.
            }

            @Override
            public CompletableFuture<Long> append(InputStream data, Duration timeout) {
                return CompletableFuture.completedFuture(this.currentOffset.incrementAndGet());
            }

            @Override
            public CompletableFuture<Boolean> truncate(long upToSequence, Duration timeout) {
                return CompletableFuture.completedFuture(true);
            }

            @Override
            public CloseableIterator<ReadItem, DurableDataLogException> getReader(long afterSequence) throws DurableDataLogException {
                return null;
            }

            @Override
            public int getMaxAppendLength() {
                return 0;
            }

            @Override
            public long getLastAppendSequence() {
                return this.currentOffset.get();
            }

            @Override
            public void close() {
                // Nothing to do.
            }
        }

        private static class NoOpIterator implements CloseableIterator<DurableDataLog.ReadItem, DurableDataLogException> {

            @Override
            public DurableDataLog.ReadItem getNext() throws DurableDataLogException {
                return null;
            }

            @Override
            public void close() {
                // Nothing to do.
            }
        }
    }

    //endregion
}
