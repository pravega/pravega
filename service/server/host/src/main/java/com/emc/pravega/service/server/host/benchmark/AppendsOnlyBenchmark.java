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

package com.emc.pravega.service.server.host.benchmark;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.store.ServiceBuilder;
import lombok.Cleanup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * StreamSegmentStore Benchmark when doing only Appends.
 */
public class AppendsOnlyBenchmark extends Benchmark {
    //region Test Parameters

    private static final int APPEND_COUNT = 10000;
    private static final int MAX_APPEND_SIZE = ONE_MB;
    private static final int[] BURST_SIZES = new int[]{1, 100, 1000, Integer.MAX_VALUE};
    private static final int[] APPEND_SIZES = new int[]{100, 1024, 10 * 1024, 100 * 1024}; //, MAX_APPEND_SIZE};
    private static final int[] SEGMENT_COUNTS = new int[]{1, 5, 10, 20};
    private static final List<TestInput> INPUTS;

    static {
        // Config for Append-Only test
        INPUTS = new ArrayList<>();
        for (int segmentCount : SEGMENT_COUNTS) {
            int appendCount = APPEND_COUNT / segmentCount;
            for (int appendSize : APPEND_SIZES) {
                for (int burstSize : BURST_SIZES) {
                    INPUTS.add(new TestInput(segmentCount, appendCount, appendSize, burstSize));
                }
            }
        }
    }

    //endregion

    //region Constructor

    public AppendsOnlyBenchmark(Supplier<ServiceBuilder> serviceBuilderProvider) {
        super(serviceBuilderProvider);
    }

    //endregion

    //region Benchmark Base Class Implementation

    @Override
    public void run() {
        log("Starting");
        List<TestOutput> results = new ArrayList<>();
        for (TestInput input : INPUTS) {
            @Cleanup
            ServiceBuilder serviceBuilder = super.serviceBuilderProvider.get();
            TestOutput output = runSingleBenchmark(input, serviceBuilder);
            results.add(output);
        }

        printResults(results);
    }

    @Override
    protected String getTestName() {
        return "AppendOnly";
    }

    //endregion

    //region Actual benchmark implementation

    private TestOutput runSingleBenchmark(TestInput testInput, ServiceBuilder serviceBuilder) {
        log("Running %s", testInput);

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

    private void printResults(List<TestOutput> results) {
        Collections.sort(results);
        printResultLine("Segments", "Appends", "Append Size", "Burst Size", "TPut(MB/S)", "L.min", "L.max", "L.avg", "L.50%", "L.90%", "L.95%", "L.99%", "L.99.9%");
        for (TestOutput result : results) {
            int appendCount = result.input.segmentCount * result.input.appendsPerSegment;
            double mbps = (double) result.totalAppendLength / ONE_MB / (result.totalDurationNanos / 1000.0 / 1000 / 1000);
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

    //endregion

    //region TestOutput

    private static class TestOutput implements Comparable<TestOutput> {
        final TestInput input;
        long totalAppendLength;
        long totalDurationNanos;
        final SeriesStatistic latencies;

        TestOutput(TestInput input, SeriesStatistic latencies) {
            this.input = input;
            this.latencies = latencies;
        }

        @Override
        public String toString() {
            return String.format("%s, Tput = %.1f MB/s, Latencies = %s", this.input, (double) this.totalAppendLength / ONE_MB / (this.totalDurationNanos / 1000000000), this.latencies);
        }

        @Override
        public int compareTo(TestOutput testOutput) {
            return this.input.compareTo(testOutput.input);
        }
    }

    //endregion

    //region TestInput

    private static class TestInput implements Comparable<TestInput> {
        final int segmentCount;
        final int appendsPerSegment;
        final int appendSize;
        final int burstSize;

        TestInput(int segmentCount, int appendsPerSegment, int appendSize, int burstSize) {
            this.segmentCount = segmentCount;
            this.appendsPerSegment = appendsPerSegment;
            this.appendSize = appendSize;
            this.burstSize = burstSize;
        }

        @Override
        public String toString() {
            return String.format("Segments = %d, App/Seg = %d, App.Size = %d, BurstSize = %d", this.segmentCount, this.appendsPerSegment, this.appendSize, this.burstSize);
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
}
