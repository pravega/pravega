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

package com.emc.logservice.serverhost.benchmark;

import com.emc.logservice.common.FutureHelpers;
import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.ContainerNotFoundException;
import com.emc.logservice.contracts.RuntimeStreamingException;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.server.ContainerHandle;
import com.emc.logservice.server.SegmentContainer;
import com.emc.logservice.server.SegmentContainerRegistry;
import com.emc.logservice.server.service.ServiceBuilder;
import lombok.Cleanup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * StreamSegmentStore benchmark for Recovery.
 */
public class RecoveryBenchmark extends Benchmark {
    //region TestParameters

    private static final int[] SEGMENT_COUNTS = new int[]{1};
    private static final int[] TOTAL_APPEND_LENGTH_MB = new int[]{128};
    private static final int[] APPEND_SIZES = new int[]{ONE_MB};
    //    private static final int[] SEGMENT_COUNTS = new int[]{1, 1000, 100 * 1000};
    //    private static final int[] TOTAL_APPEND_LENGTH_MB = new int[]{128, 512, 1024, 2048};
    //    private static final int[] APPEND_SIZES = new int[]{100, 10 * 1024, 100 * 1024, ONE_MB};
    private static final List<TestInput> INPUTS;

    static {
        INPUTS = new ArrayList<>();
        for (int segmentCount : SEGMENT_COUNTS) {
            for (long totalAppendMb : TOTAL_APPEND_LENGTH_MB) {
                for (int appendSize : APPEND_SIZES) {
                    // Calculate # of appends per segment, given total number of appends, their sizes, and # of segments.
                    int appendCount = (int) (totalAppendMb * ONE_MB / appendSize / segmentCount);
                    if (appendCount <= 0) {
                        // Some combinations really don't make sense, but that's what we get for doing a cross-product
                        // of various sets of input parameters. It's OK, we can safely skip these.
                        continue;
                    }

                    INPUTS.add(new TestInput(segmentCount, appendCount, appendSize, totalAppendMb * ONE_MB));
                }
            }
        }
    }

    //endregion

    //region Constructor

    public RecoveryBenchmark(Supplier<ServiceBuilder> serviceBuilderProvider) {
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
            try {
                TestOutput output = runSingleBenchmark(input, serviceBuilder);
                results.add(output);
            } catch (Exception ex) {
                System.out.println(ex);
                throw ex;
            }
        }

        printResults(results);
    }

    @Override
    protected String getTestName() {
        return "Recovery";
    }

    //endregion

    //region Actual benchmark implementation

    private TestOutput runSingleBenchmark(TestInput testInput, ServiceBuilder serviceBuilder) {
        final String containerId = "0";
        log("Running %s", testInput);

        serviceBuilder.getContainerManager().initialize(TIMEOUT).join();
        SegmentContainerRegistry containerRegistry = serviceBuilder.getSegmentContainerRegistry();
        if (containerRegistry.getContainerCount() != 1) {
            throw new IllegalStateException("This benchmark only works if there is exactly one container registered.");
        }

        SegmentContainer container;
        try {
            container = serviceBuilder.getSegmentContainerRegistry().getContainer(containerId);
        } catch (ContainerNotFoundException ex) {
            throw new RuntimeStreamingException(ex);
        }

        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        List<String> segmentNames = createStreamSegments(store, testInput.segmentCount);

        // Use a single append buffer for all appends. This makes sure that what we are measuring is actually the
        // time the StreamSegmentStore uses to process the append, not to do extra copying in memory.
        final byte[] appendData = new byte[testInput.appendSize];
        final UUID clientId = UUID.randomUUID();

        // Do all the appends.
        List<CompletableFuture<Long>> appendCompletions = new ArrayList<>();
        for (int appendIndex = 0; appendIndex < testInput.appendsPerSegment; appendIndex++) {
            // Append context is unique per Append Index (we can share it with multiple StreamSegments).
            AppendContext appendContext = new AppendContext(clientId, appendIndex);
            for (String segmentName : segmentNames) {
                CompletableFuture<Long> appendCompletion = store.append(segmentName, appendData, appendContext, TIMEOUT);
                appendCompletions.add(appendCompletion);
            }
        }

        // Wait for all appends to be committed.
        FutureHelpers.allOf(appendCompletions).join();

        // Stop the service and do a recovery.
        container.stopAsync().awaitTerminated();
        container.close();
        TestOutput result = new TestOutput(testInput);
        final long testStartTime = System.nanoTime();
        ContainerHandle handle = serviceBuilder.getSegmentContainerRegistry().startContainer(containerId, TIMEOUT).join();
        result.totalDurationNanos = System.nanoTime() - testStartTime;
        return result;
    }

    private void printResults(List<TestOutput> results) {
        Collections.sort(results);
        printResultLine("Segments", "Appends", "Append Size", "TotalLength (MB)", "TPut(MB/S)", "Duration (s)");
        for (TestOutput result : results) {
            int appendCount = result.input.segmentCount * result.input.appendsPerSegment;
            double duration = result.totalDurationNanos / 1000.0 / 1000 / 1000;
            double mbps = (double) result.input.totalAppendLength / ONE_MB / duration;
            printResultLine(
                    result.input.segmentCount,
                    appendCount,
                    result.input.appendSize,
                    result.input.totalAppendLength / ONE_MB,
                    mbps,
                    duration);
        }
    }
    //endregion

    //region TestOutput

    private static class TestOutput implements Comparable<TestOutput> {
        public final TestInput input;
        public long totalDurationNanos;

        public TestOutput(TestInput input) {
            this.input = input;
        }

        @Override
        public String toString() {
            return String.format("%s, Tput = %.1f MB/s, Latencies = %s", this.input, (double) this.input.totalAppendLength / ONE_MB / (this.totalDurationNanos / 1000000000));
        }

        @Override
        public int compareTo(TestOutput testOutput) {
            return this.input.compareTo(testOutput.input);
        }
    }

    //endregion

    //region TestInput

    private static class TestInput implements Comparable<TestInput> {
        public final int segmentCount;
        public final int appendsPerSegment;
        public final int appendSize;
        public final long totalAppendLength;

        public TestInput(int segmentCount, int appendsPerSegment, int appendSize, long totalAppendLength) {
            this.segmentCount = segmentCount;
            this.appendsPerSegment = appendsPerSegment;
            this.appendSize = appendSize;
            this.totalAppendLength = totalAppendLength;
        }

        @Override
        public String toString() {
            return String.format("Segments = %d, App/Seg = %d, App.Size = %d, TotalAppendLength = %d", this.segmentCount, this.appendsPerSegment, this.appendSize, this.totalAppendLength);
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
                diff = Long.compare(testInput.totalAppendLength, this.totalAppendLength);
            }

            return diff;
        }
    }

    //endregion
}
