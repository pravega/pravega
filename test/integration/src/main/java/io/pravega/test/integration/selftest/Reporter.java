/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.pravega.common.AbstractTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.val;

/**
 * Reports Test State on a periodic basis.
 */
class Reporter extends AbstractScheduledService {
    //region Members

    private static final int ONE_MB = 1024 * 1024;
    private static final String LOG_ID = "Reporter";
    private static final int REPORT_INTERVAL_MILLIS = 1000;
    private final TestState testState;
    private final TestConfig testConfig;
    private final Supplier<ExecutorServiceHelpers.Snapshot> storePoolSnapshotProvider;
    private final ScheduledExecutorService executorService;
    private final AtomicLong lastReportTime = new AtomicLong(-1);
    private final AtomicLong lastReportLength = new AtomicLong(-1);
    private final AtomicLong lastReportOperationCount = new AtomicLong(-1);

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Reporter class.
     *
     * @param testState                 The TestState to attach to.
     * @param testConfig                The TestConfig to use.
     * @param storePoolSnapshotProvider A Supplier that can return a Snapshot of the Store Executor Pool.
     * @param executorService           The executor service to use.
     */
    Reporter(TestState testState, TestConfig testConfig, Supplier<ExecutorServiceHelpers.Snapshot> storePoolSnapshotProvider, ScheduledExecutorService executorService) {
        this.testState = Preconditions.checkNotNull(testState, "testState");
        this.testConfig = Preconditions.checkNotNull(testConfig, "testConfig");
        this.storePoolSnapshotProvider = Preconditions.checkNotNull(storePoolSnapshotProvider, "storePoolSnapshotProvider");
        this.executorService = Preconditions.checkNotNull(executorService, "executorService");
    }

    //endregion

    //region AbstractScheduledService Implementation

    @Override
    protected void runOneIteration() {
        outputState();
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(REPORT_INTERVAL_MILLIS, REPORT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    protected ScheduledExecutorService executor() {
        return this.executorService;
    }

    //endregion

    /**
     * Outputs the current state of the test.
     */
    void outputState() {
        val testPoolSnapshot = ExecutorServiceHelpers.getSnapshot(this.executorService);
        @SuppressWarnings("ImportControl")
        val joinPoolSnapshot = ExecutorServiceHelpers.getSnapshot(java.util.concurrent.ForkJoinPool.commonPool());
        val storePoolSnapshot = this.storePoolSnapshotProvider.get();
        long time = System.nanoTime();
        long producedLength = this.testState.getProducedLength();
        long opCount = this.testState.getSuccessfulOperationCount();
        long lastReportTime = this.lastReportTime.getAndSet(time);
        double elapsedSeconds = toSeconds(time - lastReportTime);
        double instantThroughput = lastReportTime < 0 ? 0 : (producedLength - this.lastReportLength.get()) / elapsedSeconds;
        double instantOps = lastReportTime < 0 ? 0 : Math.max(0, opCount - this.lastReportOperationCount.get()) / elapsedSeconds;

        this.lastReportLength.set(producedLength);
        this.lastReportOperationCount.set(opCount);

        String ops = this.testState.isWarmup()
                ? "(warmup)"
                : String.format("%s/%s", this.testState.getSuccessfulOperationCount(), this.testConfig.getOperationCount());

        String threadPools = String.format("TPools (Q/T/S): %s, %s, %s",
                formatSnapshot(storePoolSnapshot, "S"),
                formatSnapshot(testPoolSnapshot, "T"),
                formatSnapshot(joinPoolSnapshot, "FJ"));

        if (this.testConfig.getTestType().isTablesTest()) {
            outputTableState(producedLength, ops, (int) instantOps, instantThroughput, threadPools);
        } else {
            outputStreamState(producedLength, ops, (int) instantOps, instantThroughput, threadPools);
        }
    }

    private void outputStreamState(long producedLength, String ops, int instantOps, double instantThroughput, String threadPools) {
        String data = String.format(this.testConfig.isReadsEnabled() ? " (P/T/C/S): %.1f/%.1f/%.1f/%.1f" : ": %.1f",
                toMB(producedLength),
                toMB(this.testState.getVerifiedTailLength()),
                toMB(this.testState.getVerifiedCatchupLength()),
                toMB(this.testState.getVerifiedStorageLength()));

        TestLogger.log(
                LOG_ID,
                "%s; Ops = %d/%d; Data%s MB; TPut: %.1f/%.1f MB/s; %s.",
                ops,
                instantOps,
                (int) this.testState.getOperationsPerSecond(),
                data,
                instantThroughput < 0 ? 0.0 : toMB(instantThroughput),
                toMB(this.testState.getThroughput()),
                threadPools);
    }

    private void outputTableState(long producedLength, String ops, int instantOps, double instantThroughput, String threadPools) {
        TestLogger.log(
                LOG_ID,
                "%s; Ops = %d/%d; U/G/GK:%d/%d/%d; Data: %.1f MB; TPut: %.1f/%.1f MB/s; %s.",
                ops,
                instantOps,
                (int) this.testState.getOperationsPerSecond(),
                this.testState.getTableUpdateCount(),
                this.testState.getTableGetCount(),
                this.testState.getTableGetTotalKeyCount(),
                toMB(producedLength),
                instantThroughput < 0 ? 0.0 : toMB(instantThroughput),
                toMB(this.testState.getThroughput()),
                threadPools);
    }

    private String formatSnapshot(ExecutorServiceHelpers.Snapshot s, String name) {
        if (s == null) {
            return String.format("%s = ?/?/?", name);
        }

        return String.format("%s = %d/%d/%d", name, s.getQueueSize(), s.getActiveThreadCount(), s.getPoolSize());
    }

    /**
     * Outputs a summary for all the operation types (Count + Latencies).
     */
    void outputSummary() {
        TestLogger.log(LOG_ID, "Operation Summary");
        outputRow("Operation Type", "Count", "LSum", "LAvg", "L50", "L75", "L90", "L99", "L999");
        for (OperationType ot : TestState.SUMMARY_OPERATION_TYPES) {
            val durations = this.testState.getDurations(ot);
            if (durations == null || durations.count() == 0) {
                continue;
            }

            int[] percentiles = durations.percentiles(0.5, 0.75, 0.9, 0.99, 0.999);
            outputRow(ot, durations.count(), (int) durations.sum(), (int) durations.average(), percentiles[0], percentiles[1], percentiles[2], percentiles[3], percentiles[4]);
        }
    }

    private void outputRow(Object opType, Object count, Object sum, Object lAvg, Object l50, Object l75, Object l90, Object l99, Object l999) {
        TestLogger.log(LOG_ID, "%18s | %7s | %9s | %5s | %5s | %5s | %5s | %5s | %5s", opType, count, sum, lAvg, l50, l75, l90, l99, l999);
    }

    private double toMB(double bytes) {
        return bytes / ONE_MB;
    }

    private double toSeconds(long nanos) {
        return (double) nanos / AbstractTimer.NANOS_TO_MILLIS / 1000;
    }
}
