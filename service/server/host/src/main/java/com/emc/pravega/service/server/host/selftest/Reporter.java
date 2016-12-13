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

package com.emc.pravega.service.server.host.selftest;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final ScheduledExecutorService executorService;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Reporter class.
     *
     * @param testState       The TestState to attach to.
     * @param testConfig      The TestConfig to use.
     * @param executorService The executor service to use.
     */
    Reporter(TestState testState, TestConfig testConfig, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(testState, "testState");
        Preconditions.checkNotNull(testConfig, "testConfig");
        Preconditions.checkNotNull(executorService, "executorService");
        this.testState = testState;
        this.testConfig = testConfig;
        this.executorService = executorService;
    }

    //endregion

    //region AbstractScheduledService Implementation

    @Override
    protected void runOneIteration() throws Exception {
        long generatedOperationCount = this.testState.getGeneratedOperationCount();
        double producedLength = toMB(this.testState.getProducedLength());
        double verifiedTailLength = toMB(this.testState.getVerifiedTailLength());
        double verifiedCatchupLength = toMB(this.testState.getVerifiedCatchupLength());
        double verifiedStorageLength = toMB(this.testState.getVerifiedStorageLength());

        TestLogger.log(
                LOG_ID,
                "Ops = %s/%s, Produced = %.2f MB, Verified: T = %.2f MB, C = %.2f MB, S = %.2f MB",
                generatedOperationCount,
                this.testConfig.getOperationCount(),
                producedLength,
                verifiedTailLength,
                verifiedCatchupLength,
                verifiedStorageLength);
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
     * Outputs a summary for all the operation types (Count + Latencies).
     */
    void outputSummary() {
        TestLogger.log(LOG_ID, "Operation Summary");
        outputRow("Operation Type", "Count", "LAvg", "L50", "L90", "L99", "L999");
        for (OperationType ot : TestState.SUMMARY_OPERATION_TYPES) {
            List<Integer> durations = this.testState.getSortedDurations(ot);
            if (durations == null || durations.size() == 0) {
                continue;
            }

            outputRow(ot,
                    durations.size(),
                    (int) durations.stream().mapToDouble(a -> a).average().orElse(0.0),
                    getPercentile(durations, 0.5),
                    getPercentile(durations, 0.9),
                    getPercentile(durations, 0.99),
                    getPercentile(durations, 0.999));
        }
    }

    private void outputRow(Object opType, Object count, Object lAvg, Object l50, Object l90, Object l99, Object l999) {
        TestLogger.log(LOG_ID, "%18s | %6s | %5s | %5s | %5s | %5s | %5s", opType, count, lAvg, l50, l90, l99, l999);
    }

    private <T> T getPercentile(List<T> list, double percentile) {
        return list.get((int) (list.size() * percentile));
    }

    private double toMB(long bytes) {
        return bytes / (double) ONE_MB;
    }
}
