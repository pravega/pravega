/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.common.metrics;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The type Yammer provider test.
 */
public class YammerProviderTest {
    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("");

    /**
     * Test Event and Value registered and worked well with OpStats.
     */
    @Test
    public void testOpStatsData() {
        long startTime = System.nanoTime();
        OpStatsLogger opStatsLogger = statsLogger.createStats("testOpStatsLogger");
        // register 2 event: 1 success, 1 fail.
        opStatsLogger.registerSuccessfulEvent(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        opStatsLogger.registerFailedEvent(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        opStatsLogger.reportSuccess(System.nanoTime() - startTime);
        opStatsLogger.reportFailure(System.nanoTime() - startTime);

        opStatsLogger.registerSuccessfulValue(1);
        opStatsLogger.registerFailedValue(1);
        opStatsLogger.report(1);

        OpStatsData statsData = opStatsLogger.toOpStatsData();
        // 2 = 2 event + 2 value
        assertEquals(4, statsData.getNumSuccessfulEvents());
        assertEquals(3, statsData.getNumFailedEvents());
    }

    /**
     * Test counter registered and  worked well with StatsLogger.
     */
    @Test
    public void testCounter() {
        Counter testCounter = statsLogger.createCounter("testCounter");
        testCounter.add(17);
        assertEquals(17, testCounter.get());
    }

    /**
     * Test gauge registered and  worked well with StatsLogger..
     */
    @Test
    public void testGauge() {
        AtomicInteger value = new AtomicInteger(1);
        statsLogger.registerGauge("testGauge", value::get);
        for (int i = 1; i < 10; i++) {
            value.set(i);
            assertEquals(i, MetricsProvider.getMetrics().getGauges().get("testGauge").getValue());
        }
    }
}
