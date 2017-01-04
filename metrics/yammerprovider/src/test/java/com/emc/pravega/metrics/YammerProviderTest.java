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
package com.emc.pravega.metrics;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

public class YammerProviderTest {
    private final StatsLogger statsLogger = MetricsFactory.getStatsLogger();

    @Test
    public void testToOpStatsData() {
        long startTime = System.nanoTime();
        OpStatsLogger opStatsLogger = statsLogger.getOpStatsLogger("testOpStatsLogger");
        // register 2 event: 1 success, 1 fail.
        opStatsLogger.registerSuccessfulEvent(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
        opStatsLogger.registerFailedEvent(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);

        opStatsLogger.registerSuccessfulValue(1);
        opStatsLogger.registerFailedValue(1);
        // the following should not throw any exception
        OpStatsData statsData = opStatsLogger.toOpStatsData();
        // 2 = 1 event + 1 value
        assertEquals(2, statsData.getNumSuccessfulEvents());
        assertEquals(2, statsData.getNumFailedEvents());
    }

    @Test
    public void testToCounter() {
        Counter testCounter = statsLogger.getCounter("testCounter");
        testCounter.add(17);
        assertEquals(Long.valueOf(17), testCounter.get());
    }

    @Test
    public void testToGauge() {
        Gauge gauge = new Gauge<Integer>() {
            @Override
            public Integer getDefaultValue() {
                return 0;
            }

            @Override
            public Integer getSample() {
                return 27;
            }
        };
        statsLogger.registerGauge("testGauge", gauge);
        assertEquals(27, MetricsFactory.getMetrics().getGauges().get("testGauge").getValue());
    }
}
