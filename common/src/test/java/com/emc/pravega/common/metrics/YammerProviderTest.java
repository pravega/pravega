/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.emc.pravega.common.Timer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * The type Yammer provider test.
 */
public class YammerProviderTest {
    static final String ENVKEY;
    static final int PORT = 9999;
    static {
        Properties properties = new Properties();

        ENVKEY = System.getenv().keySet().iterator().next();
        properties.setProperty("metrics.yammerStatsDHost", "$" + ENVKEY + "$");
        properties.setProperty("metrics.yammerStatsDPort", Integer.toString(PORT));
        MetricsProvider.initialize(new MetricsConfig(properties));
    }
    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("");
    private final DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();

    /**
     * Test Event and Value registered and worked well with OpStats.
     */
    @Test
    public void testOpStatsData() {
        Timer startTime = new Timer();
        OpStatsLogger opStatsLogger = statsLogger.createStats("testOpStatsLogger");
        // register 2 event: 1 success, 1 fail.
        opStatsLogger.reportSuccessEvent(startTime.getElapsed());
        opStatsLogger.reportFailEvent(startTime.getElapsed());
        opStatsLogger.reportSuccessValue(startTime.getElapsedMillis());
        opStatsLogger.reportFailValue(startTime.getElapsedMillis());

        opStatsLogger.reportSuccessValue(1);
        opStatsLogger.reportFailValue(1);
        opStatsLogger.reportSuccessValue(1);

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

        // test dynamic counter
        int sum = 0;
        for (int i = 1; i < 10; i++) {
            sum += i;
            dynamicLogger.incCounterValue("dynamicCounter", i);
            assertEquals(sum, MetricsProvider.YAMMERMETRICS.get().getCounters().get("DYNAMIC.dynamicCounter.Counter").getCount());
        }
    }

    /**
     * Test Meter registered and  worked well with StatsLogger.
     */
    @Test
    public void testMeter() {
        Meter testMeter = statsLogger.createMeter("testMeter");
        testMeter.recordEvent();
        testMeter.recordEvent();
        assertEquals(2, testMeter.getCount());
        testMeter.recordEvents(27);
        assertEquals(29, testMeter.getCount());

        // test dynamic meter
        int sum = 0;
        for (int i = 1; i < 10; i++) {
            sum += i;
            dynamicLogger.recordMeterEvents("dynamicMeter", i);
            assertEquals(sum, MetricsProvider.YAMMERMETRICS.get().getMeters().get("DYNAMIC.dynamicMeter.Meter").getCount());
        }
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
            dynamicLogger.reportGaugeValue("dynamicGauge", i);
            assertEquals(i, MetricsProvider.YAMMERMETRICS.get().getGauges().get("testGauge").getValue());
            assertEquals(i, MetricsProvider.YAMMERMETRICS.get().getGauges().get("DYNAMIC.dynamicGauge.Gauge").getValue());
        }
    }

    /**
     * Test that an arbitrary environment variable can be used to set the value of
     * a config parameter.
     */
    @Test
    public void testConfigArbitraryEnvVar() {
        String value = System.getenv(ENVKEY);
        Assert.assertEquals(MetricsProvider.getConfig().getStatsDHost(), value);
    }

    @Test
    public void testConfigRegularValue() {
        Assert.assertEquals(MetricsProvider.getConfig().getStatsDPort(), PORT);
    }
}
