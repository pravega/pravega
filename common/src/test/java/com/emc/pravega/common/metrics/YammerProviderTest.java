/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.emc.pravega.common.Timer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

import static org.junit.Assert.assertEquals;

/**
 * The type Yammer provider test.
 */
@Slf4j
public class YammerProviderTest {

    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("");
    private final DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();

    @Before
    public void setUp() {
        Properties properties = new Properties();

        properties.setProperty("metrics." + MetricsConfig.ENABLE_STATISTICS, "true");
        MetricsProvider.initialize(new MetricsConfig(properties));
    }

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
            assertEquals(sum, MetricsProvider.YAMMERMETRICS.getCounters().get("DYNAMIC.dynamicCounter.Counter").getCount());
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
            assertEquals(sum, MetricsProvider.YAMMERMETRICS.getMeters().get("DYNAMIC.dynamicMeter.Meter").getCount());
        }
    }

    /**
     * Test gauge registered and  worked well with StatsLogger.
     */
    @Test
    public void testGauge() {
        AtomicInteger value = new AtomicInteger(1);
        statsLogger.registerGauge("testGauge", value::get);

        for (int i = 1; i < 10; i++) {
            value.set(i);
            dynamicLogger.reportGaugeValue("dynamicGauge", i);
            assertEquals(i, MetricsProvider.YAMMERMETRICS.getGauges().get("testGauge").getValue());
            assertEquals(i, MetricsProvider.YAMMERMETRICS.getGauges().get("DYNAMIC.dynamicGauge.Gauge").getValue());
        }
    }

    /**
     * Test that an arbitrary environment variable can be used to set the value of
     * a config parameter.
     */
    @Test
    public void testConfigArbitraryEnvVar() {
        Properties properties = new Properties();
        String envKey = System.getenv().keySet().iterator().next();
        int port = 9999;
        properties.setProperty("metrics.yammerStatsDHost", "$" + envKey + "$");
        properties.setProperty("metrics.yammerStatsDPort", Integer.toString(port));
        String value = System.getenv(envKey);
        MetricsConfig config = new MetricsConfig(properties);

        assertEquals(config.getStatsDHost(), value);
        assertEquals(config.getStatsDPort(), port);
    }

    /**
     * Test that we can transition from stats enabled, to disabled, to enabled.
     */
    @Test
    public void testMultipleInitialization() {
        Properties properties = new Properties();
        MetricsConfig config;

        properties.setProperty("metrics." + MetricsConfig.ENABLE_STATISTICS, "false");
        config = new MetricsConfig(properties);
        MetricsProvider.initialize(config);
        statsLogger.createCounter("counterDisabled");

        assertEquals(null, MetricsProvider.YAMMERMETRICS.getCounters().get("counterDisabled"));

        properties.setProperty("metrics." + MetricsConfig.ENABLE_STATISTICS, "true");
        config = new MetricsConfig(properties);
        MetricsProvider.initialize(config);
        statsLogger.createCounter("counterEnabled");

        Assert.assertNotNull(MetricsProvider.YAMMERMETRICS.getCounters().get("counterEnabled"));
    }

    /**
     * Test that we can transition from stats enabled, to disabled, to enabled.
     */
    @Test
    public void testContinuity() {
        Properties properties = new Properties();
        MetricsConfig config;

        statsLogger.createCounter("continuity-counter");
        properties.setProperty("metrics." + MetricsConfig.ENABLE_STATISTICS, "true");
        config = new MetricsConfig(properties);
        MetricsProvider.initialize(config);

        Assert.assertNotNull(null, MetricsProvider.YAMMERMETRICS.getCounters().get("continuity-counter"));
    }
}
