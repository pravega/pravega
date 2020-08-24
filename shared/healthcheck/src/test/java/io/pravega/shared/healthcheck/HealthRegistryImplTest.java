/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.healthcheck;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class HealthRegistryImplTest {

    @Test
    public void testHealthCheckTimeout() {
        HealthRegistryImpl.resetInstance();
        HealthRegistryImpl registry = HealthRegistryImpl.getInstance();
        SampleHealthInfoProvider provider1 = new SampleHealthInfoProvider("id", HealthAspect.SEGMENT_CONTAINER, () -> {
            try {
                Thread.sleep(1200);
                return new HealthInfo(HealthInfo.Status.HEALTH, "TEST aspect health-check result with 1.2 second delay");
            } catch (Exception e) {
                return new HealthInfo(HealthInfo.Status.UNHEALTH, "Exception thrown: " + e);
            }
        });

        Optional<HealthInfo> result = registry.checkHealth();
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, result.get().getStatus());
        Assert.assertTrue(result.get().getDetails().contains("Health-check timed-out"));
    }

    @Test
    public void testHealthCheckException() {
        HealthRegistryImpl.resetInstance();
        HealthRegistryImpl registry = HealthRegistryImpl.getInstance();
        SampleHealthInfoProvider provider1 = new SampleHealthInfoProvider("id", HealthAspect.SEGMENT_CONTAINER, () -> {
            throw new RuntimeException("Health-check runtime exception");
        });

        Optional<HealthInfo> result = registry.checkHealth();
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, result.get().getStatus());
        Assert.assertTrue(result.get().getDetails().contains("Health-check runtime exception"));
    }

    @Test
    public void testHealthCheckWithoutResponse() {
        HealthRegistryImpl.resetInstance();
        HealthRegistryImpl registry = HealthRegistryImpl.getInstance();

        Optional<HealthInfo> result = registry.checkHealth();
        Assert.assertFalse(result.isPresent());
    }

    @Test
    public void testHealthRegistryAspectWithMajority() {
        HealthRegistryImpl.resetInstance();
        HealthRegistryImpl registry = HealthRegistryImpl.getInstance();

        Optional<HealthInfo> result = registry.checkHealth();
        Assert.assertFalse(result.isPresent());

        new SampleHealthInfoProvider("1111", HealthAspect.SEGMENT_CONTAINER, () -> new HealthInfo(HealthInfo.Status.HEALTH, "Segment-Container health-check details"));
        result = registry.checkHealth();
        Assert.assertEquals(HealthInfo.Status.HEALTH, result.get().getStatus());

        new SampleHealthInfoProvider("22222", HealthAspect.SEGMENT_CONTAINER, () -> new HealthInfo(HealthInfo.Status.UNHEALTH, "Segment-Container health-check details"));
        result = registry.checkHealth();
        Assert.assertEquals(HealthInfo.Status.HEALTH, result.get().getStatus());

        new SampleHealthInfoProvider("4444", HealthAspect.SEGMENT_CONTAINER, () -> new HealthInfo(HealthInfo.Status.UNKNOWN, "Segment-Container health-check details"));
        result = registry.checkHealth();
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, result.get().getStatus());

        new SampleHealthInfoProvider("3333", HealthAspect.SEGMENT_CONTAINER, () -> new HealthInfo(HealthInfo.Status.HEALTH, "Segment-Container health-check details"));
        result = registry.checkHealth();
        Assert.assertEquals(HealthInfo.Status.HEALTH, result.get().getStatus());
    }
}
