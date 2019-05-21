/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.shared.segment;

import org.junit.Assert;
import org.junit.Test;

public class ScaleTypeTests {

    @Test
    public void testToValue() {
        ScaleType noScaling = ScaleType.NoScaling;
        ScaleType throughput = ScaleType.Throughput;
        ScaleType eventRate = ScaleType.EventRate;

        Assert.assertTrue(noScaling.name().equals(ScaleType.NoScaling.name()));
        Assert.assertTrue(throughput.name().equals(ScaleType.Throughput.name()));
        Assert.assertTrue(eventRate.name().equals(ScaleType.EventRate.name()));

        Assert.assertTrue(noScaling.getValue() == ScaleType.NoScaling.getValue());
        Assert.assertTrue(throughput.getValue() == ScaleType.Throughput.getValue());
        Assert.assertTrue(eventRate.getValue() == ScaleType.EventRate.getValue());
    }

    @Test
    public void testFromValue() {
        ScaleType noScaling = ScaleType.fromValue(ScaleType.NoScaling.getValue());
        ScaleType throughput = ScaleType.fromValue(ScaleType.Throughput.getValue());
        ScaleType eventRate = ScaleType.fromValue(ScaleType.EventRate.getValue());

        Assert.assertTrue(noScaling.name().equals(ScaleType.NoScaling.name()));
        Assert.assertTrue(throughput.name().equals(ScaleType.Throughput.name()));
        Assert.assertTrue(eventRate.name().equals(ScaleType.EventRate.name()));

        Assert.assertTrue(noScaling.getValue() == ScaleType.NoScaling.getValue());
        Assert.assertTrue(throughput.getValue() == ScaleType.Throughput.getValue());
        Assert.assertTrue(eventRate.getValue() == ScaleType.EventRate.getValue());
    }

}
