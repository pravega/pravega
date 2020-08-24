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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HealthInfoAggregationRulesTest {

    @Test
    public void testMajority() {
        List<HealthInfo> infos = new LinkedList<>();
        Assert.assertFalse(HealthInfoAggregationRules.majority(infos).isPresent());

        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.HEALTH, HealthInfoAggregationRules.majority(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNHEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.majority(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNKNOWN, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.majority(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""), new HealthInfo(HealthInfo.Status.HEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.HEALTH, HealthInfoAggregationRules.majority(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""), new HealthInfo(HealthInfo.Status.UNHEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.HEALTH, HealthInfoAggregationRules.majority(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""), new HealthInfo(HealthInfo.Status.UNKNOWN, ""));
        Assert.assertEquals(HealthInfo.Status.HEALTH, HealthInfoAggregationRules.majority(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNHEALTH, ""), new HealthInfo(HealthInfo.Status.UNKNOWN, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.majority(infos).get().getStatus());
    }

    @Test
    public void testSingleOrNone() {
        List<HealthInfo> infos = new LinkedList<>();
        Assert.assertFalse(HealthInfoAggregationRules.singleOrNone(infos).isPresent());

        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.HEALTH, HealthInfoAggregationRules.singleOrNone(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNHEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.singleOrNone(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNKNOWN, ""));
        Assert.assertEquals(HealthInfo.Status.UNKNOWN, HealthInfoAggregationRules.singleOrNone(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNKNOWN, ""), new HealthInfo(HealthInfo.Status.HEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.UNKNOWN, HealthInfoAggregationRules.singleOrNone(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNHEALTH, ""), new HealthInfo(HealthInfo.Status.UNKNOWN, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.singleOrNone(infos).get().getStatus());
    }

    @Test
    public void testOneVeto() {
        List<HealthInfo> infos = new LinkedList<>();
        Assert.assertFalse(HealthInfoAggregationRules.oneVeto(infos).isPresent());

        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.HEALTH, HealthInfoAggregationRules.oneVeto(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNHEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.oneVeto(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNKNOWN, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.oneVeto(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""), new HealthInfo(HealthInfo.Status.HEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.HEALTH, HealthInfoAggregationRules.oneVeto(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNHEALTH, ""), new HealthInfo(HealthInfo.Status.UNKNOWN, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.oneVeto(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.UNHEALTH, ""), new HealthInfo(HealthInfo.Status.HEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.oneVeto(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""), new HealthInfo(HealthInfo.Status.UNKNOWN, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.oneVeto(infos).get().getStatus());

        infos = new LinkedList<>();
        Collections.addAll(infos, new HealthInfo(HealthInfo.Status.HEALTH, ""), new HealthInfo(HealthInfo.Status.UNKNOWN, ""), new HealthInfo(HealthInfo.Status.HEALTH, ""));
        Assert.assertEquals(HealthInfo.Status.UNHEALTH, HealthInfoAggregationRules.oneVeto(infos).get().getStatus());
    }
}
