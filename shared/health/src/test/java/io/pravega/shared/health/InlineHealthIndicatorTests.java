/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import org.junit.Assert;
import org.junit.Test;

public class InlineHealthIndicatorTests {

    private static final String INLINE_DETAIL_KEY = "inline-key";

    private static final String INLINE_DETAIL_VAL = "inline-value";

    @Test
    public void testHealthWithoutDetails() {
        InlineHealthIndicator indicator = new InlineHealthIndicator("inline-indicator", (builder, provider) -> builder.status(Status.UP));
        Health health = indicator.health();
        Assert.assertEquals("Indicator should report an 'UP' Status", health.getStatus(), Status.UP);
    }

    @Test
    public void testHealthWithDetails() {
        InlineHealthIndicator indicator = new InlineHealthIndicator("inline-indicator", (builder, provider) -> {
            builder.status(Status.UP);
            provider.add(INLINE_DETAIL_KEY, () -> INLINE_DETAIL_VAL);
        });
        Health health = indicator.health(true);
        Assert.assertEquals("Indicator should report an 'UP' Status", Status.UP, health.getStatus());
        Assert.assertEquals("Indicator should provide KEY -> VAL detail mapping.",
                INLINE_DETAIL_VAL,
                health.getDetails().get(INLINE_DETAIL_KEY));
    }


    @Test
    public void testEmptyHealthCheckBody() {
        InlineHealthIndicator indicator = new InlineHealthIndicator("inline-indicator", (builder, provider) -> { });
        Health health = indicator.health();
        Assert.assertEquals("Indicator should indicate an 'UNKNOWN' Status.", Status.UNKNOWN, health.getStatus());
    }

    @Test
    public void testIndicatorThrows() {
        InlineHealthIndicator indicator = new InlineHealthIndicator("inline-indicator", (builder, provider) -> {
            throw new RuntimeException();
        });
        Health health = indicator.health();
        Assert.assertEquals("HealthIndicator should have a 'DOWN' Status.", Status.DOWN, health.getStatus());
        Assert.assertTrue("HealthIndicator should be not be marked ready OR alive.", !health.isAlive() && !health.isReady());
    }


}
