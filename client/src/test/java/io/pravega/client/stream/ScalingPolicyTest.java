/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public class ScalingPolicyTest {

    @Test
    public void testScalingPolicyArguments() {
        // Check that we do not allow incorrect arguments for creating a scaling policy.
        assertThrows(RuntimeException.class, () -> ScalingPolicy.fixed(0));
        assertThrows(RuntimeException.class, () -> ScalingPolicy.byEventRate(0, 1, 1));
        assertThrows(RuntimeException.class, () -> ScalingPolicy.byDataRate(1, 0, 0));
    }
}
