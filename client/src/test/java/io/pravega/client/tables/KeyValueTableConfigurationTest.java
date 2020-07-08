/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the {@link KeyValueTableConfiguration} class.
 */
public class KeyValueTableConfigurationTest {
    @Test
    public void testBuilder() {
        val c = KeyValueTableConfiguration.builder().partitionCount(4).build();
        Assert.assertEquals(4, c.getPartitionCount());
    }
}
