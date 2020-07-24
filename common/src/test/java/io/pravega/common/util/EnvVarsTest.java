/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EnvVarsTest {

    @Test
    public void testReadValue() {
        assertEquals(32, EnvVars.readIntegerFromString("32", "testReadValue", 500));
    }

    @Test
    public void testValueNotThere() {
        assertEquals(500, EnvVars.readIntegerFromString(null, "testValueNotThere", 500));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseFailed() {
        assertEquals(500, EnvVars.readIntegerFromString("sdg", "testParseFailed", 500));
    }

}
