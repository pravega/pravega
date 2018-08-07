/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import io.pravega.test.common.AssertExtensions;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.junit.Test;

import java.io.Serializable;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.*;

/**
 * Unit tests for SerializationHelpers Class.
 */
public class SerializationHelpersTest {

    @Test
    public void serializeTest() throws SerializationException {
        TestData v1 = new TestData("v1");
        String base64V1 = SerializationHelpers.serializeBase64(v1);
        assertEquals(new TestData("v1"), SerializationHelpers.deserializeBase64(base64V1));
    }

    @Test
    public void serializeInvalidInputTest() {
        assertThrows(NullPointerException.class, () -> SerializationHelpers.serializeBase64(null));
        assertThrows(NullPointerException.class, () -> SerializationHelpers.deserializeBase64(null));
        assertThrows(IllegalArgumentException.class, () -> SerializationHelpers.deserializeBase64(""));
        assertThrows(IllegalArgumentException.class, () -> SerializationHelpers.deserializeBase64("Invalid base64 String"));
        //test with incomplete base64 data.
        assertThrows(SerializationException.class, () -> SerializationHelpers.deserializeBase64("H4sIAAAAAAAAAFvz"));
    }

    @EqualsAndHashCode
    @AllArgsConstructor
    private static class TestData implements Serializable {
        private String name;
    }
}
