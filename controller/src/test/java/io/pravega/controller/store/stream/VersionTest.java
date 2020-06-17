/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VersionTest {
    @Test
    public void intVersionSerializationTest() {
        Version.IntVersion version = new Version.IntVersion(10);
        assertEquals(version, Version.IntVersion.fromBytes(version.toBytes()));
    }
    
    @Test 
    public void unSupportedVersionException() {
        TestVersion version = new TestVersion();
        AssertExtensions.assertThrows(UnsupportedOperationException.class, version::asIntVersion);
    }

    @Test
    public void testAsVersionType() {
        Version version = new Version.IntVersion(100);
        Version.IntVersion intVersion = version.asIntVersion();
        assertEquals(100, intVersion.getIntValue());
    }
    
    static class TestVersion extends Version.UnsupportedVersion {
    }
}
