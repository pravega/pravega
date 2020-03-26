/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.client.tables.KeyVersion;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.apache.commons.lang3.SerializationException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyVersionImpl} class.
 */
public class KeyVersionImplTests {
    @Test
    public void testSpecialVersions() {
        Assert.assertEquals(TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion(), KeyVersion.NOT_EXISTS.asImpl().getSegmentVersion());
        Assert.assertEquals(TableSegmentKeyVersion.NO_VERSION.getSegmentVersion(), KeyVersion.NO_VERSION.asImpl().getSegmentVersion());
    }

    @Test
    public void testConstructor() {
        long version = 123L;
        long segmentId = 8946L;
        KeyVersion v = new KeyVersionImpl(segmentId, version);
        Assert.assertEquals(version, v.asImpl().getSegmentVersion());
    }

    @Test
    public void testFromString() {
        val noSegmentVersion = new KeyVersionImpl(KeyVersionImpl.NO_SEGMENT_ID, 1234L);
        val s1 = KeyVersion.fromString(noSegmentVersion.toString()).asImpl();
        Assert.assertEquals(noSegmentVersion, s1);

        val withSegmentVersion = new KeyVersionImpl(123L, 567L);
        val s2 = KeyVersion.fromString(withSegmentVersion.toString()).asImpl();
        Assert.assertEquals(withSegmentVersion, s2);

        AssertExtensions.assertThrows(
                "Invalid deserialization worked.",
                () -> KeyVersion.fromString("abc"),
                ex -> ex instanceof SerializationException);
    }
}

