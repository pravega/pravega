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

import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyVersion} class.
 */
public class KeyVersionTests {
    @Test
    public void testSpecialVersions() {
        Assert.assertEquals(TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion(), KeyVersion.NOT_EXISTS.getSegmentVersion());
        Assert.assertEquals(TableSegmentKeyVersion.NO_VERSION.getSegmentVersion(), KeyVersion.NO_VERSION.getSegmentVersion());
    }

    @Test
    public void testConstructor() {
        long version = 123L;
        String segmentName = "Segment";
        KeyVersion v = new KeyVersion(segmentName, version);
        Assert.assertEquals(segmentName, v.getSegmentName());
        Assert.assertEquals(version, v.getSegmentVersion());
    }
}

