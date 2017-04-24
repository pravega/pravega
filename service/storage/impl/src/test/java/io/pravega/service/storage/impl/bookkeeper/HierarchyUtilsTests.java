/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import io.pravega.service.storage.impl.bookkeeper.HierarchyUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for HierarchyUtils class.
 */
public class HierarchyUtilsTests {
    /**
     * Tests the GetPath method.
     */
    @Test(timeout = 5000)
    public void testGetPath() {
        // Empty root
        Assert.assertEquals("/1234", HierarchyUtils.getPath("", 1234, 0));

        // Flat hierarchy.
        Assert.assertEquals("/root/1234", HierarchyUtils.getPath("/root", 1234, 0));
        Assert.assertEquals("/root/0", HierarchyUtils.getPath("/root", 0, 0));

        // Sub-length hierarchy.
        Assert.assertEquals("/root/4/1234", HierarchyUtils.getPath("/root", 1234, 1));
        Assert.assertEquals("/root/4/3/1234", HierarchyUtils.getPath("/root", 1234, 2));
        Assert.assertEquals("/root/4/3/2/1234", HierarchyUtils.getPath("/root", 1234, 3));
        Assert.assertEquals("/root/4/3/2/1/1234", HierarchyUtils.getPath("/root", 1234, 4));

        // Above-length hierarchy.
        Assert.assertEquals("/root/4/3/2/1/0/1234", HierarchyUtils.getPath("/root", 1234, 5));
        Assert.assertEquals("/root/4/3/2/1/0/0/1234", HierarchyUtils.getPath("/root", 1234, 6));
    }
}
