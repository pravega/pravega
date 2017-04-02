/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.hdfs;

import org.junit.Test;

/**
 * Unit tests for the ConcatOperation class.
 */
public class ConcatOperationTests {
    /**
     * Tests a normal concatenation using various scenarios:
     * 1. Single files
     * 2. Empty files
     * 3. Multiple files across multiple epochs.
     */
    @Test
    public void testNormalOperation() throws Exception {

    }

    /**
     * Tests the scenario where the concat.next attributes result in a circular dependency.
     * Expected outcome: Exception, and no change to the file system.
     */
    @Test
    public void testCircularDependencies() throws Exception {

    }

    /**
     * Tests the scenario where the concat operation fails because of existing targets.
     * Expected outcome: Exception, and no change to the file system.
     */
    @Test
    public void testExistingTargets() throws Exception {

    }

    /**
     * Tests the scenario where the concat operation is interrupted mid-way and leaves the system in limbo.
     * Verifies that once the concat operation began, accessing the source segment will not work (FileNotFound), while
     * accessing the target segment will result in the operation being completed.
     */
    @Test
    public void testResumeAfterFailure() throws Exception {

    }
}
