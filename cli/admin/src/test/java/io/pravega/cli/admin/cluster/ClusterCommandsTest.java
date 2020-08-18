/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.cluster;

import io.pravega.cli.admin.AbstractAdminCommandTest;
import io.pravega.cli.admin.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ClusterCommandsTest extends AbstractAdminCommandTest {

    @Test
    public void testGetClusterNodesCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("cluster list-instances", STATE.get());
        Assert.assertTrue(commandResult.contains("controllers"));
        Assert.assertNotNull(GetClusterNodesCommand.descriptor());
    }

    //@Test
    public void testGetSegmentStoreByContainerCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("cluster get-host-by-container 0", STATE.get());
        Assert.assertTrue(commandResult.contains("owner_segment_store"));
        Assert.assertNotNull(GetSegmentStoreByContainerCommand.descriptor());
    }

    //@Test
    public void testListContainersCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("cluster list-containers", STATE.get());
        Assert.assertTrue(commandResult.contains("segment_store_container_map"));
        Assert.assertNotNull(ListContainersCommand.descriptor());
    }
}
