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
import org.junit.AfterClass;
import org.junit.Test;

public class ClusterCommandsNoZKTests extends AbstractAdminCommandTest {

    @Test
    public void testGetClusterNodesCommand() throws Exception {
        // Check that all the commands handle without throwing ZK being down.
        SETUP_UTILS.stopAllServices();
        TestUtils.executeCommand("cluster list-instances", STATE.get());
        TestUtils.executeCommand("cluster get-host-by-container 0", STATE.get());
        TestUtils.executeCommand("cluster list-containers", STATE.get());
    }

    @AfterClass
    public static void tearDown() {
        STATE.get().close();
    }
}
