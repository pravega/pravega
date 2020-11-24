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
import org.junit.After;
import org.junit.Test;

public class ClusterCommandsNoZKTests extends AbstractAdminCommandTest {

    @Test
    public void testGetClusterNodesCommand() throws Exception {
        // Check that all the commands handle without throwing ZK being down.
        setupUtils.close();
        TestUtils.executeCommand("cluster list-instances", state.get());
        TestUtils.executeCommand("cluster get-host-by-container 0", state.get());
        TestUtils.executeCommand("cluster list-containers", state.get());
    }

    @After
    public void tearDown() {
        state.get().close();
    }
}
