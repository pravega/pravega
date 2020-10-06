/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.utils;

import io.pravega.test.common.AssertExtensions;
import io.pravega.test.integration.utils.SetupUtils;
import org.junit.Test;

public class ZKHelperTest {

    @Test
    public void testFaultyZKScenario() throws Exception {
        AssertExtensions.assertThrows(ZKConnectionFailedException.class, () -> ZKHelper.create("wrongURL:1234", "wrongCluster"));
        SetupUtils setupUtils = new SetupUtils();
        setupUtils.startAllServices();
        ZKHelper zkHelper = ZKHelper.create(setupUtils.getZkTestServer().getConnectString(), "pravega");
        // Now, stop all services.
        setupUtils.stopAllServices();
        // Check the behavior of ZKHelper calls when ZK is down.
        zkHelper.getChild("/testPath");
        zkHelper.getData("/testPath");
    }

}
