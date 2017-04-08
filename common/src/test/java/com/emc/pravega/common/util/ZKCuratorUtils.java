/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.util;

import com.emc.pravega.testcommon.TestUtils;
import org.apache.curator.test.TestingServer;

/**
 * ZK curator test utils.
 */
public class ZKCuratorUtils {
    public static TestingServer createTestServer() throws Exception {
        System.setProperty("zookeeper.admin.serverPort", Integer.toString(TestUtils.getAvailableListenPort()));
        return new TestingServer();
    }
}
