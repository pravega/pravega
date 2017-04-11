/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.shared.testcommon;

import com.emc.pravega.shared.TestUtils;
import lombok.Getter;
import org.apache.curator.test.TestingServer;

/**
 * ZK curator TestingServer starter.
 */
public class TestingServerStarter {

    @Getter
    private final int adminServerPort;

    public TestingServerStarter() {
        adminServerPort = TestUtils.getAvailableListenPort();
    }

    public TestingServer start() throws Exception {
        System.setProperty("zookeeper.admin.serverPort", Integer.toString(adminServerPort));
        return new TestingServer();
    }
}
