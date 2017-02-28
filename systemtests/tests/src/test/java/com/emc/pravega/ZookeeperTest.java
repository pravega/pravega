/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega;

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ZookeeperTest {

    /**
     * This is used to setup the various services required by the system test framework.
     * @throws MarathonException if error in setup
     */
    @Environment
    public static void setup() throws MarathonException {
        Service zk = new ZookeeperService("zookeeper", 1, 1.0, 128.0);
        if (!zk.isRunning()) {
            zk.start(true);
        }
    }

    /**
     * Invoke the zookeeper test, ensure zookeeper can be accessed.
     * The test fails incase zookeeper cannot be accessed
     */
    @Test
    public void zkTest() {
        log.info("Start execution of ZkTest");
        Service zk = new ZookeeperService("zookeeper", 0, 0.0, 0.0);
        URI zkUri = zk.getServiceDetails().get(0);
        CuratorFramework curatorFramework =
                CuratorFrameworkFactory.newClient(zkUri.toString(), new RetryOneTime(1));
        curatorFramework.start();
        CuratorZookeeperClient zkClient = curatorFramework.getZookeeperClient();
        assertEquals(true, zkClient.isConnected());
        log.info("Zookeeper Service URI : {} ", zkUri);
        log.info("ZkTest  execution completed");
    }
}
