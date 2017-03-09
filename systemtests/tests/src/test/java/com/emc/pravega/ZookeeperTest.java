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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import static org.apache.curator.framework.imps.CuratorFrameworkState.STARTED;
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
        Service zk = new ZookeeperService("zookeeper", 1, 1.0, 3072.0);
        if (!zk.isRunning()) {
            zk.start(true);
        }
    }

    /**
     * Invoke the zookeeper test, ensure zookeeper can be accessed.
     * The test fails incase zookeeper cannot be accessed
     *
     */
    @Test
    public void zkTest() {
        log.info("Start execution of ZkTest");
        Service zk = new ZookeeperService("zookeeper", 0, 0.0, 0.0);
        URI zkUri = zk.getServiceDetails().get(0);
        CuratorFramework curatorFrameworkClient =
                CuratorFrameworkFactory.newClient(zkUri.getHost()+":"+2181, new RetryOneTime(5000));
        curatorFrameworkClient.start();
        log.info("CuratorFramework status {} ", curatorFrameworkClient.getState());
        assertEquals("Connection to zk client ", STARTED, curatorFrameworkClient.getState());
        log.info("Zookeeper Service URI : {} ", zkUri);
        log.info("ZkTest  execution completed");
    }
}
