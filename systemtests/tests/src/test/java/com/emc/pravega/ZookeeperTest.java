/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega;

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.metronome.AuthEnabledMetronomeClient;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.emc.pravega.framework.metronome.AuthEnabledMetronomeClient.getClient;
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
        AuthEnabledMetronomeClient.deleteAllJobs(getClient());
        Service zk = new ZookeeperService("zookeeper");
        if (!zk.isRunning()) {
            if (!zk.isStaged()) {
                zk.start(true);
            }
        }
    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /**
     * Invoke the zookeeper test, ensure zookeeper can be accessed.
     * The test fails incase zookeeper cannot be accessed
     */
    @Test
    public void zkTest() {
        log.debug("Start execution of ZkTest");
        Service zk = new ZookeeperService("zookeeper");
        URI zkUri = zk.getServiceDetails().get(0);
        CuratorFramework curatorFramework =
                CuratorFrameworkFactory.newClient(zkUri.toString(), new RetryOneTime(1));
        curatorFramework.start();
        CuratorZookeeperClient zkClient = curatorFramework.getZookeeperClient();
        assertEquals(true, zkClient.isConnected());
        log.debug("Zookeeper Service URI : {} ", zkUri);
        log.debug("ZkTest  execution completed");
    }

}
