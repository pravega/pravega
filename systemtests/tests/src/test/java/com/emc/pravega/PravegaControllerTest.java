/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega;

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.services.PravegaControllerService;
import com.emc.pravega.framework.services.Service;
import com.emc.pravega.framework.services.ZookeeperService;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class PravegaControllerTest {

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException if error in setup
     */
    @Environment
    public static void setup() throws MarathonException {
        Service zk = new ZookeeperService("zookeeper", 1, 1.0, 128.0);
        if (!zk.isRunning()) {
            zk.start(true);
        }
        Service con = new PravegaControllerService("controller", zk.getServiceDetails().get(0), null, 1, 0.1, 256.0);
        if (!con.isRunning()) {
                con.start(true);
        }

    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /**
     * Invoke the controller test.
     * The test fails incase controller is not running on given ports
     */

    @Test
    public void controllerTest() {
        log.debug("Start execution of controllerTest");
        Service con = new PravegaControllerService("controller", null, null, 0, 0.0, 0.0);
        List<URI> conUri = con.getServiceDetails();
        log.debug("Controller Service URI details: {} ", conUri);
        for (int i = 0; i < conUri.size(); i++) {
            assertEquals(9090, conUri.get(i).getPort());
        }
        log.debug("ControllerTest  execution completed");
    }
}
