/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.test.system;

import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.ZookeeperService;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;
import java.util.List;
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
        Service zk = new ZookeeperService("zookeeper");
        if (!zk.isRunning()) {
            zk.start(true);
        }
        Service con = new PravegaControllerService("controller", zk.getServiceDetails().get(0));
        if (!con.isRunning()) {
            con.start(true);
        }
    }

    /**
     * Invoke the controller test.
     * The test fails incase controller is not running on given ports
     */

    @Test
    public void controllerTest() {
        log.debug("Start execution of controllerTest");
        Service con = new PravegaControllerService("controller", null, 0, 0.0, 0.0);
        List<URI> conUri = con.getServiceDetails();
        log.debug("Controller Service URI details: {} ", conUri);
        for (int i = 0; i < conUri.size(); i++) {
            int port = conUri.get(i).getPort();
            boolean boolPort = port == 9092 || port == 10080;
            assertEquals(true, boolPort);
        }
        log.debug("ControllerTest  execution completed");
    }
}
