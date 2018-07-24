/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.Ignore;
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
        Service zk = Utils.createZookeeperService();
        if (!zk.isRunning()) {
            zk.start(true);
        }
        Service con = Utils.createPravegaControllerService(zk.getServiceDetails().get(0));
        if (!con.isRunning()) {
            con.start(true);
        }
    }

    /**
     * Invoke the controller test.
     * The test fails incase controller is not running on given ports
     */

    @Ignore
    @Test(timeout = 5 * 60 * 1000)
    public void controllerTest() {
        log.debug("Start execution of controllerTest");
        Service con = Utils.createPravegaControllerService(null);
        List<URI> conUri = con.getServiceDetails();
        log.debug("Controller Service URI details: {} ", conUri);
        for (int i = 0; i < conUri.size(); i++) {
            int port = conUri.get(i).getPort();
            boolean boolPort =  Utils.DOCKER_BASED ? (port == 9090 ||  port == 9091) : (port == 9092 || port == 10080);
            assertEquals(true, boolPort);
        }
        log.debug("ControllerTest  execution completed");
    }
}
