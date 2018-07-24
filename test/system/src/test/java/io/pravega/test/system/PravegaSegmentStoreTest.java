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
public class PravegaSegmentStoreTest {

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
        Service bk = Utils.createBookkeeperService(zk.getServiceDetails().get(0));
        if (!bk.isRunning()) {
            bk.start(true);
        }

        Service con = Utils.createPravegaControllerService(zk.getServiceDetails().get(0));
        if (!con.isRunning()) {
            con.start(true);
        }
        Service seg = Utils.createPravegaSegmentStoreService(zk.getServiceDetails().get(0), con.getServiceDetails().get(0));
        if (!seg.isRunning()) {
            seg.start(true);
        }
    }

    /**
     * Invoke the segmentstore test.
     * The test fails incase segmentstore is not running on given port.
     */

    @Ignore
    @Test(timeout = 5 * 60 * 1000)
    public void segmentStoreTest() {
        log.debug("Start execution of segmentStoreTest");
        Service seg = Utils.createPravegaSegmentStoreService(null, null);
        List<URI> segUri = seg.getServiceDetails();
        log.debug("Pravega SegmentStore Service URI details: {} ", segUri);
        for (int i = 0; i < segUri.size(); i++) {
            assertEquals(12345, segUri.get(i).getPort());
        }
        log.debug("SegmentStoreTest  execution completed");
    }
}
