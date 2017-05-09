/*
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
import io.pravega.test.system.framework.services.BookkeeperService;
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
public class BookkeeperTest {
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
        Service bk = new BookkeeperService("bookkeeper", zk.getServiceDetails().get(0));
        if (!bk.isRunning()) {
            bk.start(true);
        }
    }

    /**
     * Invoke the bookkeeper test.
     * The test fails incase bookkeeper is not running on given port.
     */

    @Test
    public void bkTest() {
        log.debug("Start execution of bkTest");
        Service bk = new BookkeeperService("bookkeeper", null, 0, 0.0, 0.0);
        List<URI> bkUri = bk.getServiceDetails();
        log.debug("Bk Service URI details: {} ", bkUri);
        for (int i = 0; i < bkUri.size(); i++) {
            assertEquals(3181, bkUri.get(i).getPort());
        }
        log.debug("BkTest  execution completed");
    }
}
