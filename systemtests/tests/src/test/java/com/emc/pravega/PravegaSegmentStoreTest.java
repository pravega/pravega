/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega;

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.services.BookkeeperService;
import com.emc.pravega.framework.services.PravegaSegmentStoreService;
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
public class PravegaSegmentStoreTest {

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
        Service bk = new BookkeeperService("bookkeeper", zk.getServiceDetails().get(0), 3, 0.5, 512.0);
        if (!bk.isRunning()) {
            bk.start(true);
        }
       Service seg = new PravegaSegmentStoreService("segmentstore", zk.getServiceDetails().get(0), 1, 1.0, 512.0);
        if (!seg.isRunning()) {
            seg.start(true);
        }
    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /**
     * Invoke the segmentstore test.
     * The test fails incase segmentstore is not running on given port.
     */

    @Test
    public void segmentStoreTest() {
        log.debug("Start execution of segmentStoreTest");
        Service seg = new PravegaSegmentStoreService("segmentstore", null, 0, 0.0, 0.0);
        List<URI> segUri = seg.getServiceDetails();
        log.debug("Pravega SegmentStore Service URI details: {} ", segUri);
        for (int i = 0; i < segUri.size(); i++) {
            assertEquals(12345, segUri.get(i).getPort());
        }
        log.debug("SegmentStoreTest  execution completed");
    }
}
