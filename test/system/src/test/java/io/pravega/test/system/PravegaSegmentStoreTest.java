/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.test.system;

import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.BookkeeperService;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.PravegaSegmentStoreService;
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
public class PravegaSegmentStoreTest {

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
        Service con = new PravegaControllerService("controller", zk.getServiceDetails().get(0));
        if (!con.isRunning()) {
            con.start(true);
        }
        Service seg = new PravegaSegmentStoreService("segmentstore", zk.getServiceDetails().get(0), con.getServiceDetails().get(0));
        if (!seg.isRunning()) {
            seg.start(true);
        }
    }

    /**
     * Invoke the segmentstore test.
     * The test fails incase segmentstore is not running on given port.
     */

    @Test
    public void segmentStoreTest() {
        log.debug("Start execution of segmentStoreTest");
        Service seg = new PravegaSegmentStoreService("segmentstore", null, null,  0, 0.0, 0.0);
        List<URI> segUri = seg.getServiceDetails();
        log.debug("Pravega SegmentStore Service URI details: {} ", segUri);
        for (int i = 0; i < segUri.size(); i++) {
            assertEquals(12345, segUri.get(i).getPort());
        }
        log.debug("SegmentStoreTest  execution completed");
    }
}
