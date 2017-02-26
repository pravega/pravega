/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega;

import com.emc.pravega.framework.Environment;
import com.emc.pravega.framework.SystemTestRunner;
import com.emc.pravega.framework.metronome.AuthEnabledMetronomeClient;
import com.emc.pravega.framework.services.BookkeeperService;
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

import static com.emc.pravega.framework.metronome.AuthEnabledMetronomeClient.getClient;

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

        AuthEnabledMetronomeClient.deleteAllJobs(getClient());
        Service zk = new ZookeeperService("zookeeper");
        if (!zk.isRunning()) {
            if (!zk.isStaged()) {
                zk.start(true);
            }
        }
        Service bk = new BookkeeperService("bookkeeper", zk.getServiceDetails().get(0), 3, 0.5, 512.0);
        if (!bk.isRunning()) {
            if (!bk.isStaged()) {
                bk.start(true);
            }
        }

    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        // This is the placeholder to perform any operation on the services before executing the system tests
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
            if (bkUri.get(i).getPort() == 3181) {
                log.debug("bookies running on 3181");
            }  else {
                log.error("bookies not running on 3181");
            }
            System.exit(0);
        }
        log.debug("BkTest  execution completed");
    }
}
