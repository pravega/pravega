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
     * @throws MarathonException  if error in setup
     */
    @Environment
    public static void setup() throws MarathonException {

        AuthEnabledMetronomeClient.deleteAllJobs(getClient());
        Service zk = new ZookeeperService("zookeeper");
        if (!zk.isRunning()) {
            zk.start(true);
        }
        Service bk = new BookkeeperService("bookkeeper", zk.getServiceDetails().get(0));
        if (!bk.isRunning()) {
            bk.start(true);
        }

    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException, ExecutionException, TimeoutException {
        // This is the placeholder to perform any operation on the services before executing the system tests
    }

    /**
     * Invoke the producer test, ensure we are able to produce 100 messages to the stream.
     * The test fails incase of exceptions while writing to the stream.
     */

    @Test
    public void bkPingTest() {
        log.debug("Start execution of bkPingTest");
        Service bk = new BookkeeperService("bookkeeper", null);
        List<URI> bkUri = bk.getServiceDetails();
        //TODO: validate bk uri details
        log.debug("bk Service URI details: {} ", bkUri);
        log.debug("bkPingTest  execution completed");
    }
}
