/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework.services;

import com.emc.pravega.framework.TestFrameworkException;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.HealthCheck;
import mesosphere.marathon.client.utils.MarathonException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.emc.pravega.framework.TestFrameworkException.Type.InternalError;
import static com.emc.pravega.framework.Utils.isSkipServiceInstallationEnabled;

@Slf4j
public class ZookeeperService extends MarathonBasedService {

    private static final String ZK_IMAGE = "jplock/zookeeper:3.5.1-alpha";
    private int instances = 1;
    private double cpu = 1.0;
    private double mem = 128.0;

    public ZookeeperService(final String id, int instances, double cpu, double mem) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(isSkipServiceInstallationEnabled() ? "/pravega/exhibitor" : id);
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
    }

    @Override
    public void start(final boolean wait) {
        deleteApp("/pravega/exhibitor");
        log.info("Starting Zookeeper Service: {}", getID());
        try {
            marathonClient.createApp(createZookeeperApp());
            if (wait) {
                waitUntilServiceRunning().get();
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        } catch (InterruptedException | ExecutionException ex) {
            throw new TestFrameworkException(InternalError, "Exception while " +
                    "starting Zookeeper Service", ex);
        }
    }

        //This is a placeholder to perform clean up actions
        @Override
        public void clean() {
        }

        @Override
        public void stop() {
            log.info("Stopping Zookeeper Service : {}", getID());
            try {
                marathonClient.deleteApp(getID());
            } catch (MarathonException e) {
                handleMarathonException(e);
            }
        }

    private App createZookeeperApp() {
        App app = new App();
        app.setId(this.id);
        app.setCpus(cpu);
        app.setMem(mem);
        app.setInstances(instances);
        app.setContainer(new Container());
        app.getContainer().setType(CONTAINER_TYPE);
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage(ZK_IMAGE);
        app.getContainer().getDocker().setNetwork(NETWORK_TYPE);
        List<HealthCheck> healthCheckList = new ArrayList<>();
        healthCheckList.add(setHealthCheck(900, "TCP", false, 60, 20, 0));
        app.setHealthChecks(healthCheckList);

        return app;
    }
}
