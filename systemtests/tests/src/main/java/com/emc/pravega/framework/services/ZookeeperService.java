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
import static com.emc.pravega.framework.TestFrameworkException.Type.InternalError;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ZookeeperService extends MarathonBasedService {

    private int instances = 1;
    private double cpu = 1.0;
    private double mem = 128.0;


    public ZookeeperService(final String id, int instances, double cpu, double mem) {
        super(id);
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
    }

    @Override
    public void start(final boolean wait) {

        log.info("Starting Zookeeper Service: {}", getID());
        try {
            marathonClient.createApp(createZookeeperApp());
            if (wait) {
                try {
                    waitUntilServiceRunning().get();
                } catch (InterruptedException | ExecutionException ex) {
                    throw  new TestFrameworkException(InternalError, "Exception while " +
                            "starting Zookeeper Service", ex);
                }
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    //This is a placeholder to perform actions related to  cleaning up of zk
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
        app.getContainer().setType(containerType);
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage(zkImage);
        app.getContainer().getDocker().setNetwork(networkType);
        List<HealthCheck> healthCheckList = new ArrayList<>();
        healthCheckList.add(setHealthCheck(900, "TCP", false, 60, 20, 0));
        app.setHealthChecks(healthCheckList);
        return app;
    }

}
