/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.framework.services;

import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.HealthCheck;
import mesosphere.marathon.client.model.v2.UpgradeStrategy;
import mesosphere.marathon.client.utils.MarathonException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;


@Slf4j
public class ZookeeperService extends MarathonBasedService {

    public ZookeeperService(final String id) {
        super(id);
    }

    @Override
    public void start(final boolean wait)  {
        log.info("Starting Zookeeper Service: {}", getID());
        try {
            marathonClient.createApp(createZookeeperApp());
            if (wait) {
                try {
                    waitUntilServiceRunning().get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error in wait until zookeeper service is running {}", e);
                }
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    @Override
    public void clean() {
        //TODO: Clean up to be performed after stopping the Zookeeper Service.
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
        app.setCpus(1.0);
        app.setMem(128.0);
        app.setInstances(1);
        app.setContainer(new Container());
        app.getContainer().setType("DOCKER");
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage("jplock/zookeeper:3.5.1-alpha");
        app.getContainer().getDocker().setNetwork("HOST");
        List<HealthCheck> healthCheckList = new ArrayList<>();
        HealthCheck healthCheck = new HealthCheck();
        healthCheck.setGracePeriodSeconds(900);
        healthCheck.setProtocol("TCP");
        healthCheck.setIgnoreHttp1xx(false);
        healthCheckList.add(healthCheck);
        healthCheck.setIntervalSeconds(60);
        healthCheck.setTimeoutSeconds(20);
        healthCheck.setMaxConsecutiveFailures(0);
        app.setHealthChecks(healthCheckList);
        UpgradeStrategy upgradeStrategy = new UpgradeStrategy();
        upgradeStrategy.setMaximumOverCapacity(0.0);
        upgradeStrategy.setMinimumHealthCapacity(0.0);
        app.setUpgradeStrategy(upgradeStrategy);
        return app;
    }

}
