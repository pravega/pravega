/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.framework.services;

import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.HealthCheck;
import mesosphere.marathon.client.model.v2.Parameter;
import mesosphere.marathon.client.model.v2.UpgradeStrategy;
import mesosphere.marathon.client.model.v2.Volume;
import mesosphere.marathon.client.utils.MarathonException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class BookkeeperService extends MarathonBasedService {

    private final URI zkUri;

    public BookkeeperService(final String id, final URI zkUri) {
        super(id);
        this.zkUri = zkUri;
    }


    @Override
    public void start(final boolean wait) {
        log.info("Starting Bookkeeper Service: {}", getID());
        try {
            marathonClient.createApp(createBookieApp());

            if (wait) {
                try {
                    waitUntilServiceRunning().get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error in wait until bookkeeper service is running {}", e);
                }
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    @Override
    public void clean() {
        //TODO: Clean up to be performed after stopping the Bookie Service.
    }

    @Override
    public void stop() {
        log.info("Stopping Bookkeeper Service : {}", getID());
        try {
            marathonClient.deleteApp(getID());
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    private App createBookieApp() {

        App app = new App();
        app.setId(this.id);
        app.setCpus(0.5);
        app.setMem(512.0);
        app.setInstances(3);
        List<List<String>> listString = new ArrayList<>();
        List<String> list = new ArrayList<>();
        list.add("hostname");
        list.add("UNIQUE");
        listString.add(list);
        app.setConstraints(listString);
        app.setContainer(new Container());
        app.getContainer().setType("DOCKER");
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage("asdrepo.isus.emc.com:8103/nautilus/bookkeeper:0.0-1111.2b562cb");
        app.getContainer().getDocker().setNetwork("HOST");
        app.getContainer().getDocker().setForcePullImage(true);
        Collection<Volume> volumeCollection = new ArrayList<>();
        //journal
        Volume volume1 = new Volume();
        volume1.setContainerPath("/bk/journal");
        volume1.setHostPath("/mnt/journal");
        volume1.setMode("RW");
        //index
        Volume volume2 = new Volume();
        volume2.setContainerPath("/bk/index");
        volume2.setHostPath("/mnt/index");
        volume2.setMode("RW");
        //ledgers
        Volume volume3 = new Volume();
        volume3.setContainerPath("/bk/ledgers");
        volume3.setHostPath("/mnt/ledgers");
        volume3.setMode("RW");
        //dl logs
        Volume volume4 = new Volume();
        volume4.setContainerPath("/opt/dl_all/distributedlog-service/logs/");
        volume4.setHostPath("/mnt/logs");
        volume4.setMode("RW");
        //TODO: set persistent volume size
        volumeCollection.add(volume1);
        volumeCollection.add(volume2);
        volumeCollection.add(volume3);
        volumeCollection.add(volume4);
        app.getContainer().setVolumes(volumeCollection);
        //set docker container parameters
        List<Parameter> parameterList = new ArrayList<>();
        Parameter element1 = new Parameter("env", "DLOG_EXTRA_OPTS=-Xms512m");
        parameterList.add(element1);
        app.getContainer().getDocker().setParameters(parameterList);
        app.setPorts(Arrays.asList(3181));
        app.setRequirePorts(true);
        //set env
        String zk = zkUri.getHost() + ":2181";
        Map<String, String> map = new HashMap<>();
        map.put("ZK_URL", zk);
        map.put("ZK", zk);
        map.put("bookiePort", "3181");
        app.setEnv(map);
        //healthchecks
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
        //upgrade strategy
        UpgradeStrategy upgradeStrategy = new UpgradeStrategy();
        upgradeStrategy.setMaximumOverCapacity(0.0);
        upgradeStrategy.setMinimumHealthCapacity(0.0);
        app.setUpgradeStrategy(upgradeStrategy);
        return app;
    }

}
