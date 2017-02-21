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
import mesosphere.marathon.client.utils.MarathonException;

import java.util.Arrays;

@Slf4j
public class RedisService extends MarathonBasedService {

    public RedisService(final String id) {
        super(id);
    }

    @Override
    public void start(final boolean wait) {
        log.info("Starting service: {}", getID());
        try {
            marathonClient.createApp(createRedisApp());
            if (wait) {
                waitUntilServiceRunning();
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping service: {}", getID());
        try {
            marathonClient.deleteApp("redisservice");
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    @Override
    public void clean() {
        //TODO: Clean up to be performed after stopping the service.
    }

    private App createRedisApp() {
        App app = new App();
        app.setId(this.id);
        app.setCpus(1.0);
        app.setMem(1024.0);
        app.setInstances(1);
        app.setPorts(Arrays.asList(20000));
        app.setCmd("bash /opt/bootstrap.sh");
        app.setContainer(new Container());
        app.getContainer().setType("DOCKER");
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage("gpetr/redis4mesos");
        app.getContainer().getDocker().setNetwork("HOST");
        return app;
    }
}
