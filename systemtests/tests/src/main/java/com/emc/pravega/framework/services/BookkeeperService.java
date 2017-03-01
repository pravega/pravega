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
import mesosphere.marathon.client.model.v2.Parameter;
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
import static com.emc.pravega.framework.TestFrameworkException.Type.InternalError;

@Slf4j
public class BookkeeperService extends MarathonBasedService {

    private static final int BK_PORT = 3181;
    private final URI zkUri;
    private int instances = 3;
    private double cpu = 0.5;
    private double mem = 512.0;

    public BookkeeperService(final String id, final URI zkUri, int instances, double cpu, double mem) {
        super(id);
        this.zkUri = zkUri;
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
    }

    @Override
    public void start(final boolean wait) {
        log.info("Starting Bookkeeper Service: {}", getID());
        try {
            marathonClient.createApp(createBookieApp());
            if (wait) {
                waitUntilServiceRunning().get();
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        } catch (InterruptedException | ExecutionException ex) {
            throw new TestFrameworkException(InternalError, "Exception while " +
                    "starting Bookkeeper Service", ex);
        }
    }

    //This is a placeholder to perform clean up actions
    @Override
    public void clean() {
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
        app.setCpus(cpu);
        app.setMem(mem);
        app.setInstances(instances);
        app.setConstraints(setConstraint("hostname", "UNIQUE"));
        app.setContainer(new Container());
        app.getContainer().setType(CONTAINER_TYPE);
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage(IMAGE_PATH + "/nautilus/bookkeeper:" + PRAVEGA_VERSION);
        app.getContainer().getDocker().setNetwork(NETWORK_TYPE);
        app.getContainer().getDocker().setForcePullImage(FORCE_IMAGE);
        Collection<Volume> volumeCollection = new ArrayList<>();
        volumeCollection.add(createVolume("/bk/journal", "/mnt/journal", "RW"));
        volumeCollection.add(createVolume("/bk/index", "/mnt/index", "RW"));
        volumeCollection.add(createVolume("/bk/ledgers", "/mnt/ledgers", "RW"));
        volumeCollection.add(createVolume("/opt/dl_all/distributedlog-service/logs/", "/mnt/logs", "RW"));
        //TODO: set persistent volume size
        app.getContainer().setVolumes(volumeCollection);
        //set docker container parameters
        List<Parameter> parameterList = new ArrayList<>();
        Parameter element1 = new Parameter("env", "DLOG_EXTRA_OPTS=-Xms512m");
        parameterList.add(element1);
        app.getContainer().getDocker().setParameters(parameterList);
        app.setPorts(Arrays.asList(BK_PORT));
        app.setRequirePorts(true);
        //set env
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        Map<String, String> map = new HashMap<>();
        map.put("ZK_URL", zk);
        map.put("ZK", zk);
        map.put("bookiePort", String.valueOf(BK_PORT));
        app.setEnv(map);
        //healthchecks
        List<HealthCheck> healthCheckList = new ArrayList<>();
        healthCheckList.add(setHealthCheck(900, "TCP", false, 60, 20, 0));
        app.setHealthChecks(healthCheckList);

        return app;
    }
}
