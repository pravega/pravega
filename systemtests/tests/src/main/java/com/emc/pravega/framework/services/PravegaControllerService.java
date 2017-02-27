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
import mesosphere.marathon.client.utils.MarathonException;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.emc.pravega.framework.TestFrameworkException.Type.InternalError;

/**
 * Controller Service.
 */
@Slf4j
public class PravegaControllerService extends MarathonBasedService {

    private final URI zkUri;
    private final URI segUri;
    private int instances = 1;
    private double cpu = 0.1;
    private double mem = 256;

    public PravegaControllerService(final String id, final URI zkUri, final URI segUri, int instances, double cpu, double mem) {
        super(id);
        this.zkUri = zkUri;
        this.segUri = segUri;
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
    }

    /**
     * Start the controller service.
     *
     * @param wait boolean to wait until service is running
     */
    @Override
    public void start(final boolean wait) {
        log.debug("Starting service: {}", getID());
        try {

            marathonClient.createApp(createPravegaControllerApp());
            if (wait) {
                try {
                    waitUntilServiceRunning().get();
                } catch (InterruptedException | ExecutionException ex) {
                    throw  new TestFrameworkException(InternalError, "Exception while " +
                            "starting Pravega Controller Service", ex);
                }
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    /**
     * Method to stop the service.
     */
    @Override
    public void stop() {
        log.debug("Stopping  pravega controller service: {}", getID());
        try {
            marathonClient.deleteApp(getID());
        } catch (MarathonException e) {
            handleMarathonException(e);
        }
    }

    /**
     * Cleanup after service is stopped.
     * This is a placeholder to perform actions of cleaning up configuration of controller in zk
     */
    @Override
    public void clean() {
    }

    /**
     * To configure the controller app.
     *
     * @return App instance of marathon app
     */
    private App createPravegaControllerApp() {

        App app = new App();
        app.setId(this.id);
        app.setCpus(cpu);
        app.setMem(mem);
        app.setInstances(instances);
        app.setConstraints(setConstraint("hostname", "UNIQUE"));
        app.setContainer(new Container());
        app.getContainer().setType(containerType);
        app.getContainer().setDocker(new Docker());
        //TODO: change tag to latest
        app.getContainer().getDocker().setImage(imagePath + "pravega-controller:" + pravegaVersion);
        app.getContainer().getDocker().setNetwork(networkType);
        app.getContainer().getDocker().setForcePullImage(forceImage);
        //set docker container parameters
        String zk = zkUri.getHost() + ":" + zkPort;
        List<Parameter> parameterList = new ArrayList<>();
        Parameter element1 = new Parameter("env", "SERVER_OPTS=\"-DZK_URL=" + zk + "\\");
        parameterList.add(element1);
        app.getContainer().getDocker().setParameters(parameterList);
        //set port
        app.setPorts(Arrays.asList(controllerPort, restPort));
        app.setRequirePorts(true);
        List<HealthCheck> healthCheckList = new ArrayList<HealthCheck>();
        healthCheckList.add(setHealthCheck(900, "TCP", false, 60, 20, 0));
        app.setHealthChecks(healthCheckList);
        //set env
        Map<String, String> map = new HashMap<>();
        map.put("ZK_URL", zk);
        map.put("SERVICE_HOST_IP", segUri.getHost());
        map.put("REST_SERVER_PORT", String.valueOf(restPort));
        app.setEnv(map);
        return app;
    }
}
