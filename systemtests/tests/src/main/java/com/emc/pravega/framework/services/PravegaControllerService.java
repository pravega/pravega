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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import static com.emc.pravega.framework.TestFrameworkException.Type.InternalError;
import static com.emc.pravega.framework.Utils.isSkipServiceInstallationEnabled;

/**
 * Controller Service.
 */
@Slf4j
public class PravegaControllerService extends MarathonBasedService {

    private static final int CONTROLLER_PORT = 9092;
    private static final int REST_PORT = 10080;
    private final URI zkUri;
    private int instances = 1;
    private double cpu = 0.1;
    private double mem = 700;

    public PravegaControllerService(final String id, final URI zkUri) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(isSkipServiceInstallationEnabled() ? "/pravega/controller" : id);
        this.zkUri = zkUri;
    }

    public PravegaControllerService(final String id, final URI zkUri, int instances, double cpu, double mem) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(isSkipServiceInstallationEnabled() ? "/pravega/controller" : id);
        this.zkUri = zkUri;
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
        deleteApp("/pravega/controller");
        log.debug("Starting service: {}", getID());
        try {
            marathonClient.createApp(createPravegaControllerApp());
            if (wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        } catch (InterruptedException | ExecutionException  | TimeoutException ex) {
            throw new TestFrameworkException(InternalError, "Exception while " +
                    "starting Pravega Controller Service", ex);
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
     * This is a placeholder to perform clean up actions
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
        app.getContainer().setType(CONTAINER_TYPE);
        app.getContainer().setDocker(new Docker());
        //set volume
        Collection<Volume> volumeCollection = new ArrayList<Volume>();
        volumeCollection.add(createVolume("/tmp/logs", "/mnt/logs", "RW"));
        app.getContainer().setVolumes(volumeCollection);
        app.getContainer().getDocker().setImage(IMAGE_PATH + "/nautilus/pravega-controller:" + PRAVEGA_VERSION);
        app.getContainer().getDocker().setNetwork(NETWORK_TYPE);
        app.getContainer().getDocker().setForcePullImage(FORCE_IMAGE);
        //set docker container parameters
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        List<Parameter> parameterList = new ArrayList<>();
        Parameter element1 = new Parameter("env", "SERVER_OPTS=\"-DZK_URL=" + zk + "\"");
        parameterList.add(element1);
        app.getContainer().getDocker().setParameters(parameterList);
        //set port
        app.setPorts(Arrays.asList(CONTROLLER_PORT, REST_PORT));
        app.setRequirePorts(true);
        List<HealthCheck> healthCheckList = new ArrayList<HealthCheck>();
        healthCheckList.add(setHealthCheck(900, "TCP", false, 60, 20, 0));
        app.setHealthChecks(healthCheckList);
        //set env
        Map<String, String> map = new HashMap<>();
        map.put("ZK_URL", zk);
        map.put("CONTROLLER_SERVER_PORT", String.valueOf(CONTROLLER_PORT));
        map.put("REST_SERVER_PORT", String.valueOf(REST_PORT));
        app.setEnv(map);

        return app;
    }
}
