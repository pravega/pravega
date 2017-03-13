/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.framework.services;

import com.emc.pravega.framework.TestFrameworkException;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.utils.MarathonException;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.HealthCheck;
import mesosphere.marathon.client.model.v2.Volume;
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

@Slf4j
public class PravegaSegmentStoreService extends MarathonBasedService {

    private static final int SEGMENTSTORE_PORT = 12345;
    private static final int BACK_OFF_SECS = 7200;
    private final URI zkUri;
    private int instances = 1;
    private double cpu = 0.1;
    private double mem = 1000.0;
    private final URI conUri;

    public PravegaSegmentStoreService(final String id, final URI zkUri, final URI conUri) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(isSkipServiceInstallationEnabled() ? "/pravega/host" : id);
        this.zkUri = zkUri;
        this.conUri = conUri;
    }

    public PravegaSegmentStoreService(final String id, final URI zkUri, final URI conUri, int instances, double cpu, double mem) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(isSkipServiceInstallationEnabled() ? "/pravega/host" : id);
        this.zkUri = zkUri;
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
        this.conUri = conUri;
    }

    @Override
    public void start(final boolean wait) {
        deleteApp("/pravega/host");
        log.info("Starting Pravega SegmentStore Service: {}", getID());
        try {
            marathonClient.createApp(createPravegaSegmentStoreApp());
            if (wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new TestFrameworkException(InternalError, "Exception while " +
                    "starting Pravega SegmentStore Service", ex);
        }
    }

        /**
         * Cleanup after service is stopped.
         * This is a placeholder to perform clean up actions
         */
        @Override
        public void clean() {
        }

        @Override
        public void stop() {
            log.info("Stopping Pravega SegmentStore Service : {}", getID());
            try {
                marathonClient.deleteApp(getID());
            } catch (MarathonException e) {
                handleMarathonException(e);
            }
        }

    private App createPravegaSegmentStoreApp() {
        App app = new App();
        app.setId(this.id);
        app.setBackoffSeconds(BACK_OFF_SECS);
        app.setCpus(cpu);
        app.setMem(mem);
        app.setInstances(instances);
        //set constraints
        app.setConstraints(setConstraint("hostname", "UNIQUE"));
        //docker container
        app.setContainer(new Container());
        app.getContainer().setType(CONTAINER_TYPE);
        app.getContainer().setDocker(new Docker());
        //set volume
        Collection<Volume> volumeCollection = new ArrayList<Volume>();
        volumeCollection.add(createVolume("/tmp/logs", "/mnt/logs", "RW"));
        app.getContainer().setVolumes(volumeCollection);
        //set the image and network
        app.getContainer().getDocker().setImage(IMAGE_PATH + "/nautilus/pravega-host:" + PRAVEGA_VERSION);
        app.getContainer().getDocker().setNetwork(NETWORK_TYPE);
        app.getContainer().getDocker().setForcePullImage(FORCE_IMAGE);
        //set port
        app.setPorts(Arrays.asList(SEGMENTSTORE_PORT));
        app.setRequirePorts(true);
        //healthchecks
        List<HealthCheck> healthCheckList = new ArrayList<HealthCheck>();
        healthCheckList.add(setHealthCheck(900, "TCP", false, 60, 20, 0));
        app.setHealthChecks(healthCheckList);
        //set env
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        Map<String, String> map = new HashMap<>();
        map.put("ZK_URL", zk);
        map.put("pravegaservice_zkURL", zk);
        map.put("dlog_hostname", zkUri.getHost());
        map.put("hdfs_fs_default_name", "namenode-0.hdfs.mesos:9001");
        map.put("pravegaservice_controllerUri", conUri.toString());
        app.setEnv(map);

        return app;
    }
}
