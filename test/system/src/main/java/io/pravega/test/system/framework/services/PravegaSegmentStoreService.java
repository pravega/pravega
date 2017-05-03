/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system.framework.services;

import io.pravega.test.system.framework.TestFrameworkException;
import io.pravega.test.system.framework.Utils;
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

import static io.pravega.test.system.framework.TestFrameworkException.Type.InternalError;

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
        super(Utils.isSkipServiceInstallationEnabled() ? "/pravega/segmentstore" : id);
        this.zkUri = zkUri;
        this.conUri = conUri;
    }

    public PravegaSegmentStoreService(final String id, final URI zkUri, final URI conUri, int instances, double cpu, double mem) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(Utils.isSkipServiceInstallationEnabled() ? "/pravega/segmentstore" : id);
        this.zkUri = zkUri;
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
        this.conUri = conUri;
    }

    @Override
    public void start(final boolean wait) {
        deleteApp("/pravega/segmentstore");
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
            deleteApp(getID());
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
        app.getContainer().getDocker().setImage(IMAGE_PATH + "/nautilus/pravega:" + PRAVEGA_VERSION);
        app.getContainer().getDocker().setNetwork(NETWORK_TYPE);
        app.getContainer().getDocker().setForcePullImage(FORCE_IMAGE);
        List<Parameter> parameterList = new ArrayList<>();
        Parameter element1 = new Parameter("env", "JAVA_OPTS=-Xmx900m");
        parameterList.add(element1);
        app.getContainer().getDocker().setParameters(parameterList);
        //set port
        app.setPorts(Arrays.asList(SEGMENTSTORE_PORT));
        app.setRequirePorts(true);
        //healthchecks
        List<HealthCheck> healthCheckList = new ArrayList<HealthCheck>();
        healthCheckList.add(setHealthCheck(900, "TCP", false, 60, 20, 0));
        app.setHealthChecks(healthCheckList);
        //set env
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;

        //System properties to configure SS service.
        String hostSystemProperties = setSystemProperty("pravegaservice.zkURL", zk) +
                setSystemProperty("dlog.hostname", zkUri.getHost()) +
                setSystemProperty("hdfs.hdfsUrl", "hdfs.marathon.containerip.dcos.thisdcos.directory:8020") +
                setSystemProperty("autoScale.muteInSeconds", "120") +
                setSystemProperty("autoScale.cooldownInSeconds", "120") +
                setSystemProperty("autoScale.cacheExpiryInSeconds", "120") +
                setSystemProperty("autoScale.cacheCleanUpInSeconds", "120") +
                setSystemProperty("autoScale.controllerUri", conUri.toString()) +
                setSystemProperty("log.level", "DEBUG");

        Map<String, String> map = new HashMap<>();
        map.put("PRAVEGA_SEGMENTSTORE_OPTS", hostSystemProperties);
        app.setEnv(map);
        app.setArgs(Arrays.asList("segmentstore"));

        return app;
    }
}
