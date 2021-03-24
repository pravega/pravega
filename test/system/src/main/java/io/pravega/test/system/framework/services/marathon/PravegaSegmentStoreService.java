/**
 * Copyright Pravega Authors.
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
package io.pravega.test.system.framework.services.marathon;

import com.google.common.base.Strings;
import io.pravega.test.system.framework.TestFrameworkException;
import io.pravega.test.system.framework.Utils;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.HealthCheck;

import static io.pravega.test.system.framework.TestFrameworkException.Type.InternalError;

@Slf4j
public class PravegaSegmentStoreService extends MarathonBasedService {

    private static final int SEGMENTSTORE_PORT = 12345;
    private static final String SEGMENTSTORE_EXTRA_ENV = System.getProperty("segmentStoreExtraEnv");
    private static final String ENV_SEPARATOR = ";;";
    private static final java.lang.String KEY_VALUE_SEPARATOR = "::";
    private final URI zkUri;
    private int instances = 1;
    private double cpu = 0.5;
    private double mem = 1741.0;
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
                waitUntilServiceRunning().get(10, TimeUnit.MINUTES);
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
        app.setCpus(cpu);
        app.setMem(mem);
        app.setInstances(instances);
        //set constraints
        app.setConstraints(setConstraint("hostname", "UNIQUE"));
        //docker container
        app.setContainer(new Container());
        app.getContainer().setType(CONTAINER_TYPE);
        app.getContainer().setDocker(new Docker());
        //set the image and network
        app.getContainer().getDocker().setImage(IMAGE_PATH + "/nautilus/pravega:" + PRAVEGA_VERSION);
        //set port
        app.setPortDefinitions(Arrays.asList(createPortDefinition(SEGMENTSTORE_PORT)));
        app.setRequirePorts(true);
        //healthchecks
        List<HealthCheck> healthCheckList = new ArrayList<HealthCheck>();
        healthCheckList.add(setHealthCheck(300, "TCP", false, 60, 20, 0, SEGMENTSTORE_PORT));
        app.setHealthChecks(healthCheckList);
        //set env
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;

        //Environment variables to configure SS service.
        Map<String, Object> map = new HashMap<>();
        map.put("ZK_URL", zk);
        map.put("BK_ZK_URL", zk);
        map.put("CONTROLLER_URL", conUri.toString());
        getCustomEnvVars(map, SEGMENTSTORE_EXTRA_ENV);

        //Properties set to override defaults for system tests
        String hostSystemProperties = "-Xmx1024m" +
                buildSystemProperty("autoScale.muteInSeconds", "120") +
                buildSystemProperty("autoScale.cooldownInSeconds", "120") +
                buildSystemProperty("autoScale.cacheExpiryInSeconds", "120") +
                buildSystemProperty("autoScale.cacheCleanUpInSeconds", "120") +
                buildSystemProperty("log.level", "DEBUG") +
                buildSystemProperty("log.dir", "$MESOS_SANDBOX/pravegaLogs") +
                buildSystemProperty("curator-default-session-timeout", String.valueOf(30 * 1000)) +
                buildSystemProperty("hdfs.replaceDataNodesOnFailure", "false");

        map.put("PRAVEGA_SEGMENTSTORE_OPTS", hostSystemProperties);
        app.setEnv(map);
        app.setArgs(Arrays.asList("segmentstore"));

        return app;
    }

    private void getCustomEnvVars(Map<String, Object> map, String segmentstoreExtraEnv) {
        log.info("Extra segment store env variables are {}", segmentstoreExtraEnv);
        if (!Strings.isNullOrEmpty(segmentstoreExtraEnv)) {
            Arrays.stream(segmentstoreExtraEnv.split(ENV_SEPARATOR)).forEach(str -> {
                String[] pair = str.split(KEY_VALUE_SEPARATOR);
                if (pair.length != 2) {
                    log.warn("Key Value not present {}", str);
                } else {
                    map.put(pair[0], pair[1]);
                }
            });
        } else {
            // Set HDFS as the default for Tier2.
            map.put("HDFS_URL", "hdfs.marathon.containerip.dcos.thisdcos.directory:8020");
            map.put("TIER2_STORAGE", "HDFS");
        }
    }
}
