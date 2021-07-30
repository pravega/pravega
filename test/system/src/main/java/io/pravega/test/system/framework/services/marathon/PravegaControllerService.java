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
import mesosphere.marathon.client.model.v2.App;
import mesosphere.marathon.client.model.v2.Container;
import mesosphere.marathon.client.model.v2.Docker;
import mesosphere.marathon.client.model.v2.HealthCheck;
import mesosphere.marathon.client.MarathonException;
import static io.pravega.test.system.framework.TestFrameworkException.Type.InternalError;

/**
 * Controller Service.
 */
@Slf4j
public class PravegaControllerService extends MarathonBasedService {

    public static final int CONTROLLER_PORT = 9092;
    public static final int REST_PORT = 10080;
    private static final String COMPONENT_CODE = "controller";
    private final URI zkUri;
    private int instances = 1;
    private double cpu = 0.5;
    private double mem = 700;

    public PravegaControllerService(final String id, final URI zkUri) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(Utils.isSkipServiceInstallationEnabled() ? "/pravega/controller" : id);
        this.zkUri = zkUri;
    }

    public PravegaControllerService(final String id, final URI zkUri, int instances, double cpu, double mem) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(Utils.isSkipServiceInstallationEnabled() ? "/pravega/controller" : id);
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
                waitUntilServiceRunning().get(10, TimeUnit.MINUTES);
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
        deleteApp(getID());
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
        app.getContainer().getDocker().setImage(IMAGE_PATH + "/nautilus/pravega:" + PRAVEGA_VERSION);
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        //set port
        app.setPortDefinitions(Arrays.asList(createPortDefinition(CONTROLLER_PORT), createPortDefinition(REST_PORT)));
        app.setRequirePorts(true);
        List<HealthCheck> healthCheckList = new ArrayList<HealthCheck>();
        healthCheckList.add(setHealthCheck(300, "TCP", false, 60, 20, 0, CONTROLLER_PORT));
        app.setHealthChecks(healthCheckList);

        String controllerSystemProperties = "-Xmx512m" +
                buildSystemProperty(propertyName("zk.connect.uri"), zk) +
                buildSystemProperty(propertyName("service.rpc.published.host.nameOrIp"), this.id + ".marathon.mesos") +
                buildSystemProperty(propertyName("service.rpc.published.port"), String.valueOf(CONTROLLER_PORT)) +
                buildSystemProperty(propertyName("service.rpc.listener.port"), String.valueOf(CONTROLLER_PORT)) +
                buildSystemProperty(propertyName("service.rest.listener.port"), String.valueOf(REST_PORT)) +
                buildSystemProperty("log.level", "DEBUG") +
                buildSystemProperty("log.dir", "$MESOS_SANDBOX/pravegaLogs") +
                buildSystemProperty("curator-default-session-timeout", String.valueOf(10 * 1000)) +
                buildSystemProperty(propertyName("transaction.lease.count.max"), String.valueOf(600 * 1000)) +
                buildSystemProperty(propertyName("retention.frequency.minutes"), String.valueOf(2));

        Map<String, Object> map = new HashMap<>();
        map.put("PRAVEGA_CONTROLLER_OPTS", controllerSystemProperties);
        app.setEnv(map);
        app.setArgs(Arrays.asList("controller"));

        return app;
    }

    private String propertyName(String str) {
        return String.format("%s.%s", COMPONENT_CODE, str);
    }
}
