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
import java.util.ArrayList;
import java.util.List;
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

@Slf4j
public class ZookeeperService extends MarathonBasedService {

    private static final String ZK_IMAGE = "zookeeper:3.5.4-beta";
    private int instances = 1;
    private double cpu = 1.0;
    private double mem = 1024.0;

    public ZookeeperService(final  String id) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(Utils.isSkipServiceInstallationEnabled() ? "/pravega/exhibitor" : id);
    }

    public ZookeeperService(final String id, int instances, double cpu, double mem) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(Utils.isSkipServiceInstallationEnabled() ? "/pravega/exhibitor" : id);
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
    }

    @Override
    public void start(final boolean wait) {
        deleteApp("/pravega/exhibitor");
        log.info("Starting Zookeeper Service: {}", getID());
        try {
            marathonClient.createApp(createZookeeperApp());
            if (wait) {
                waitUntilServiceRunning().get(10, TimeUnit.MINUTES);
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
            throw new TestFrameworkException(InternalError, "Exception while " +
                    "starting Zookeeper Service", ex);
        }
    }

        //This is a placeholder to perform clean up actions
        @Override
        public void clean() {
        }

        @Override
        public void stop() {
            log.info("Stopping Zookeeper Service : {}", getID());
            deleteApp(getID());
        }

    private App createZookeeperApp() {
        App app = new App();
        app.setId(this.id);
        app.setCpus(cpu);
        app.setMem(mem);
        app.setInstances(instances);
        app.setContainer(new Container());
        app.getContainer().setType(CONTAINER_TYPE);
        app.getContainer().setDocker(new Docker());
        app.getContainer().getDocker().setImage(ZK_IMAGE);
        List<HealthCheck> healthCheckList = new ArrayList<>();
        final HealthCheck hc = setHealthCheck(300, "TCP", false, 60, 20, 0, ZKSERVICE_ZKPORT);
        healthCheckList.add(hc);
        app.setHealthChecks(healthCheckList);

        return app;
    }
}
