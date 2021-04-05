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
import java.util.Collection;
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
import mesosphere.marathon.client.model.v2.Volume;
import mesosphere.marathon.client.MarathonException;

import static io.pravega.test.system.framework.TestFrameworkException.Type.InternalError;

@Slf4j
public class BookkeeperService extends MarathonBasedService {

    private static final int BK_PORT = 3181;
    private final URI zkUri;
    private int instances = 3;
    private double cpu = 0.5;
    private double mem = 1024.0;

    public BookkeeperService(final String id, final URI zkUri) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(Utils.isSkipServiceInstallationEnabled() ? "/pravega/bookkeeper" : id);
        this.zkUri = zkUri;
    }

    public BookkeeperService(final String id, final URI zkUri, int instances, double cpu, double mem) {
        // if SkipserviceInstallation flag is enabled used the default id.
        super(Utils.isSkipServiceInstallationEnabled() ? "/pravega/bookkeeper" : id);
        this.zkUri = zkUri;
        this.instances = instances;
        this.cpu = cpu;
        this.mem = mem;
    }

    @Override
    public void start(final boolean wait) {
        deleteApp("/pravega/bookkeeper");
        log.info("Starting Bookkeeper Service: {}", getID());
        try {
            marathonClient.createApp(createBookieApp());
            if (wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
        } catch (MarathonException e) {
            handleMarathonException(e);
        } catch (InterruptedException | ExecutionException | TimeoutException ex) {
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
        deleteApp(getID());
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
        Collection<Volume> volumeCollection = new ArrayList<>();
        volumeCollection.add(createVolume("/bk", "mnt", "RW"));
        //TODO: add persistent volume  (see issue https://github.com/pravega/pravega/issues/639)
        app.getContainer().setVolumes(volumeCollection);
        app.setPorts(Arrays.asList(BK_PORT));
        app.setRequirePorts(true);
        //set env
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        Map<String, Object> map = new HashMap<>();
        map.put("ZK_URL", zk);
        map.put("ZK", zk);
        map.put("bookiePort", String.valueOf(BK_PORT));
        map.put("DLOG_EXTRA_OPTS", "-Xms512m");
        app.setEnv(map);
        //healthchecks
        List<HealthCheck> healthCheckList = new ArrayList<>();
        healthCheckList.add(setHealthCheck(300, "TCP", false, 60, 20, 0, BK_PORT));
        app.setHealthChecks(healthCheckList);

        return app;
    }
}
