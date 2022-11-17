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
package io.pravega.test.system;

import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.marathon.PravegaControllerService;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.List;

import static io.pravega.test.system.framework.Utils.REST_PORT;
import static io.pravega.test.system.framework.services.kubernetes.AbstractService.CONTROLLER_GRPC_PORT;
import static io.pravega.test.system.framework.services.kubernetes.AbstractService.CONTROLLER_REST_PORT;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class PravegaControllerTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException if error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {
        Service zk = Utils.createZookeeperService();
        if (!zk.isRunning()) {
            zk.start(true);
        }
        Service con = Utils.createPravegaControllerService(zk.getServiceDetails().get(0));
        if (!con.isRunning()) {
            con.start(true);
        }
    }

    /**
     * Invoke the controller test.
     * The test fails in case controller is not running on given ports.
     */
    @Test
    public void controllerTest() {
        log.debug("Start execution of controllerTest");
        Service con = Utils.createPravegaControllerService(null);
        List<URI> conUri = con.getServiceDetails();
        log.debug("Controller Service URI details: {} ", conUri);
        assertTrue(conUri.stream().map(URI::getPort).allMatch(port -> {
            switch (Utils.EXECUTOR_TYPE) {
                case REMOTE_SEQUENTIAL:
                    return port == PravegaControllerService.CONTROLLER_PORT || port == PravegaControllerService.REST_PORT;
                case DOCKER:
                    return port == CONTROLLER_GRPC_PORT || port == REST_PORT;
                case KUBERNETES:
                default:
                    return port == CONTROLLER_GRPC_PORT || port == CONTROLLER_REST_PORT;
            }
        }));
        log.debug("ControllerTest  execution completed");
    }
}
