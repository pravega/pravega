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
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.kubernetes.AbstractService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import mesosphere.marathon.client.MarathonException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertNotNull;

@Slf4j
public class ParseSystemPropertyTest {
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
     * Test to verify if the config values are appropriately parsed as in a string.
     */
    @Test
    public void parseSysPropTest() {
        val tier2Config = "azure.connection.string=\"DefaultEndpointsProtocol=https;AccountName=xyz29;AccountKey=WR4m2VjvIRFsDN8KsKbcZkK3/EsIflzWIDNgAVLdJDLf62A8Fs0yIeVoGZKzIUMs8ZfxKj938q/9+ASt7MiVhw==;EndpointSuffix=core.windows.net\",azure.container=azuretests,azure.prefix=prefix,azure.container.create=false";
        val retValue = AbstractService.parseSystemPropertyAsMap("Test", tier2Config);
        assertNotNull(retValue);
        log.info("The number of config parameters passed is {}", retValue.size());
    }

    @Test
    public void parseSysPropPositiveTest() {
        val tier2Config = "key = value";
        val retValue = AbstractService.parseSystemPropertyAsMap("Test", tier2Config);
        log.debug("The test successfully parses config parameters as key and value pairs.");
    }
}
