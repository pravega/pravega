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
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import java.net.URI;
import static org.apache.curator.framework.imps.CuratorFrameworkState.STARTED;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ZookeeperTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);

    /**
     * This is used to setup the various services required by the system test framework.
     * @throws MarathonException if error in setup
     */
    @Environment
    public static void initialize() throws MarathonException {
        Service zk = Utils.createZookeeperService();
        if (!zk.isRunning()) {
            zk.start(true);
        }
    }

    /**
     * Invoke the zookeeper test, ensure zookeeper can be accessed.
     * The test fails in case zookeeper cannot be accessed.
     *
     */
    @Test
    public void zkTest() {
        log.info("Start execution of ZkTest");
        Service zk = Utils.createZookeeperService();
        URI zkUri = zk.getServiceDetails().get(0);
        CuratorFramework curatorFrameworkClient = CuratorFrameworkFactory.newClient(zkUri.getHost() + ":2181",
                                                                                    new RetryOneTime(5000));
        curatorFrameworkClient.start();
        log.info("CuratorFramework status {} ", curatorFrameworkClient.getState());
        assertEquals("Connection to zk client ", STARTED, curatorFrameworkClient.getState());
        log.info("Zookeeper Service URI : {} ", zkUri);
        log.info("ZkTest  execution completed");
    }
}
