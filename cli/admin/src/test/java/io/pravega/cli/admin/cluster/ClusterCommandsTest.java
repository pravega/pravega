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
package io.pravega.cli.admin.cluster;

import io.pravega.cli.admin.AbstractAdminCommandTest;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.test.common.SerializedClassRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@Slf4j
@RunWith(SerializedClassRunner.class)
public class ClusterCommandsTest extends AbstractAdminCommandTest {

    @Test
    public void testGetClusterNodesCommand() throws Exception {
        String commandResult = TestUtils.executeCommand("cluster list-instances", STATE.get());
        Assert.assertTrue(commandResult.contains("controllers"));
        Assert.assertNotNull(GetClusterNodesCommand.descriptor());
    }

    @Test
    public void testGetSegmentStoreByContainerCommand() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", 1234);
        log.info("COMPLETED createDummyHostContainerAssignment");
        String commandResult = TestUtils.executeCommand("cluster get-host-by-container 0", STATE.get());
        log.info("COMPLETED TestUtils.executeCommand " + commandResult);
        Assert.assertTrue(commandResult.contains("owner_segment_store"));
        log.info("COMPLETED ASSERTION");
        Assert.assertNotNull(GetSegmentStoreByContainerCommand.descriptor());
    }

    @Test
    public void testListContainersCommand() throws Exception {
        TestUtils.createDummyHostContainerAssignment(SETUP_UTILS.getZkTestServer().getConnectString(), "localhost", 1234);
        log.info("COMPLETED createDummyHostContainerAssignment 2");
        String commandResult = TestUtils.executeCommand("cluster list-containers", STATE.get());
        log.info("COMPLETED TestUtils.executeCommand 2 " + commandResult);
        Assert.assertTrue(commandResult.contains("segment_store_container_map"));
        log.info("COMPLETED ASSERTION 2");
        Assert.assertNotNull(ListContainersCommand.descriptor());
    }
}
