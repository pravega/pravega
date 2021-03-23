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
import org.junit.AfterClass;
import org.junit.Test;

public class ClusterCommandsNoZKTests extends AbstractAdminCommandTest {

    @Test
    public void testGetClusterNodesCommand() throws Exception {
        // Check that all the commands handle without throwing ZK being down.
        SETUP_UTILS.stopAllServices();
        TestUtils.executeCommand("cluster list-instances", STATE.get());
        TestUtils.executeCommand("cluster get-host-by-container 0", STATE.get());
        TestUtils.executeCommand("cluster list-containers", STATE.get());
    }

    @AfterClass
    public static void tearDown() {
        STATE.get().close();
    }
}
