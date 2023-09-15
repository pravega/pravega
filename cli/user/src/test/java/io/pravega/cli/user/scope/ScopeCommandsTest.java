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
package io.pravega.cli.user.scope;

import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.test.integration.utils.ClusterWrapper;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static io.pravega.cli.user.TestUtils.createPravegaCluster;
import static io.pravega.cli.user.TestUtils.getCLIControllerUri;
import static io.pravega.cli.user.TestUtils.createCLIConfig;

public class ScopeCommandsTest {

    private static final ClusterWrapper CLUSTER = createPravegaCluster(false, false);
    private static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), false, false);

    protected InteractiveConfig cliConfig() {
        return CONFIG;
    }

    @BeforeClass
    public static void start() {
        CLUSTER.start();
    }

    @AfterClass
    public static void shutDown() {
        if (CLUSTER != null) {
            CLUSTER.close();
        }
    }

    @Test(timeout = 10000)
    @SneakyThrows
    public void testCreateScope() {
        final String scope = "testCreate";
        String commandResult = TestUtils.executeCommand("scope create " + scope, cliConfig());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(ScopeCommand.Create.descriptor());

        String cleanUp = TestUtils.executeCommand("scope delete " + scope, cliConfig());
    }

    @Test(timeout = 10000)
    @SneakyThrows
    public void testDeleteScope() {
        String scopeToDelete = "toDelete";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scopeToDelete), cliConfig());
        Assert.assertNotNull(commandArgs.toString());
        new ScopeCommand.Create(commandArgs).execute();
        String commandResult = TestUtils.executeCommand("scope delete " + scopeToDelete, cliConfig());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(ScopeCommand.Delete.descriptor());
    }

    @Test(timeout = 10000)
    @SneakyThrows
    public void testListScope() {
        final String scope1 = "b";
        final String scope2 = "z";
        final String scope3 = "c";
        final String scope4 = "a";
        final String scope5 = "aaa";

        TestUtils.executeCommand("scope create " + scope1, cliConfig());
        TestUtils.executeCommand("scope create " + scope2, cliConfig());
        TestUtils.executeCommand("scope create " + scope3, cliConfig());
        TestUtils.executeCommand("scope create " + scope4, cliConfig());
        TestUtils.executeCommand("scope create " + scope5, cliConfig());

        String commandResult = TestUtils.executeCommand("scope list", cliConfig());
        Assert.assertTrue(commandResult.equals(
                "\t_system\n" +
                        "\ta\n" +
                        "\taaa\n" +
                        "\tb\n" +
                        "\tc\n" +
                        "\tz\n"));
        Assert.assertNotNull(ScopeCommand.List.descriptor());
    }

    public static class SecureScopeCommandsTest extends ScopeCommandsTest {
        private static final ClusterWrapper CLUSTER = createPravegaCluster(true, true);
        private static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), true, true);

        @Override
        protected InteractiveConfig cliConfig() {
            return CONFIG;
        }

        @BeforeClass
        public static void start() {
            CLUSTER.start();
        }

        @AfterClass
        public static void shutDown() {
            if (CLUSTER != null) {
                CLUSTER.close();
            }
        }
    }
}
