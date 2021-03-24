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
package io.pravega.cli.user.kvs;

import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
import io.pravega.test.integration.demo.ClusterWrapper;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static io.pravega.cli.user.TestUtils.createPravegaCluster;
import static io.pravega.cli.user.TestUtils.getCLIControllerUri;
import static io.pravega.cli.user.TestUtils.createCLIConfig;

public class SecureKVTCommandsTest {
    private static final ClusterWrapper CLUSTER = createPravegaCluster(true, true);
    private static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), true, true);

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
    public void testCreateKVT() {
        final String scope = "createKVTable";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        String commandResult = TestUtils.executeCommand("scope create " + scope, cliConfig());
        Assert.assertTrue(commandResult.contains("created successfully"));

        commandResult = TestUtils.executeCommand("kvt create " + table, cliConfig());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Create.descriptor());
    }

    @Test(timeout = 20000)
    @SneakyThrows
    public void testDeleteKVT() {
        final String scope = "deleteKVTable";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), cliConfig());
        new ScopeCommand.Create(commandArgs).execute();

        commandArgs = new CommandArgs(Collections.singletonList(table), cliConfig());
        new KeyValueTableCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("kvt delete " + table, cliConfig());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(KeyValueTableCommand.Delete.descriptor());
    }

    @Test(timeout = 10000)
    @SneakyThrows
    public void testListKVT() {
        final String scope = "listKVTable";
        final String table = scope + "/kvt1";
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), cliConfig());
        new ScopeCommand.Create(commandArgs).execute();

        CommandArgs commandArgsCreate = new CommandArgs(Collections.singletonList(table), cliConfig());
        new KeyValueTableCommand.Create(commandArgsCreate).execute();

        String commandResult = TestUtils.executeCommand("kvt list " + scope, cliConfig());
        Assert.assertTrue(commandResult.contains("kvt1"));
        Assert.assertNotNull(KeyValueTableCommand.ListKVTables.descriptor());
    }

    // TODO: Test KVT commands in the secure scenario (auth+TLS).
    // Cannot at this point due to the following issue:
    // Issue 5374: Updating a KeyValueTable throws an InvalidClaimException against standalone with auth and TLS enabled
    // https://github.com/pravega/pravega/issues/5374
}
