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
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
import io.pravega.test.integration.utils.ClusterWrapper;
import java.util.Collections;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.pravega.cli.user.TestUtils.createCLIConfig;
import static io.pravega.cli.user.TestUtils.createPravegaCluster;
import static io.pravega.cli.user.TestUtils.getCLIControllerUri;

public class KVTCommandsTest extends SecureKVTCommandsTest {
    private static final ClusterWrapper CLUSTER = createPravegaCluster(false, false);
    private static final InteractiveConfig CONFIG = createCLIConfig(getCLIControllerUri(CLUSTER.controllerUri()), false, false);
    private static final int PK_LENGTH = 2;
    private static final int SK_LENGTH = 2;

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

    @Test
    public void testDescriptors() {
        Assert.assertNotNull(KeyValueTableCommand.Create.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.Delete.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.ListKVTables.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.Put.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.Put.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.PutIf.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.PutIfAbsent.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.PutAll.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.Get.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.Remove.descriptor());
        Assert.assertNotNull(KeyValueTableCommand.ListEntries.descriptor());
    }

    @Test(timeout = 60000)
    public void testKVTNoSecondaryKey() {
        final String scope = "kvTableNoSecondaryKey";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), cliConfig());
        new ScopeCommand.Create(commandArgs).execute();

        checkCommand(String.format("kvt create %s %s %s", table, PK_LENGTH, 0), "created successfully");

        // Exercise puts first.
        checkCommand(String.format("kvt put %s 00 value1", table), "updated successfully");
        checkCommand(String.format("kvt put-if %s 00 0:1 value2", table), "BadKeyVersionException");
        checkCommand(String.format("kvt put-if-absent %s 01 value3", table), "inserted successfully");
        checkCommand(String.format("kvt put-if-absent %s 11 value4", table), "inserted successfully");

        // Exercise list commands.
        checkCommand(String.format("kvt list-entries %s prefix 0 ", table), "value1", "value3");
        checkCommand(String.format("kvt list-entries %s range 00 09 ", table), "value1", "value3");
        checkCommand(String.format("kvt list-entries %s", table), "value1", "value3", "value4");

        // Exercise Get command.
        checkCommand(String.format("kvt get %s {[00, 01, 11]}", table), "Get");

        // Exercise Remove commands.
        checkCommand(String.format("kvt remove %s {[[00]]}", table), "Removed");
        checkCommand(String.format("kvt delete %s", table), "deleted successfully");
    }

    @Test(timeout = 60000)
    public void testKVTWithSecondaryKey() {
        final String scope = "kvTableSecondaryKey";
        final String table = NameUtils.getScopedStreamName(scope, "kvt1");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), cliConfig());
        new ScopeCommand.Create(commandArgs).execute();

        checkCommand(String.format("kvt create %s %s %s", table, PK_LENGTH, SK_LENGTH), "created successfully");

        // Exercise puts first.
        checkCommand(String.format("kvt put %s \"00:01\" value1", table), "updated successfully");
        checkCommand(String.format("kvt put-if %s \"00:01\" 0:1 value2", table), "BadKeyVersionException");
        checkCommand(String.format("kvt put-if-absent %s \"01:00\" value3", table), "inserted successfully");
        checkCommand(String.format("kvt put-if-absent %s \"11:10\" value4", table), "inserted successfully");
        checkCommand(String.format("kvt put-all %s {[[\"00:01\", value5], [\"00:03\", value6]]}", table), "Updated");

        // Exercise list commands.
        checkCommand(String.format("kvt list-entries %s pk 00 01 09", table), "value5", "value6");
        checkCommand(String.format("kvt list-entries %s pk 00 0 ", table), "value5", "value6");
        checkCommand(String.format("kvt list-entries %s prefix 0 ", table), "value5", "value3", "value6");
        checkCommand(String.format("kvt list-entries %s range 00 09 ", table), "value5", "value3", "value6");
        checkCommand(String.format("kvt list-entries %s", table), "value3", "value4", "value5", "value6");

        // Exercise Get command.
        checkCommand(String.format("kvt get %s {[\"00:01\", \"01:00\", \"11:10\"]}", table), "Get");

        // Exercise Remove commands.
        checkCommand(String.format("kvt remove %s {[[\"00:01\"], [\"00:03\"]]}", table), "Removed");
        checkCommand(String.format("kvt delete %s", table), "deleted successfully");
    }
}
