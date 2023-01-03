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
package io.pravega.cli.user.stream;

import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.TestUtils;
import io.pravega.cli.user.config.InteractiveConfig;
import io.pravega.cli.user.scope.ScopeCommand;
import io.pravega.shared.NameUtils;
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

public class StreamCommandTest {

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

    @Test(timeout = 5000)
    @SneakyThrows
    public void testCreateStream() {
        String scope = "createStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "newStream");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), cliConfig());
        new ScopeCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("stream create " + stream, cliConfig());
        Assert.assertTrue(commandResult.contains("created successfully"));
        Assert.assertNotNull(StreamCommand.Create.descriptor());
    }

    @Test(timeout = 5000)
    @SneakyThrows
    public void testDeleteStream() {
        String scope = "deleteStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "deleteStream");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), cliConfig());
        new ScopeCommand.Create(commandArgs).execute();

        commandArgs = new CommandArgs(Collections.singletonList(stream), cliConfig());
        new StreamCommand.Create(commandArgs).execute();

        String commandResult = TestUtils.executeCommand("stream delete " + stream, cliConfig());
        Assert.assertTrue(commandResult.contains("deleted successfully"));
        Assert.assertNotNull(StreamCommand.Delete.descriptor());
    }

    @Test(timeout = 5000)
    @SneakyThrows
    public void testListStream() {
        String scope = "listStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "theStream");
        CommandArgs commandArgsScope = new CommandArgs(Collections.singletonList(scope), cliConfig());
        new ScopeCommand.Create(commandArgsScope).execute();

        // List Streams in scope when it is empty.
        String commandResult = TestUtils.executeCommand("stream list " + scope, cliConfig());
        Assert.assertFalse(commandResult.contains("theStream"));

        CommandArgs commandArgsStream = new CommandArgs(Collections.singletonList(stream), cliConfig());
        new StreamCommand.Create(commandArgsStream).execute();

        // List Streams in scope when we have one.
        commandResult = TestUtils.executeCommand("stream list " + scope, cliConfig());
        Assert.assertTrue(commandResult.contains("theStream"));
        Assert.assertNotNull(StreamCommand.List.descriptor());
    }

    @Test(timeout = 40000)
    @SneakyThrows
    public void testAppendAndReadStream() {
        String scope = "appendAndReadStreamScope";
        String stream = NameUtils.getScopedStreamName(scope, "appendAndReadStream");
        CommandArgs commandArgs = new CommandArgs(Collections.singletonList(scope), cliConfig());
        new ScopeCommand.Create(commandArgs).execute();

        CommandArgs commandArgsStream = new CommandArgs(Collections.singletonList(stream), cliConfig());
        new StreamCommand.Create(commandArgsStream).execute();

        String commandResult = TestUtils.executeCommand("stream append " + stream + " 100", cliConfig());
        Assert.assertTrue(commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Append.descriptor());

        // Need to use a timeout for readers, otherwise the test never completes.
        commandResult = TestUtils.executeCommand("stream read " + stream + " true 5", cliConfig());
        Assert.assertTrue(commandResult.contains("Done"));

        commandResult = TestUtils.executeCommand("stream append " + stream + " key 100", cliConfig());
        Assert.assertTrue(commandResult.contains("Done"));
        commandResult = TestUtils.executeCommand("stream read " + stream + " 5", cliConfig());
        Assert.assertTrue(commandResult.contains("Done"));
        Assert.assertNotNull(StreamCommand.Read.descriptor());
    }

    public static class SecureStreamCommandsTest extends StreamCommandTest {
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
