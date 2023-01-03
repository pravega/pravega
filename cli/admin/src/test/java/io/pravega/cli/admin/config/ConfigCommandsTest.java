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
package io.pravega.cli.admin.config;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class ConfigCommandsTest {

    @Test
    public void testSetAndListConfigCommands() throws Exception {
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", "test");
        @Cleanup
        AdminCommandState adminCommandState = new AdminCommandState();
        adminCommandState.getConfigBuilder().include(pravegaProperties);
        String commandResult = TestUtils.executeCommand("config list", adminCommandState);
        Assert.assertTrue(commandResult.contains("cli.controller.rest.uri"));
        TestUtils.executeCommand("config set hello=world", adminCommandState);
        commandResult = TestUtils.executeCommand("config list", adminCommandState);
        Assert.assertTrue(commandResult.contains("hello=world"));
    }

    @Test
    public void testEnvSetConfig() throws Exception {
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", "dummy");
        @Cleanup
        AdminCommandState adminCommandState = new AdminCommandState();
        adminCommandState.getConfigBuilder().include(pravegaProperties);
        TestUtils.executeCommand("config set cli.controller.rest.uri=$PWD", adminCommandState);
        String commandResult = TestUtils.executeCommand("config list", adminCommandState);
        Assert.assertTrue(commandResult.contains("cli.controller.rest.uri=" + System.getenv("PWD")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonExistentEnvSetConfig() throws Exception {
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("cli.controller.rest.uri", "test");
        @Cleanup
        AdminCommandState adminCommandState = new AdminCommandState();
        adminCommandState.getConfigBuilder().include(pravegaProperties);
        TestUtils.executeCommand("config set hello=$world", adminCommandState);
        TestUtils.executeCommand("config list", adminCommandState);
    }

}
