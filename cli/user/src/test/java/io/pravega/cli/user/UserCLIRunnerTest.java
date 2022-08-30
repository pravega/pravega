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
package io.pravega.cli.user;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.pravega.cli.user.config.InteractiveConfig;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class UserCLIRunnerTest {

    @Test
    public void testWrongExecCommandInputs() {
        UserCLIRunner.processCommand("", InteractiveConfig.builder().build());
        UserCLIRunner.processCommand("fakecommand", InteractiveConfig.builder().build());
        UserCLIRunner.processCommand("scope fakeoption", InteractiveConfig.builder().build());
        UserCLIRunner.processCommand("help", InteractiveConfig.builder().build());
        UserCLIRunner.processCommand("scope create", InteractiveConfig.builder().build());
    }

    @Test
    public void testDoMain() {
        UserCLIRunner.doMain(new String[]{"scope", "wrongCommand"}, System.in);
    }

    @Test
    public void testDoMainInteractive() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Level previousLevel =  loggerContext.getLoggerList().get(0).getLevel();

        try {
            InputStream in = new ByteArrayInputStream("config set log-level=info\nexit\n".getBytes());
            UserCLIRunner.doMain(new String[]{}, in);
            Assert.assertEquals(loggerContext.getLoggerList().get(0).getLevel(), Level.INFO);
        } finally {
            loggerContext.getLoggerList().get(0).setLevel(previousLevel);
        }
    }

    @Test
    public void testCommandDetails() {
        Map<String, String> environment = Collections.singletonMap("TestKey", "TestValue");
        UserCLIRunner.printCommandDetails(Parser.parse("kvt create test", InteractiveConfig.getDefault(environment)));
        UserCLIRunner.printCommandDetails(Parser.parse("wrong command", InteractiveConfig.getDefault(environment)));
    }

}
