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

import io.pravega.cli.user.config.InteractiveConfig;
import org.junit.Test;

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
        UserCLIRunner.doMain(new String[]{"scope", "wrongCommand"});
    }

    @Test
    public void testCommandDetails() {
        UserCLIRunner.printCommandDetails(Parser.parse("kvt create test", InteractiveConfig.getDefault()));
        UserCLIRunner.printCommandDetails(Parser.parse("wrong command", InteractiveConfig.getDefault()));
    }
}
