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
package io.pravega.cli.admin;

import java.io.IOException;
import java.util.Arrays;
import lombok.Cleanup;
import lombok.val;
import org.junit.Test;

public class AdminCLIRunnerTests {

    @Test
    public void testWrongExecCommandInputs() throws IOException {
        val commands = Arrays.asList("", "fakecommand", "scope fakeoption", "help", "controller describe-scope 1 2 3");
        for (val cmd : commands) {
            @Cleanup
            val state = new AdminCommandState();
            AdminCLIRunner.processCommand(cmd, state);
        }
    }

    @Test
    public void testDoMain() throws IOException {
        System.setProperty("pravega.configurationFile", "../../config/admin-cli.properties");
        AdminCLIRunner.doMain(new String[]{"scope", "wrongCommand"});
    }

    @Test
    public void testCommandDetails() {
        AdminCLIRunner.printCommandDetails(Parser.parse("controller describe-scope"));
        AdminCLIRunner.printCommandDetails(Parser.parse("wrong command"));
    }

}
