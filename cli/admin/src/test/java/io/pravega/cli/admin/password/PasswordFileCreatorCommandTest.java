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
package io.pravega.cli.admin.password;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class PasswordFileCreatorCommandTest {

    @Test
    public void testPasswordFileCreatorCommand() throws Exception {
        final String fileName = "passwordFileTest";
        try {
            @Cleanup
            val c1 = new AdminCommandState();
            TestUtils.executeCommand("password create-password-file " + fileName + " user:password:acl", c1);
            Assert.assertTrue(Files.exists(Paths.get(fileName)));
            // Test wrong input arguments.
            @Cleanup
            val c2 = new AdminCommandState();
            TestUtils.executeCommand("password create-password-file " + fileName + " wrong", c2);
            @Cleanup
            val c3 = new AdminCommandState();
            TestUtils.executeCommand("password create-password-file wrong", c3);
            // Remove generated file by command.
        } finally {
            Files.delete(Paths.get(fileName));
        }
    }

    @Test
    public void testPasswordFileCreatorCommandParsesAuthFormat() throws Exception {
        final String fileName = "fileWithAuthFormat";
        try {
            @Cleanup
            val c1 = new AdminCommandState();
            TestUtils.executeCommand("password create-password-file " + fileName + " user:password:prn::/scope:scope1,READ_UPDATE,", c1);
            Assert.assertTrue(Files.exists(Paths.get(fileName)));
        } finally {
            Files.delete(Paths.get(fileName));
        }
    }
}
