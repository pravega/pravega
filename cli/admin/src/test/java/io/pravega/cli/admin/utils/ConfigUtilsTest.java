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
package io.pravega.cli.admin.utils;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.test.common.SerializedClassRunner;
import lombok.Cleanup;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(SerializedClassRunner.class)
public class ConfigUtilsTest {

    @Test
    public void testConfigUtilsWithValidFile() throws IOException {
        System.setProperty("pravega.configurationFile", "../../config/admin-cli.properties");
        @Cleanup
        AdminCommandState commandState = new AdminCommandState();
        System.setProperty("pravegaservice", "pravegaservice");
        ConfigUtils.loadProperties(commandState);
    }

    @Test
    public void testConfigUtilsWithInValidFile() throws IOException {
        System.setProperty("pravega.configurationFile", "../../config/admin-cli.properties");
        @Cleanup
        AdminCommandState commandState = new AdminCommandState();
        System.setProperty("pravega.configurationFile", "dummy");
        System.setProperty("pravegaservice", "pravegaservice");
        ConfigUtils.loadProperties(commandState);
    }

}
