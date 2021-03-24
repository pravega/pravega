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
package io.pravega.test.common;

import lombok.Getter;
import org.apache.curator.test.TestingServer;

/**
 * ZK curator TestingServer starter.
 */
public class TestingServerStarter {

    @Getter
    private final int adminServerPort;

    public TestingServerStarter() {
        adminServerPort = TestUtils.getAvailableListenPort();
    }

    public TestingServer start() throws Exception {
        System.setProperty("zookeeper.admin.serverPort", Integer.toString(adminServerPort));
        return new TestingServer();
    }
}
