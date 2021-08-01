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
package io.pravega.local;

import io.pravega.test.common.SerializedClassRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.pravega.local.PravegaSanityTests.testWriteAndReadAnEvent;

/**
 * This class contains tests for in-process standalone cluster. It also configures and runs standalone mode cluster
 * with appropriate configuration for itself as well as for sub-classes.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class InProcPravegaClusterTest {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().build();
    final String msg = "Test message on the plaintext channel";

    /**
     * Compares reads and writes to verify that an in-process Pravega cluster responds properly with
     * with valid client configuration.
     *
     * Note:
     * Strictly speaking, this test is really an "integration test" and is a little time consuming. For now, its
     * intended to also run as a unit test, but it could be moved to an integration test suite if and when necessary.
     *
     */
    @Test(timeout = 50000)
    public void testWriteAndReadEventWithValidClientConfig() throws Exception {
        testWriteAndReadAnEvent("TestScope", "TestStream", msg, EMULATOR.getClientConfig());
    }
}
