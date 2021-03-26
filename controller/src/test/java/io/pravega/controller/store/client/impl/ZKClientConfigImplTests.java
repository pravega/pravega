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
package io.pravega.controller.store.client.impl;

import io.pravega.controller.store.client.ZKClientConfig;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ZKClientConfigImplTests {
    // region Tests that verify the toString() method.

    // Note: It might seem odd that we are unit testing the toString() method of the code under test. The reason we are
    // doing that is that the method is hand-rolled and there is a bit of logic there that isn't entirely unlikely to fail.
    @Test
    public void testToStringIsSuccessfulWithAllConfigSpecified() {
        ZKClientConfig config = new ZKClientConfigImpl("connectString", "namespace",
                5, 2, 60000, true,
                "/zk.truststore.jks", "zk.truststore.password");
        assertNotNull(config.toString());
    }

    @Test
    public void testToStringIsSuccessfulWithTlsDisabled() {
        ZKClientConfig config = new ZKClientConfigImpl("connectString", "namespace",
                5, 2, 60000, false,
                null, null);
        assertNotNull(config.toString());
    }

    // endregion
}
