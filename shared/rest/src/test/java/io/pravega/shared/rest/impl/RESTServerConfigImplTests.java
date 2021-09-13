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
package io.pravega.shared.rest.impl;

import io.pravega.shared.rest.RESTServerConfig;
import io.pravega.test.common.SecurityConfigDefaults;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class RESTServerConfigImplTests {
    // region Tests that verify the toString() method.

    // Note: It might seem odd that we are unit testing the toString() method of the code under test. The reason we are
    // doing that is that the method is hand-rolled and there is a bit of logic there that isn't entirely unlikely to fail.

    @Test
    public void testToStringIsSuccessfulWithAllConfigSpecified() {
        RESTServerConfig config = RESTServerConfigImpl.builder().host("localhost").port(2020).authorizationEnabled(true)
                .userPasswordFile("/passwd").tlsEnabled(true).tlsProtocolVersion(SecurityConfigDefaults.TLS_PROTOCOL_VERSION)
                .keyFilePath("/rest.keystore.jks").keyFilePasswordPath("/keystore.jks.passwd").build();
        assertNotNull(config.toString());
    }

    @Test
    public void testToStringIsSuccessfulWithTlsDisabled() {
        RESTServerConfig config = RESTServerConfigImpl.builder().host("localhost").port(2020).authorizationEnabled(false)
                .userPasswordFile(null).tlsEnabled(false).keyFilePath(null).keyFilePasswordPath(null).build();
        assertNotNull(config.toString());
    }

    // endregion
}
