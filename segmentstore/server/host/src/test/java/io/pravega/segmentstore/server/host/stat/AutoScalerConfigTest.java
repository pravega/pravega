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
package io.pravega.segmentstore.server.host.stat;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class AutoScalerConfigTest {

    // region Tests that verify the toString() method.

    // Note: It might seem odd that we are unit testing the toString() method of the code under test. The reason we are
    // doing that is that the method is hand-rolled and there is a bit of logic there that isn't entirely unlikely to fail.

    @Test
    public void testToStringIsSuccessfulWithAllNonDefaultConfigSpecified() {
        AutoScalerConfig config = AutoScalerConfig.builder()
                .with(AutoScalerConfig.TLS_CERT_FILE, "/cert.pem")
                .build();
        assertNotNull(config.toString());
    }

    @Test
    public void testToStringIsSuccessfulWithNoConfigSpecified() {
        AutoScalerConfig config = AutoScalerConfig.builder()
                .build();
        assertNotNull(config.toString());
    }

    // endregion
}
