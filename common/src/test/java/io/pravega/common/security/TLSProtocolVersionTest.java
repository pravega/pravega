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
package io.pravega.common.security;

import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test to check the correctness of TLS Protocol version enum.
 */

public class TLSProtocolVersionTest {

    @Test
    public void passingValidTlsProtocolVersionTest() {
        String tls12 = "TLSv1.2";
        String tls13 = "TLSv1.3";
        String tls1213 = "TLSv1.2,TLSv1.3";
        String tls1312 = "TLSv1.3,TLSv1.2";

        Assert.assertArrayEquals(new String[] {"TLSv1.2"}, new TLSProtocolVersion(tls12).getProtocols());
        Assert.assertArrayEquals(new String[] {"TLSv1.3"}, new TLSProtocolVersion(tls13).getProtocols());
        Assert.assertArrayEquals(new String[] {"TLSv1.2", "TLSv1.3"}, new TLSProtocolVersion(tls1213).getProtocols());
        Assert.assertArrayEquals(new String[] {"TLSv1.3", "TLSv1.2"}, new TLSProtocolVersion(tls1312).getProtocols());
    }

    @Test
    public void passingInValidTlsProtocolVersionTest() {
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> new TLSProtocolVersion("TLSv1.1").getProtocols());
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> new TLSProtocolVersion("TLSv1_2").getProtocols());
        AssertExtensions.assertThrows(IllegalArgumentException.class, () -> new TLSProtocolVersion("TLSv1.4").getProtocols());
    }
}