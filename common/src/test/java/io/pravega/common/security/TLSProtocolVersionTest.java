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

        Assert.assertArrayEquals(new String[] {"TLSv1.2"}, TLSProtocolVersion.parse(tls12));
        Assert.assertArrayEquals(new String[] {"TLSv1.3"}, TLSProtocolVersion.parse(tls13));
        Assert.assertArrayEquals(new String[] {"TLSv1.2", "TLSv1.3"}, TLSProtocolVersion.parse(tls1213));
        Assert.assertArrayEquals(new String[] {"TLSv1.3", "TLSv1.2"}, TLSProtocolVersion.parse(tls1312));
    }

    @Test
    public void passingInValidTlsProtocolVersionTest() {
        Assert.assertNull(TLSProtocolVersion.parse("TLSv1.1"));
        Assert.assertNull(TLSProtocolVersion.parse("TLSv1_1"));
    }
}