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
package io.pravega.client.security.auth;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DelegationTokenTest {

    @Test
    public void testEquals() {
        DelegationToken token1 = new DelegationToken("token1", 123L);
        DelegationToken token2 = new DelegationToken("token1", 123L);
        DelegationToken token3 = new DelegationToken("token2", null);
        assertEquals(token1, token2);
        assertNotEquals(token1, token3);
    }

    @Test
    public void testHashCode() {
        DelegationToken token1 = new DelegationToken("token1", 123L);
        DelegationToken token2 = new DelegationToken("token1", 123L);
        DelegationToken token3 = new DelegationToken("token1", null);
        assertEquals(token1.hashCode(), token2.hashCode());
        assertNotEquals(token1.hashCode(), token3.hashCode());
    }
}
