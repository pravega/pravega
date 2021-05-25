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
package io.pravega.shared.security.token;


import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class JsonWebTokenTest {

    @Test
    public void testConstructionIsSuccessfulWithMinimalValidInput() {
        String token = new JsonWebToken("subject", "audience", "secret".getBytes()).toCompactString();
        assertNotNull(token);
        assertNotEquals("", token);
    }

    @Test
    public void testInputIsRejectedWhenMandatoryInputIsNull() {
        assertThrows(NullPointerException.class,
                () -> new JsonWebToken(null, "audience", "signingKeyString".getBytes())
        );

        assertThrows(NullPointerException.class,
                () -> new JsonWebToken("subject", null, "signingKeyString".getBytes())
        );

        assertThrows(NullPointerException.class,
                () -> new JsonWebToken("subject", "audience", null)
        );
    }

    @Test
    public void testCtorRejectsInvalidTtl() {
        assertThrows(IllegalArgumentException.class,
                () -> new JsonWebToken("subject", "audience", "signingKeyString".getBytes(),
                        null, -2)
        );
    }
}
