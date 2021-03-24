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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.control.impl.Controller;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.JwtBody;
import java.time.Instant;
import org.junit.Test;

import static io.pravega.test.common.JwtTestUtils.createEmptyDummyToken;
import static io.pravega.test.common.JwtTestUtils.toCompact;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DelegationTokenProviderFactoryTest {

    private Controller dummyController = mock(Controller.class);

    @Test
    public void testCreateWithEmptyToken() {
       DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.createWithEmptyToken();
       assertEquals("", tokenProvider.retrieveToken().join());
       assertFalse(tokenProvider.populateToken("new-token"));
    }

    @Test
    public void testCreateWithNullInputThrowsException() {
        AssertExtensions.assertThrows("Null controller argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(null, "test-scope", "test-stream", AccessOperation.ANY),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null Controller argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(createEmptyDummyToken(), null, mock(Segment.class), AccessOperation.ANY),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null scopeName argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, null, "test-stream", AccessOperation.ANY),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null streamName argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, "test-scope", null, AccessOperation.ANY),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null segment argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, null, AccessOperation.ANY),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null segment argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(createEmptyDummyToken(), dummyController, null, AccessOperation.ANY),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testCreateWithEmptyInputThrowsException() {
        AssertExtensions.assertThrows("Empty scopeName argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, "", "test-stream", AccessOperation.ANY),
                e -> e instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("Empty streamName argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, "test-scope", "", AccessOperation.ANY),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void testCreateWithNonJwtToken() {
        String nonJwtDelegationToken = "non-jwt-token";
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(nonJwtDelegationToken,
                dummyController, new Segment("test-scope", "test-stream", 1), AccessOperation.ANY);
        assertEquals(nonJwtDelegationToken, tokenProvider.retrieveToken().join());

        String newNonJwtDelegationToken = "new-non-jwt-token";
        tokenProvider.populateToken(newNonJwtDelegationToken);
        assertEquals("new-non-jwt-token", tokenProvider.retrieveToken().join());
    }

    @Test
    public void testCreateWithJwtToken() {
        String jwtDelegationToken = String.format("base-64-encoded-header.%s.base-64-encoded-signature", toCompact(
                JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(jwtDelegationToken,
                dummyController, new Segment("test-scope", "test-stream", 1), AccessOperation.ANY);
        assertEquals(jwtDelegationToken, tokenProvider.retrieveToken().join());
    }

    @Test
    public void testCreateWithNullDelegationToken() {
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(null,
                dummyController, new Segment("test-scope", "test-stream", 1), AccessOperation.ANY);
        assertTrue(tokenProvider instanceof JwtTokenProviderImpl);

        tokenProvider = DelegationTokenProviderFactory.create(dummyController, "test-scope", "test-stream", AccessOperation.ANY);
        assertTrue(tokenProvider instanceof JwtTokenProviderImpl);
    }
}
