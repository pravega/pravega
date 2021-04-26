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
package io.pravega.segmentstore.server.host.delegationtoken;

import io.pravega.auth.InvalidClaimException;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.shared.security.token.JsonWebToken;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;
import static io.pravega.test.common.AssertExtensions.assertThrows;

public class TokenVerifierImplTest {

    @Test
    public void testTokenVerificationFailsWhenTokenIsNull() {
        DelegationTokenVerifier tokenVerifier = new TokenVerifierImpl("secret");

        assertThrows(InvalidTokenException.class,
                () -> tokenVerifier.verifyToken("dummy", null, READ)
        );
    }

    @Test
    public void testTokenVerificationFailsWhenTokenIsExpired() {
        DelegationTokenVerifier tokenVerifier = new TokenVerifierImpl("secret");
        String tokenWithExpiry = prepareJwt("prn::*, READ_UPDATE", 0);

        assertThrows(TokenExpiredException.class,
                () -> tokenVerifier.verifyToken("dummy/resource", tokenWithExpiry, READ)
        );
    }

    @Test
    public void testTokenVerificationSucceedsWhenTokenIsNotExpired() throws TokenException {
        DelegationTokenVerifier tokenVerifier = new TokenVerifierImpl("secret");
        String tokenWithExpiry = prepareJwt("prn::*, READ_UPDATE", Integer.MAX_VALUE);

        // No exception is expected here.
        tokenVerifier.verifyToken("dummy/resource", tokenWithExpiry, READ_UPDATE);
    }

    @Test
    public void testTokenVerificationFailsWhenTokenDoesNotContainMatchingResource() {
        DelegationTokenVerifier tokenVerifier = new TokenVerifierImpl("secret");
        String tokenWithClaim = prepareJwt("prn::/scope:abc, READ_UPDATE", Integer.MAX_VALUE);

        assertThrows(InvalidClaimException.class,
                () -> tokenVerifier.verifyToken("xyz", tokenWithClaim, READ)
        );
    }

    @Test
    public void testTokenVerificationFailsWhenTokenDoesNotContainMatchingPermission() {
        DelegationTokenVerifier tokenVerifier = new TokenVerifierImpl("secret");
        String tokenWithClaim = prepareJwt("prn::/scope:abc, READ", Integer.MAX_VALUE);

        assertThrows(InvalidClaimException.class,
                () -> tokenVerifier.verifyToken("abc", tokenWithClaim, READ_UPDATE)
        );
    }

    @Test
    public void testTokenVerificationSucceedsWhenTokenContainsMatchingClaim() throws TokenException {
        DelegationTokenVerifier tokenVerifier = new TokenVerifierImpl("secret");

        String tokenWithWildcardClaim = prepareJwt("prn::*, READ_UPDATE", Integer.MAX_VALUE);
        tokenVerifier.verifyToken("xyz", tokenWithWildcardClaim, READ);

        String tokenWithSpecificClaim = prepareJwt("prn::/scope:abc, READ_UPDATE", Integer.MAX_VALUE);
        tokenVerifier.verifyToken("abc", tokenWithSpecificClaim, READ);
    }

    @Test
    public void testTokenVerificationSuceedsForInternalSegments() throws TokenException {
        DelegationTokenVerifier tokenVerifier = new TokenVerifierImpl("secret");

        String token = prepareJwt("prn::/scope:_system/stream:_requeststream, READ_UPDATE", null);
        tokenVerifier.verifyToken("_system/_requeststream/0.#epoch.0", token, READ);
    }

    private String prepareJwt(String acl, Integer ttlInSeconds) {
        return prepareJwt(Arrays.asList(acl), ttlInSeconds);
    }

    private String prepareJwt(List<String> acls, Integer ttlInSeconds) {
        Map<String, Object> permissionsByResource = new HashMap<>();
        for (String acl: acls) {
            String[] aclContent = acl.split(",");
            String resource = aclContent[0].trim();
            String permission = aclContent[1].trim();
            permissionsByResource.put(resource, permission);
        }
        JsonWebToken token = new JsonWebToken("segmentstoreresource",
                "segmentstore", "secret".getBytes(), permissionsByResource, ttlInSeconds);
        return token.toCompactString();
    }
}