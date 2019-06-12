/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.delegationtoken;

import io.pravega.auth.InvalidClaimException;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.shared.security.token.JsonWebToken;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static io.pravega.auth.AuthHandler.Permissions.READ;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TokenVerifierImplTest {

    @Test
    public void testTokenVerificationSucceedsWhenAuthIsDisabled() throws TokenException {
        DelegationTokenVerifier tokenVerifier = prepareTokenVerifier(false);

        // No exception is expected here.
        tokenVerifier.verifyToken("xyz", null, READ);
        assertTrue(tokenVerifier.isTokenValid("xyz", null, READ));
    }

    @Test
    public void testTokenVerificationFailsWhenTokenIsNull() {
        DelegationTokenVerifier tokenVerifier = prepareTokenVerifier(true);

        assertThrows(InvalidTokenException.class,
                () -> tokenVerifier.verifyToken("xyz", null, READ)
        );
        assertFalse(tokenVerifier.isTokenValid("xyz", null, READ));
    }

    @Test
    public void testTokenVerificationFailsWhenTokenIsExpired() {
        DelegationTokenVerifier tokenVerifier = prepareTokenVerifier(true);
        String tokenWithExpiry = prepareJwt("*, READ_UPDATE", 0);

        assertThrows(TokenExpiredException.class,
                () -> tokenVerifier.verifyToken("xyz", tokenWithExpiry, READ)
        );
        assertFalse(tokenVerifier.isTokenValid("xyz", tokenWithExpiry, READ));
    }

    @Test
    public void testTokenVerificationSucceedsWhenTokenIsNotExpired() throws TokenException {
        DelegationTokenVerifier tokenVerifier = prepareTokenVerifier(true);
        String tokenWithExpiry = prepareJwt("*, READ_UPDATE", Integer.MAX_VALUE);

        // No exception is expected here.
        tokenVerifier.verifyToken("s1", tokenWithExpiry, READ_UPDATE);
        assertTrue(tokenVerifier.isTokenValid("s1", tokenWithExpiry, READ_UPDATE));
    }

    @Test
    public void testTokenVerificationFailsWhenTokenDoesNotContainMatchingResource() {
        DelegationTokenVerifier tokenVerifier = prepareTokenVerifier(true);
        String tokenWithClaim = prepareJwt("abc, READ_UPDATE", Integer.MAX_VALUE);

        assertThrows(InvalidClaimException.class,
                () -> tokenVerifier.verifyToken("xyz", tokenWithClaim, READ)
        );
        assertFalse(tokenVerifier.isTokenValid("xyz", tokenWithClaim, READ));
    }

    @Test
    public void testTokenVerificationFailsWhenTokenDoesNotContainMatchingPermission() {
        DelegationTokenVerifier tokenVerifier = prepareTokenVerifier(true);
        String tokenWithClaim = prepareJwt("abc, READ", Integer.MAX_VALUE);

        assertThrows(InvalidClaimException.class,
                () -> tokenVerifier.verifyToken("abc", tokenWithClaim, READ_UPDATE)
        );
        assertFalse(tokenVerifier.isTokenValid("xyz", tokenWithClaim, READ));
    }

    @Test
    public void testTokenVerificationSucceedsWhenTokenContainsMatchingClaim() throws TokenException {
        DelegationTokenVerifier tokenVerifier = prepareTokenVerifier(true);

        String tokenWithWildcardClaim = prepareJwt("*, READ_UPDATE", Integer.MAX_VALUE);
        tokenVerifier.verifyToken("xyz", tokenWithWildcardClaim, READ);
        assertTrue(tokenVerifier.isTokenValid("xyz", tokenWithWildcardClaim, READ));

        String tokenWithSpecificClaim = prepareJwt("abc, READ_UPDATE", Integer.MAX_VALUE);
        tokenVerifier.verifyToken("abc", tokenWithSpecificClaim, READ);
        assertTrue(tokenVerifier.isTokenValid("abc", tokenWithSpecificClaim, READ));
    }

    @Test
    public void testTokenVerificationSuceedsForInternalSegments() throws TokenException {
        DelegationTokenVerifier tokenVerifier = prepareTokenVerifier(true);

        String token = prepareJwt("_system/_requeststream, READ_UPDATE", null);
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
                "segmentstore", "secret".getBytes(), ttlInSeconds, permissionsByResource);
        return token.toCompactString();
    }

    private DelegationTokenVerifier prepareTokenVerifier(boolean isAuthEnabled) {
        AutoScalerConfig config = AutoScalerConfig.builder()
                .with(AutoScalerConfig.AUTH_ENABLED, isAuthEnabled)
                .with(AutoScalerConfig.TOKEN_SIGNING_KEY, "secret")
                .build();
        return new TokenVerifierImpl(config);
    }
}