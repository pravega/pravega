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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.InvalidClaimException;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.common.Exceptions;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import io.pravega.shared.security.token.JsonWebToken;
import io.pravega.shared.security.token.JwtParser;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TokenVerifierImpl implements DelegationTokenVerifier {

    private static final Pattern RESOURCE_PARTS_TO_REPLACE = Pattern.compile("prn::|/scope:|stream:");

    private final byte[] tokenSigningKey;

    @VisibleForTesting
    public TokenVerifierImpl(String tokenSigningKeyBasis) {
        Exceptions.checkNotNullOrEmpty(tokenSigningKeyBasis, "tokenSigningKeyBasis");
        this.tokenSigningKey = tokenSigningKeyBasis.getBytes();
    }

    @Override
    public JsonWebToken verifyToken(@NonNull String resource, String token, @NonNull AuthHandler.Permissions expectedLevel)
            throws TokenExpiredException, InvalidTokenException, InvalidClaimException, TokenException {

        if (Strings.isNullOrEmpty(token)) {
            throw new InvalidTokenException("Token is null or empty");
        }

        // All key value pairs inside the payload are returned, including standard fields such as sub (for subject),
        // aud (for audience), iat, exp, as well as custom fields of the form "<resource> -> <permission>" set by
        // Pravega.
        JsonWebToken jwt = JwtParser.parse(token, tokenSigningKey);
        Map<String, Object> permissionsByResource = jwt.getPermissionsByResource();

        Optional<Map.Entry<String, Object>> matchingClaim = permissionsByResource.entrySet().stream()
                    .filter(entry -> resourceMatchesClaimKey(entry.getKey(), resource) &&
                                     expectedLevel.compareTo(AuthHandler.Permissions.valueOf(entry.getValue().toString())) <= 0)
                    .findFirst();

        if (!matchingClaim.isPresent()) {
            log.debug(String.format("No matching claim found for resource [%s] and permission [%s] in token.",
                        resource, expectedLevel));

            throw new InvalidClaimException(String.format(
                        "No matching claim found for resource: [%s] and permission: [%s] in the delegation token.",
                        resource, expectedLevel));
        }
        return jwt;
     }

    /**
     * Returns whether the specified resource} string matches the given claim key.
     *
     * @param claimKey
     * @param resource
     * @return
     */
    private boolean resourceMatchesClaimKey(String claimKey, String resource) {
        log.trace("claimKey = {}, resourceKey = {}", claimKey, resource);

        // Replace `prn::`, `/scope:` and `stream:` with an empty string.
        claimKey = RESOURCE_PARTS_TO_REPLACE.matcher(claimKey).replaceAll("");

        /*
         * Examples of the conditions when the claimKey (key of the key-value pair claim) matches the resource are:
         *      1) claimKey = "myscope", resource = "myscope"
         *      2) claimKey = "abc/", resource = "abc/xyx"
         *      3) claimKey = "_system/_requeststream", resource = "_system/_requeststream/0.#epoch.0"
         *      4) claimKey = "*" (the wildcard character)
         */
        boolean result = resource.equals(claimKey) // example 1
                || claimKey.endsWith("/") && resource.startsWith(claimKey) // example 2
                || resource.startsWith(claimKey + "/") // example 3
                || claimKey.equals("*");
        log.trace("claimKey: [{}], resource: [{}], result: [{}]", claimKey, resource, result);
        return result;
    }
}
