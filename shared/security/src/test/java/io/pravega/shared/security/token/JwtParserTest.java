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

import io.jsonwebtoken.Claims;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import org.junit.Test;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JwtParserTest {

    @Test
    public void testParseClaimsPopulatesTokenWithAllInputs() throws TokenException {
        Map<String, Object> customClaims = new HashMap<>();
        customClaims.put("abc", "READ_UPDATE");

        String token =  new JsonWebToken("subject", "audience", "signingKeyString".getBytes(),
                customClaims, Integer.MAX_VALUE)
                .toCompactString();

        Claims allClaims = JwtParser.parseClaims(token, "signingKeyString".getBytes());

        assertEquals("subject", allClaims.getSubject());
        assertEquals("audience", allClaims.getAudience());
    }

    @Test
    public void testParsePopulatesTokenFully() {
        Map<String, Object> customClaims = new HashMap<>();
        customClaims.put("abc", "READ_UPDATE");

        Date expirationDateTime = Date.from(Instant.now().plusSeconds(1000));
        String token =  new JsonWebToken("subject", "audience", "signingKeyString".getBytes(),
                expirationDateTime, customClaims).toCompactString();

        JsonWebToken jwt = JwtParser.parse(token, "signingKeyString".getBytes());
        assertEquals("subject", jwt.getSubject());
        assertEquals("audience", jwt.getAudience());
        assertEquals(expirationDateTime.toString(), jwt.getExpirationTime().toString());
        assertEquals("READ_UPDATE", jwt.getPermissionsByResource().get("abc"));
    }

    @Test
    public void testTokenDoesNotContainExpiryWhenTtlIsMinusOne() throws TokenException {
        String token =  new JsonWebToken("subject", "audience", "signingKeyString".getBytes(), null, -1)
                .toCompactString();

        Claims allClaims = JwtParser.parseClaims(token, "signingKeyString".getBytes());
        assertNull(allClaims.getExpiration());
    }

    @Test
    public void testParseClaimsThrowsExceptionWhenTokenIsNull() {
        assertThrows(InvalidTokenException.class,
                () ->  JwtParser.parseClaims(null, "token".getBytes())
        );
    }

    @Test
    public void testParseClaimsThrowsExceptionWhenSigningKeyIsNull() throws TokenException {
        assertThrows(IllegalArgumentException.class,
                () ->  JwtParser.parseClaims("abx.mno.xyz", null)
        );
    }

    @Test
    public void testParseClaimsThrowsExceptionWhenSigningKeyIsInvalid() {
        assertThrows(InvalidTokenException.class,
                () ->  JwtParser.parseClaims("abx.mno.xyz", "abc".getBytes())
        );
    }
}
