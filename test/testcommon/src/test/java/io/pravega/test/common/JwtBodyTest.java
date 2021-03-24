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
package io.pravega.test.common;

import org.junit.Test;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JwtBodyTest {

    @Test
    public void testSerialize() {
        JwtBody jwtBody = JwtBody.builder()
                .subject("testsubject")
                .audience("segmentstore")
                .issuedAtTime(Instant.now().getEpochSecond())
                .expirationTime(Instant.now().plusSeconds(50).getEpochSecond())
                .build();
        String jwtBodyJson = jwtBody.toString();
        assertTrue(jwtBodyJson.contains("testsubject"));
        assertTrue(jwtBodyJson.contains("segmentstore"));
    }

    @Test
    public void testDeserialize() {
        String json = "{\"sub\":\"subject\",\"aud\":\"segmentstore\",\"iat\":1569837384,\"exp\":1569837434}";
        JwtBody jwtBody = JwtBody.fromJson(json);

        assertEquals("subject", jwtBody.getSubject());
        assertEquals("segmentstore", jwtBody.getAudience());
        assertEquals(Long.valueOf(1569837384), jwtBody.getIssuedAtTime());
        assertEquals(Long.valueOf(1569837434), jwtBody.getExpirationTime());
    }
}
