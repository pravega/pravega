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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;

/**
 * Represents a JWT body for serialization/deserialization purposes.
 */
@Builder
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JwtBody {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // See https://tools.ietf.org/html/rfc7519#page-9 for additional details about these fields.

    /**
     * The "sub" (for subject) claim of the JWT body.
     */
    @JsonProperty("sub")
    private final String subject;

    /**
     * The "aud" (for audience) claim of the JWT body.
     */
    @JsonProperty("aud")
    private final String audience;

    /**
     * The "iat" (for issued at) claim of the JWT body.
     */
    @JsonProperty("iat")
    private final Long issuedAtTime;

    /**
     * The "exp" (for expiration time) claim of the JWT body. It identifies the time on or after which the JWT must not
     * be accepted for processing. The value represents seconds past 1970-01-01 00:00:00Z.
     */
    @JsonProperty("exp")
    private final Long expirationTime;

    @SneakyThrows
    @Override
    public String toString() {
        return MAPPER.writeValueAsString(this);
    }

    @SneakyThrows
    public static JwtBody fromJson(String json) {
        return MAPPER.readValue(json.getBytes(StandardCharsets.UTF_8), JwtBody.class);
    }
}
