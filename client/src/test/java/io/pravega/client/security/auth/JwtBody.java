/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security.auth;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents a JWT body for serialization/deserialization purposes.
 */
@Builder
@Getter
@Setter
class JwtBody {

    // See https://tools.ietf.org/html/rfc7519#page-9 for additional details about these fields.

    /**
     * The "sub" (for subject) claim of the JWT body.
     */
    @SerializedName("sub")
    private final String subject;

    /**
     * The "aud" (for audience) claim of the JWT body.
     */
    @SerializedName("aud")
    private final String audience;

    /**
     * The "iat" (for issued at) claim of the JWT body.
     */
    @SerializedName("iat")
    private final Long issuedAtTime;

    /**
     * The "exp" (for expiration time) claim of the JWT body. It identifies the time on or after which the JWT must not
     * be accepted for processing. The value represents seconds past 1970-01-01 00:00:00Z.
     */
    @SerializedName("exp")
    private final Long expirationTime;
}

