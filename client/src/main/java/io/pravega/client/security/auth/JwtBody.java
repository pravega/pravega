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

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
class JwtBody {

    // See https://tools.ietf.org/html/rfc7519#page-9 for additional details about these fields.

    /**
     * The "sub" (for subject) claim of the JWT body.
     */
    private final String sub;

    /**
     * The "aud" (for audience) claim of the JWT body.
     */
    private final String aud;

    /**
     * The "iat" (for issued at) claim of the JWT body.
     */
    private final Long iat;

    /**
     * The "exp" (for expiration time) claim of the JWT body. It identifies the time on or after which the JWT must not
     * be accepted for processing. The value represents seconds past 1970-01-01 00:00:00Z.
     */
    private final Long exp;
}

