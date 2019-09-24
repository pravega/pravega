/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
class Jwt {

    private String sub;

    private String aud;

    private Integer iat;

    private Integer exp;
}

