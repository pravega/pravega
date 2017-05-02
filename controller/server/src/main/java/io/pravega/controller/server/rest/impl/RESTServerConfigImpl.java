/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.controller.server.rest.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.server.rest.RESTServerConfig;
import lombok.Builder;
import lombok.Getter;

/**
 * REST server config.
 */
@Getter
public class RESTServerConfigImpl implements RESTServerConfig {
    private final String host;
    private final int port;

    @Builder
    RESTServerConfigImpl(final String host, final int port) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(port > 0, "port", "Should be positive integer");

        this.host = host;
        this.port = port;
    }
}
