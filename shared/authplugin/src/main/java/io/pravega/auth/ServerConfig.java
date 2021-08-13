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
package io.pravega.auth;

import java.util.Properties;

/**
 * Represents the configuration object passed down to the custom plugin.
 */
public interface ServerConfig {

    /**
     * Fetches the settings which indicates whether authorization is enabled.
     *
     * @return Whether this deployment has auth enabled.
     */
    default boolean isAuthorizationEnabled() {
        return false;
    }

    /**
     * Returns the configuration items related to {@link io.pravega.auth.AuthHandler}
     * in this object in a {@link Properties} format.
     *
     * @return configuration properties
     */
    Properties toAuthHandlerProperties();
}
