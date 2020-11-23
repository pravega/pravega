/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.auth;

import java.util.Properties;

/**
 * Represents the configuration object passed down to the custom plugin.
 */
public interface ServerConfig {

    /**
     * Returns the configuration items related to {@link io.pravega.auth.AuthHandler}
     * in this object in a {@link Properties} format.
     *
     * @return configuration properties
     */
    Properties toAuthHandlerProperties();
}
