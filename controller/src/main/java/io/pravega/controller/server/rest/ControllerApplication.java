/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest;

import javax.ws.rs.core.Application;
import java.util.Set;

/**
 * Application to register the REST resource classes.
 */
public class ControllerApplication extends Application {
    private final Set<Object> resource;

    public ControllerApplication(final Set<Object> resources) {
        super();
        resource = resources;
    }

    /**
     * Get a set of root resources.
     *
     * @return a set of root resource instances.
     */
    @Override
    public Set<Object> getSingletons() {
        return resource;
    }
}
