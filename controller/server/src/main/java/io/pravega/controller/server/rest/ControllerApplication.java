/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
