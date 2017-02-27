/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.framework.services;

import java.net.URI;
import java.util.List;

/**
 * Service abstraction for the test framework. T
 * System tests use Service apis to configure, fetch deployment details and get the current status of the service.
 * .
 */
public interface Service {

    /**
     * Start a given service.
     *
     *  @param wait true indicates that it is a blocking call.
     */
    public void start(final boolean wait);

    /**
     * Stop a service.
     */
    public void stop();

    /**
     * Clean the service.
     */
    public void clean();

    /**
     * Return the ID of the service.
     *
     * @return ID of the service.
     */
    public String getID();

    /**
     * Check if the service is up and running.
     *
     *  @return true if the service is running.
     */
    public boolean isRunning();

    /**
     * Get the list of Host:port URIs where the service is running.
     *
     *  @return List<URI> list of Host:port where the service is running.
     */
    public List<URI> getServiceDetails();

}
