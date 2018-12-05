/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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
     *  @return List of {@link URI}s where the service is running.
     */
    public List<URI> getServiceDetails();

    /**
     * Scale service to the new instance count.
     *
     * Increasing instance count will result in new deployments while decreasing the instance count will result in
     * killing of running instances.
     *
     * An instance count of zero would suspend the service.
     * @param instanceCount new instance count for the service.
     * @return A future representing the status of scale service operation.
     *
     */
    public CompletableFuture<Void> scaleService(final int instanceCount);
}
