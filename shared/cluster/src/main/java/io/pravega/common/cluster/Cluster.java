/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.cluster;


import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Cluster interface enables to register / de-register a Host to a cluster.
 */
public interface Cluster extends AutoCloseable {

    /**
     * Register a Host to a cluster.
     *
     * @param host Host to be part of cluster.
     */
    public void registerHost(final Host host);

    /**
     * De-register a Host from a cluster.
     *
     * @param host Host to be removed from cluster.
     */
    public void deregisterHost(final Host host);

    /**
     * Add Listeners.
     *
     * @param listener Cluster event listener.
     */
    public void addListener(final ClusterListener listener);

    /**
     * Add Listeners with an executor to run the listener on.
     *
     * @param listener Cluster event listener.
     * @param executor Executor to run listener on.
     */
    public void addListener(final ClusterListener listener, final Executor executor);

    /**
     * Get the current cluster members.
     *
     * @return List of cluster members.
     */
    public Set<Host> getClusterMembers();

}
