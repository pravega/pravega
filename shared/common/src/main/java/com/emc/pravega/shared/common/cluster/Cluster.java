/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.shared.common.cluster;


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
     * @throws Exception Error while adding ClusterListener.
     */
    public void addListener(final ClusterListener listener) throws Exception;

    /**
     * Add Listeners with an executor to run the listener on.
     *
     * @param listener Cluster event listener.
     * @param executor Executor to run listener on.
     * @throws Exception Error while adding ClusterListener.
     */
    public void addListener(final ClusterListener listener, final Executor executor) throws Exception;

    /**
     * Get the current cluster members.
     *
     * @return List of cluster members.
     * @throws Exception Error while getting Cluster members.
     */
    public Set<Host> getClusterMembers() throws Exception;

}
