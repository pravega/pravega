/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.cluster.zkImpl;

import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.ClusterListener;
import com.emc.pravega.common.cluster.Host;
import org.apache.commons.lang.NotImplementedException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Memory based implementation of Cluster.
 */
@ThreadSafe
public class InMemoryClusterImpl implements Cluster {
    private final Set<Host> clusterMembers;

    public InMemoryClusterImpl(final Host host) {
        clusterMembers = Collections.singleton(host);
    }

    @Override
    public void registerHost(Host host) {
        throw new NotImplementedException();
    }

    @Override
    public void deregisterHost(Host host) {
        throw new NotImplementedException();
    }

    @Override
    public void addListener(ClusterListener listener) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public void addListener(ClusterListener listener, Executor executor) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public Set<Host> getClusterMembers() throws Exception {
        return clusterMembers;
    }

    @Override
    public void close() throws Exception {
        throw new NotImplementedException();
    }
}
