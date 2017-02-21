/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.cluster.zkImpl;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.ClusterListener;
import com.emc.pravega.common.cluster.Host;
import com.google.common.base.Preconditions;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.emc.pravega.common.cluster.ClusterListener.EventType.ERROR;
import static com.emc.pravega.common.cluster.ClusterListener.EventType.HOST_ADDED;
import static com.emc.pravega.common.cluster.ClusterListener.EventType.HOST_REMOVED;

/**
 * Zookeeper based implementation of Cluster.
 * - It uses persistent ephemeral node which is an ephemeral node that attempts to stay present in ZooKeeper, even through
 * connection and session interruptions.
 * - Ephemeral Node is valid until a session timeout, default session timeout is 60 seconds.
 * System property "curator-default-session-timeout" can be used to change it.
 */
@Slf4j
public class ClusterZKImpl implements Cluster {

    private final static String PATH_CLUSTER = "/cluster/";
    private final static String HOSTS = "hosts";
    private final static int INIT_SIZE = 3;

    private final CuratorFramework client;
    private final String clusterName;

    private final Map<Host, PersistentNode> entryMap = new HashMap<>(INIT_SIZE);
    private Optional<PathChildrenCache> cache = Optional.empty();

    public ClusterZKImpl(CuratorFramework zkClient, String clusterName) {
        this.clusterName = clusterName;
        this.client = zkClient;
        if (client.getState().equals(CuratorFrameworkState.LATENT)) {
            client.start();
        }
    }

    /**
     * Register Host to cluster.
     *
     * @param host Host to be part of cluster.
     */
    @Override
    @Synchronized
    public void registerHost(Host host) {
        Preconditions.checkNotNull(host, "host");
        Exceptions.checkArgument(!entryMap.containsKey(host), "host", "host is already registered to cluster.");

        String hostPath = ZKPaths.makePath(PATH_CLUSTER, clusterName, HOSTS, host.getIpAddr() + ":" + host.getPort());
        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, hostPath,
                SerializationUtils.serialize(host));

        node.start(); //start creation of ephemeral node in background.
        entryMap.put(host, node);
    }

    /**
     * Remove Host from cluster.
     *
     * @param host Host to be removed from cluster.
     */
    @Override
    @Synchronized
    public void deregisterHost(Host host) {
        Preconditions.checkNotNull(host, "host");
        PersistentNode node = entryMap.get(host);
        Preconditions.checkNotNull(node, "Host is not present in cluster.");
        entryMap.remove(host);
        close(node);
    }

    /**
     * Add Listener to the cluster.
     *
     * @param listener Cluster event Listener.
     * @throws Exception Error while communicating to Zookeeper.
     */
    @Override
    @Synchronized
    public void addListener(ClusterListener listener) throws Exception {
        Preconditions.checkNotNull(listener, "listener");
        if (!cache.isPresent()) {
            initializeCache();
        }
        cache.get().getListenable().addListener(pathChildrenCacheListener(listener));
    }

    /**
     * Add Listener to the cluster.
     *
     * @param listener Cluster event Listener.
     * @param executor Executor to run the listener on.
     * @throws Exception Error while communicating to Zookeeper.
     */
    @Override
    @Synchronized
    public void addListener(final ClusterListener listener, final Executor executor) throws Exception {
        Preconditions.checkNotNull(listener, "listener");
        Preconditions.checkNotNull(executor, "executor");
        if (!cache.isPresent()) {
            initializeCache();
        }
        cache.get().getListenable().addListener(pathChildrenCacheListener(listener), executor);
    }

    /**
     * Get the current cluster members.
     *
     * @return List of cluster members.
     * @throws Exception Error while communicating to Zookeeper.
     */
    @Override
    @Synchronized
    public Set<Host> getClusterMembers() throws Exception {
        if (!cache.isPresent()) {
            initializeCache();
        }
        List<ChildData> data = cache.get().getCurrentData();
        return data.stream()
                .map(d -> (Host) SerializationUtils.deserialize(d.getData()))
                .collect(Collectors.toSet());
    }

    @Override
    public void close() throws Exception {
        synchronized (entryMap) {
            entryMap.values().forEach(this::close);
            cache.ifPresent(this::close);
        }
    }

    private void close(Closeable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (IOException e) {
            log.error("Error while closing resource", e);
        }
    }

    private void initializeCache() throws Exception {
        cache = Optional.of(new PathChildrenCache(client, ZKPaths.makePath(PATH_CLUSTER, clusterName, HOSTS), true));
        cache.get().start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    }

    private PathChildrenCacheListener pathChildrenCacheListener(final ClusterListener listener) {
        return (client, event) -> {
            log.debug("Event {} generated on cluster:{}", event, clusterName);
            switch (event.getType()) {
                case CHILD_ADDED:
                    log.info("Node {} added to cluster:{}", getServerName(event), clusterName);
                    listener.onEvent(HOST_ADDED, (Host) SerializationUtils.deserialize(event.getData().getData()));
                    break;
                case CHILD_REMOVED:
                    log.info("Node {} removed from cluster:{}", getServerName(event), clusterName);
                    listener.onEvent(HOST_REMOVED, (Host) SerializationUtils.deserialize(event.getData().getData()));
                    break;
                case CHILD_UPDATED:
                    log.warn("Invalid usage: Node {} updated externally for cluster:{}", getServerName(event), clusterName);
                    break;
                case CONNECTION_LOST:
                    log.error("Connection lost with Zookeeper");
                    listener.onEvent(ERROR, null);
                    break;
                default:
                    log.warn("Received unknown event {}", event.getType());
            }
        };
    }

    private String getServerName(final PathChildrenCacheEvent event) {
        String path = event.getData().getPath();
        return path.substring(path.lastIndexOf("/") + 1);
    }
}
