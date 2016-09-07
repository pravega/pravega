/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.cluster;

import com.emc.pravega.cluster.zkutils.abstraction.ConfigChangeListener;
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManager;
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManagerFactory;
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManagerType;
import com.emc.pravega.cluster.zkutils.common.Endpoint;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClusterImpl  implements Cluster, ConfigChangeListener {
    private final ConcurrentHashMap<Endpoint, PravegaNode> nodes;
    private final ConcurrentHashMap<Endpoint, PravegaController> controllers;
    private final ConcurrentHashMap<String, ClusterListener> listeners;
    private final ConfigSyncManagerType syncType;
    private final String connectionString;
    private final int sessionTimeout;
    private ConfigSyncManager manager;

    public ClusterImpl(ConfigSyncManagerType syncType, String connectionString, int sessionTimeout) throws Exception {
        nodes = new ConcurrentHashMap<>();
        controllers = new ConcurrentHashMap<>();
        listeners = new ConcurrentHashMap<>();
        this.syncType = syncType;
        this.connectionString = connectionString;
        this.sessionTimeout = sessionTimeout;
        initializeCluster();
    }

    /**
     * Gets a list of nodes with the cluster class right now.
     *
     * @return
     */
    @Override
    public Iterable<PravegaNode> getPravegaNodes() {
        return new ConcurrentHashMap<>(nodes).values();
    }

    /**
     * Returns list of controllers available with the cluster class right now.
     *
     * @return
     */
    @Override
    public Iterable<PravegaController> getPravegaControllers() {
        return new ConcurrentHashMap<>(controllers).values();
    }

    /**
     * Returns list of listeners available with the cluster class right now.
     *
     * @return
     */
    @Override
    public Iterable<ClusterListener> getListeners() {
        return listeners.values();
    }

    /**
     * Initializes the cluster with a given config manager type
     *
     */
    private void initializeCluster() throws Exception {
        if (manager == null) {
            manager = new ConfigSyncManagerFactory().createManager(syncType, connectionString,
                    sessionTimeout, this);
        }
        refreshCluster();
    }

    /**
     * Reads the complete cluster from the store and updates the existing data
     **/
    @Override
    public void refreshCluster() throws Exception {
        nodes.clear();
        controllers.clear();
        manager.refreshCluster();
    }

    /**
     * Private method to add a node and notify all listeners
     *
     * @param node
     * @param endpoint
     */
    private void addNode(PravegaNode node, Endpoint endpoint) {
        nodes.put(endpoint, node);
        listeners.forEach(
                (name, listener) -> {
                    try {
                        listener.nodeAdded(node);
                    } catch (Exception e) {
                        log.warn("Listener {} threw an exception while handling add node.", name, e);
                    }
                });
    }

    /**
     * Private method to add a controller and notify all listeners
     *
     * @param controller
     * @param endpoint
     */
    private void addController(PravegaController controller, Endpoint endpoint) {
        controllers.put(endpoint, controller);
        listeners.forEach(
                (name, listener) -> {
                    try {
                        listener.controllerAdded(controller);
                    } catch (Exception e) {
                        log.warn("Listener {} threw an exception while handling add controller.", name, e);
                    }
                });
    }

    /**
     * Private method to remove a controller and notify all the listeners
     *
     * @param endpoint
     */
    private void removeController(Endpoint endpoint) {
        PravegaController controller = controllers.remove(endpoint);
        listeners.forEach(
                (name, listener) -> {
                    try {
                        listener.controllerRemoved(controller);
                    } catch (Exception e) {
                        log.warn("Listener {} threw an exception while handling remove controller.", name, e);
                    }
                });
    }

    /**
     * Private method to remove a node and notify all the listeners
     *
     * @param endpoint
     */
    private void removeNode(Endpoint endpoint) {
        PravegaNode node = nodes.remove(endpoint);
        listeners.forEach(
                (name, listener) -> {
                    try {
                        listener.nodeRemoved(node);
                    } catch (Exception e) {
                        log.warn("Listener {} threw an exception while handling remove node.", name, e);
                    }
                });
    }

    /**
     * Registers the current Pravega node with a specific hostname and port with the config store
     *
     * @param host
     * @param port
     * @param jsonMetadata
     */
    @Override
    public void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception {
        manager.registerPravegaNode(host, port, jsonMetadata);
    }

    /**
     * Registers the current Pravega controller with a specific hostname and port with the config store
     *
     * @param host
     * @param port
     * @param jsonMetadata
     */
    @Override
    public void registerPravegaController(String host, int port, String jsonMetadata) throws Exception {
        manager.registerPravegaController(host, port, jsonMetadata);
    }

    /**
     * Unregisters the current Pravega controller with a specific hostname and port with the config store
     *
     * @param host
     * @param port
     */
    @Override
    public void unregisterPravegaController(String host, int port) throws Exception {
        Preconditions.checkNotNull(host);
        manager.unregisterPravegaController(host, port);
    }

    /**
     * Unregisters the current Pravega node with a specific hostname and port with the config store
     *
     * @param host
     * @param port
     */
    @Override
    public void unregisterPravegaNode(String host, int port) throws Exception {
        Preconditions.checkNotNull(host);
        manager.unregisterPravegaNode(host, port);
    }

    /**
     * Listener specific functions:
     * Registers a new listener
     *
     * @param clusterListener
     */
    @Override
    public void registerListener(String name, ClusterListener clusterListener) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(clusterListener);

        listeners.put(name, clusterListener);
    }

    /**
     * Listener specific functions:
     * Removes a registered listener
     *
     * @param clusterListener
     */
    @Override
    public void unregisterListener(ClusterListener clusterListener) {
        Preconditions.checkNotNull(clusterListener);
        listeners.remove(clusterListener);
    }

    /**
     * Notification from the config manager abstraction to notify a node is added
     *
     * @param ep
     */
    @Override
    public void nodeAddedNotification(Endpoint ep) {
        addNode(new PravegaNode(ep), ep);
    }

    /**
     * Notification from config manager abstraction to notify a controller is added
     *
     * @param ep
     */
    @Override
    public void controllerAddedNotification(Endpoint ep) {
        addController(new PravegaController(ep), ep);
    }

    /**
     * Notification from the config manager abstraction to notify a node is removed
     *
     * @param ep
     */
    @Override
    public void nodeRemovedNotification(Endpoint ep) {
        removeNode(ep);
    }

    /**
     * Notification from config manager abstraction to notify a controller is removed
     *
     * @param ep
     */
    @Override
    public void controllerRemovedNotification(Endpoint ep) {
        removeController(ep);
    }

}
