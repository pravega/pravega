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
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManagerCreator;
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManagerType;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a pravega cluster.
 * A pravega cluster contains a number of pravega nodes and controller nodes.
 * Each node instance is uniquely identified by an Endpoint class.
 *
 * An Endpoint class represents a server IP and port on which either the
 * pravega node Or the controller listens
 */
@Slf4j
public final class Cluster implements ConfigChangeListener {
    private ConcurrentHashMap<Endpoint, PravegaNode> nodes;
    private ConcurrentHashMap<Endpoint, PravegaController> controllers;
    private ConcurrentHashMap<String, ClusterListener> listeners;
    private ConfigSyncManager manager;

    public Cluster() {
        nodes        = new ConcurrentHashMap<>();
        controllers  = new ConcurrentHashMap<>();
        listeners    = new ConcurrentHashMap<>();
        manager      = null;
    }

    public Cluster(ConfigSyncManagerType syncType, String connectionString, int sessionTimeout) throws Exception {
        this();
        initializeCluster(syncType, connectionString, sessionTimeout);

    }


    /**
     * Gets a list of nodes with the cluster class right now.
     * @return
     */
    public Iterable<PravegaNode> getPravegaNodes() {
        return nodes.values();
    }

    /**
     * Returns list of controllers available with the cluster class right now.
     * @return
     */
    public Iterable<PravegaController> getPravegaControllers() {
        return controllers.values();
    }

    /**
     * Returns list of listeners available with the cluster class right now.
     * @return
     */
    public Iterable<ClusterListener> getListeners() {
        log.warn("Returning listeners size = " + listeners.size());
        return listeners.values();
    }

    /**
     * Initializes the cluster with a given config manager type
     * @param syncType               Type of the config manager
     * @param connectionString       String used to connect to the config manager
     * @param sessionTimeout         Session timeout for the connection
     */
    public void  initializeCluster(ConfigSyncManagerType syncType, String connectionString,
                                   int sessionTimeout) throws Exception {
        synchronized (this) {
            if (manager == null)
                manager = new ConfigSyncManagerCreator().createManager(syncType, connectionString,
                        sessionTimeout, this);
            refreshCluster();
        }
    }

    /**
     * Reads the complete cluster from the store and updates the existing data
     **/
    public void refreshCluster() throws Exception {
        nodes.clear();
        controllers.clear();
        manager.refreshCluster();
    }



    /**
     * Private method to add a node and notify all listeners
     * @param node
     * @param endpoint
     */
    private void addNode(PravegaNode node, Endpoint endpoint) {
        nodes.put(endpoint, node);
        listeners.forEach(
                (name, listener) -> {
                    listener.nodeAdded(node);
                });
    }


    /**
     * Private method to add a controller and notify all listeners
     * @param controller
     * @param endpoint
     */
    private void addController(PravegaController controller, Endpoint endpoint) {
        controllers.put(endpoint, controller);
        listeners.forEach(
                (name, listener) -> {
                    listener.controllerAdded(controller);
                });
    }


    /**
     * Private method to remove a controller and notify all the listeners
     * @param endpoint
     */
    private void removeController(Endpoint endpoint) {
        PravegaController controller = controllers.remove(endpoint);
        listeners.forEach(
                (name, listener) -> {
                    listener.controllerRemoved(controller);
                });
    }

    /**
     * Private method to remove a node and notify all the listeners
     * @param endpoint
     */
    private void removeNode(Endpoint endpoint) {
        PravegaNode node = nodes.remove(endpoint);
        listeners.forEach(
                (name, listener) -> {
                    listener.nodeRemoved(node);
                });
    }

    /**
     * Registers the current pravega node with a specific hostname and port with the config store
     * @param host
     * @param port
     * @param jsonMetadata
     */
    public void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception {
        manager.registerPravegaNode(host, port, jsonMetadata);
    }

    /**
     * Registers the current pravega controller with a specific hostname and port with the config store
     * @param host
     * @param port
     * @param jsonMetadata
     */
    public void registerPravegaController(String host, int port,  String jsonMetadata) throws Exception {
        manager.registerPravegaController(host, port, jsonMetadata);
    }

    /**
     * Unregisters the current pravega controller with a specific hostname and port with the config store
     * @param host
     * @param port
     */
    public void deregisterPravegaController(String host, int port) throws Exception {
        manager.deregisterPravegaController(host, port);
    }

    /**
     * Unregisters the current pravega node with a specific hostname and port with the config store
     * @param host
     * @param port
     */
    public void deregisterPravegaNode(String host, int port) {
        manager.deregisterPravegaNode(host, port);
    }

    /**
     * Listener specific functions:
     * Registers a new listener
     * @param clusterListener
     */
    public synchronized void registerListener(String name, ClusterListener clusterListener) {
        listeners.put(name, clusterListener);
    }

    /**
     * Listener specific functions:
     * Removes a registered listener
     * @param clusterListener
     */
    public void deRegisterListener(ClusterListener clusterListener) {
        if (listeners.contains(clusterListener)) listeners.remove(clusterListener);
    }

    /**
     * Notification from the config manager abstraction to notify a node is added
     * @param host
     * @param port
     */
    @Override
    public void nodeAddedNotification(String host, int port) {
        Endpoint ep = new Endpoint(host, port);
        addNode(new PravegaNode(ep), ep);
    }

    /**
     * Notification from config manager abstraction to notify a controller is added
     * @param host
     * @param port
     */
    @Override
    public void controllerAddedNotification(String host, int port) {
        Endpoint ep = new Endpoint(host, port);
        addController(new PravegaController(ep), ep);
    }

    /**
     * Notification from the config manager abstraction to notify a node is removed
     * @param host
     * @param port
     */
    @Override
    public void nodeRemovedNotification(String host, int port) {
        Endpoint ep = new Endpoint(host, port);
        removeNode(ep);
    }

    /**
     * Notification from config manager abstraction to notify a controller is removed
     * @param host
     * @param port
     */
    @Override
    public void controllerRemovedNotification(String host, int port) {
        Endpoint ep = new Endpoint(host, port);
        removeController(ep);
    }


}