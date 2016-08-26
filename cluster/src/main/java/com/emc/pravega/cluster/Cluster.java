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

import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManagerType;

/**
 * This class represents a pravega cluster.
 * A pravega cluster contains a number of pravega nodes and controller nodes.
 * Each node instance is uniquely identified by an Endpoint class.
 * <p>
 * An Endpoint class represents a server IP and port on which either the
 * pravega node Or the controller listens
 */
public interface Cluster {


    /**
     * Gets a list of nodes with the cluster class right now.
     *
     * @return
     */
    Iterable<PravegaNode> getPravegaNodes();

    /**
     * Returns list of controllers available with the cluster at this point in time.
     * Users of this API can register a listener to be notified about updates to this data.
     *
     * @return
     */
    Iterable<PravegaController> getPravegaControllers();

    /**
     * Returns list of listeners available with the cluster at this point in time.
     * Users of this API can register a listener to be notified about updates to this data.
     *
     * @return
     */
    Iterable<ClusterListener> getListeners();

    /**
     * Initializes the cluster with a given config manager type
     *
     * @param syncType         Type of the config manager
     * @param connectionString String used to connect to the config manager
     * @param sessionTimeout   Session timeout for the connection
     */
    void initializeCluster(ConfigSyncManagerType syncType, String connectionString,
                                               int sessionTimeout) throws Exception;

    /**
     * Reads the complete cluster from the store and updates the existing data.
     * This can cause a flurry of notifications for the listener.
     **/
    void refreshCluster() throws Exception;


    /**
     * Registers the current pravega node with a specific hostname and port with the config store
     *
     * @param host
     * @param port
     * @param jsonMetadata
     */
    void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception;

    /**
     * Registers the current pravega controller with a specific hostname and port with the config store
     *
     * @param host
     * @param port
     * @param jsonMetadata
     */
    void registerPravegaController(String host, int port, String jsonMetadata) throws Exception;

    /**
     * Unregisters the current pravega controller with a specific hostname and port with the config store
     *
     * @param host
     * @param port
     */
    void unregisterPravegaController(String host, int port) throws Exception;

    /**
     * Unregisters the current pravega node with a specific hostname and port with the config store
     *
     * @param host
     * @param port
     */
    void unregisterPravegaNode(String host, int port) throws Exception;

    /**
     * Listener specific functions:
     * Registers a new listener
     *
     * @param clusterListener
     */
    void registerListener(String name, ClusterListener clusterListener);

    /**
     * Listener specific functions:
     * Removes a registered listener
     *
     * @param clusterListener
     */
    void unregisterListener(ClusterListener clusterListener);

}