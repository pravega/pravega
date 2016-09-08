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

/**
 * This class represents a Pravega cluster.
 * A Pravega cluster contains a number of Pravega nodes and controller nodes.
 * Each node instance is uniquely identified by an Endpoint class.
 * <p>
 * An Endpoint class represents a server IP and port on which either the
 * Pravega node Or the controller listens
 */
public interface Cluster {


    /**
     * Gets a list of nodes with the cluster class right now.
     *
     * @return List of nodes cached with cached with the local cluster object
     */
    Iterable<PravegaNode> getPravegaNodes();

    /**
     * Returns list of controllers available with the cluster at this point in time.
     * Users of this API can register a listener to be notified about updates to this data.
     *
     * @return List of Pravega controllers cached with the local cluster object
     */
    Iterable<PravegaController> getPravegaControllers();

    /**
     * Returns list of listeners available with the cluster at this point in time.
     * Users of this API can register a listener to be notified about updates to this data.
     *
     * @return List of listeners
     */
    Iterable<ClusterListener> getListeners();

    /**
     * Reads the complete cluster from the store and updates the existing data.
     * This can cause a flurry of notifications for the listener.
     **/
    void refreshCluster() throws Exception;


    /**
     * Registers the current Pravega node with a specific hostname and port with the config store
     *
     * @param host Unique name/IP of the host to be registered
     * @param port The port used to communicate to Pravega node
     * @param jsonMetadata Reserved for future use. A Node/controller can use this field to store important metadata
     */
    void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception;

    /**
     * Registers the current Pravega controller with a specific hostname and port with the config store
     *
     * @param host Unique hostname/IP of the controller
     * @param port Port used to communicate to the controller
     * @param jsonMetadata Reserved for future use. A Node/controller can use this field to store important metadata
     */
    void registerPravegaController(String host, int port, String jsonMetadata) throws Exception;

    /**
     * Unregisters the current Pravega controller with a specific hostname and port with the config store
     *
     * @param host Unique hostname/IP of the controller
     * @param port Port used to communicate to the controller
     */
    void unregisterPravegaController(String host, int port) throws Exception;

    /**
     * Unregisters the current Pravega node with a specific hostname and port with the config store
     *
     * @param host Unique name/IP of the host to be registered
     * @param port The port used to communicate to Pravega node
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