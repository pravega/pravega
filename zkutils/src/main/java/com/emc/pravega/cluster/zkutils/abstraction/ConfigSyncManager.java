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


package com.emc.pravega.cluster.zkutils.abstraction;


/**
  * Created by kandha on 8/8/16.
  */

/**
 * Interface to abstract the configuration and synchronization component.
 * Current implementation include vnest, ZK and a dummy implementation
 */
public interface ConfigSyncManager {



  /**
    * Sample configuration/synchronization methods. Will add more as implementation progresses
    **/

  /**
     * Create an entry at a given path. Right now all the entries are ephemeral
     * @param path
     * @param value
     * @throws Exception
     */
  void createEntry(String path, byte[] value) throws Exception;

    /**
     * Deletes an existing entry
     * @param path
     * @throws Exception
     */
  void deleteEntry(String path) throws Exception;

    /**
     * Removes existing cache and refreshes data from the store
     * @throws Exception
     */
  void  refreshCluster() throws Exception;

    /**
     * Registers a pravega node
     * @param host
     * @param port
     * @param jsonMetadata
     * @throws Exception
     */
  void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception;

    /**
     * Registers a pravega controller
     * @param host
     * @param port
     * @param jsonMetadata
     * @throws Exception
     */
  void registerPravegaController(String host, int port, String jsonMetadata) throws Exception;

    /**
     * Unregisters a pravega controller
     * @param host
     * @param port
     * @throws Exception
     */
  void unregisterPravegaController(String host, int port) throws Exception;

    /**
     * Unregisters a pravega node
     * @param host
     * @param port
     * @throws Exception
     */
  void unregisterPravegaNode(String host, int port) throws Exception;
}

