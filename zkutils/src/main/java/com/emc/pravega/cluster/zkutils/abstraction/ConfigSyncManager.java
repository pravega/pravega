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
 * Interface to abstract the configuration and synchronization component.
 * Current implementation include vnest, ZK and a dummy implementation
 */
public interface ConfigSyncManager {



  /*
     Sample configuration/synchronization methods. Will add more as implementation progresses
    */

  /**
     * Create an entry at a given path. Right now all the entries are ephemeral
     * @param path The path in the ZK/Vnest which is created
     * @param value The value stored at this path
     * @throws Exception Passes on the exception thrown by curator/Vnest
     */
  void createEntry(String path, byte[] value) throws Exception;

    /**
     * Deletes an existing entry
     * @param path The path in ZK/Vnest which is removed
     * @throws Exception Passes on the exception thrown by curator/Vnest
     */
  void deleteEntry(String path) throws Exception;

    /**
     * Removes existing cache and refreshes data from the store
     * @throws Exception Passes on the exception thrown by curator/Vnest
     */
  void  refreshCluster() throws Exception;

    /**
     * Registers a Pravega node
     * @param host Unique name/IP of the host to be registered
     * @param port The port used to communicate to Pravega node
     * @param jsonMetadata Reserved for future use. A Node/controller can use this field to store important metadata
     * @throws Exception Passes on the exception thrown by curator/Vnest
     */
  void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception;

    /**
     * Registers a Pravega controller
     * @param host Unique hostname/IP of the controller
     * @param port Port used to communicate to the controller
     * @param jsonMetadata Reserved for future use. A Node/controller can use this field to store important metadata
     * @throws Exception Passes on the exception thrown by curator/Vnest
     */
  void registerPravegaController(String host, int port, String jsonMetadata) throws Exception;

    /**
     * Unregisters a Pravega controller
     * @param host Unique hostname/IP of the controller
     * @param port Port used to communicate to the controller
     * @throws Exception Passes on the exception thrown by curator/Vnest
     */
  void unregisterPravegaController(String host, int port) throws Exception;

    /**
     * Unregisters a Pravega node
     * @param host Unique name/IP of the host to be registered
     * @param port The port used to communicate to Pravega node
     * @throws Exception Passes on the exception thrown by curator/Vnest
     */
  void unregisterPravegaNode(String host, int port) throws Exception;
}

