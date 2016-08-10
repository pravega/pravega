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

package com.emc.pravega.cluster

import java.util

import com.emc.pravega.zkutils.abstraction.ConfigSyncManager

import scala.collection.mutable

/**
  * This class represents a pravega cluster.
  * A pravega cluster contains a number of pravega nodes and controller nodes.
  * Each node instance is uniquely identified by an Endpoint class.
  *
  * An Endpoint class represents a server IP and port on which either the
  * pravega node Or the controller listens
  */
class cluster {
  val nodes:        mutable.HashMap[Endpoint, PravegaNode] = new mutable.HashMap[Endpoint, PravegaNode]

  val controllers:  mutable.HashMap[Endpoint, PravegaController] = new mutable.HashMap[Endpoint, PravegaController]

  val listeners:   mutable.Set[ClusterListener] = mutable.Set[ClusterListener]()

  var manager:ConfigSyncManager = null

  def getPravegaNodes: Iterable[PravegaNode] = nodes.values

  def getPravegaControllers: Iterable[PravegaController] = controllers.values

  def  initializeCluster(syncType:String,connectionString: String,sessionTimeout:Int):Unit = {
    this.synchronized {
      if(manager == null)
      manager = ConfigSyncManager.createManager(syncType, connectionString, sessionTimeout)
      refreshCluster()
      manager
    }
  }

  /**
    * Reads the complete cluster from the store and updates the existing data
    **/
  def refreshCluster():Unit = {
    manager.refreshCluster()
  }

  def addNode(node:PravegaNode, endpoint:Endpoint): Unit = {
    nodes.put(endpoint, node)
    listeners.foreach {_.nodeAdded(node)}
  }

  def addController(controller:PravegaController, endpoint:Endpoint): Unit = {
    controllers.put(endpoint, controller)
    listeners.foreach(_.controllerAdded(controller))
  }

  def registerListener(clusterListener: ClusterListener):Unit = {
    if(listeners.contains(clusterListener)==false) listeners.add(clusterListener)
  }

  def deRegisterListener(clusterListener: ClusterListener):Unit = {
    if(listeners.contains(clusterListener)) listeners.remove(clusterListener)
  }
}