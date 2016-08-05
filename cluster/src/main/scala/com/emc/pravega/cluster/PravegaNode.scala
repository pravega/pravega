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

/**
  * Created by kandha on 8/4/16.
  */
class PravegaNode (val endpoint: Endpoint){

}

object PravegaNode {
  /**
    * Creates a PravegaNode object from a given configuration
    * @param jsonString pravega node configuration.
    *                   This will come in from zookeeper abstraction
    * @return A PravegaNode object
    */
  def createPravegaNode(jsonString:String):PravegaNode = {
    //TODO: Parse the json string to get node specific settings
    val endpoints:Array[String] = jsonString.split(':')
    new PravegaNode(new Endpoint(endpoints(0),endpoints(1).toInt))
  }
}
