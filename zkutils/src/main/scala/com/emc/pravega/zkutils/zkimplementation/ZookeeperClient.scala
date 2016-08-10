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
package com.emc.pravega.zkutils.zkimplementation

import com.emc.pravega.zkutils.abstraction.ConfigSyncManager
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.collection.{Iterable, Map}

class ZookeeperClient(connectString: String, sessionTimeout: Int, watcher :Watcher)
  extends ZooKeeper(connectString, sessionTimeout, watcher) with Watcher with ConfigSyncManager {

  val pravegaNodesPath       = "/pravega/nodes"
  val pravegaControllersPath = "/pravega/controllers"
  /**
    * Callback from the watcher
    */
  override def process(event: WatchedEvent): Unit = {
    //Process the incoming events
  }

  /**
    * Sample ZK methods. Will add more as implementation progresses
    **/
  override def createEntry(path: String, value: Array[Byte]): String = ???

  override def deleteEntry(path: String): Unit = ???

  override def refreshCluster(): Unit = ???

  def jsonEncode(obj: Any): String = {
      obj match {
        case null => "null"
        case b: Boolean => b.toString
        case s: String => "\"" + s + "\""
        case n: Number => n.toString
        case m: Map[_, _] =>
          "{" +
            m.map(elem =>
              elem match {
                case t: Tuple2[_,_] => jsonEncode(t._1) + ":" + jsonEncode(t._2)
                case _ => throw new IllegalArgumentException("Invalid element (" + elem + ") in " + obj)
              }).mkString(",") + "}"
        case a: Array[_] => jsonEncode(a.toSeq)
        case i: Iterable[_] => "[" + i.map(jsonEncode).mkString(",") + "]"
        case other: AnyRef => throw new IllegalArgumentException("Unknown type " + other.getClass + ": " + other)
      }
  }

  override def registerPravegaNode(host: String, port: Int, jsonMetadata: String): Unit = {
    val jsonMap = Map(
      "host" -> host,
      "port" -> port,
      "metadata" -> jsonMetadata
    )

    val nodeJson = jsonEncode(jsonMap)

  }

  override def registerPravegaController(host: String, port: Int, jsonMetadata: String): Unit = ???
}