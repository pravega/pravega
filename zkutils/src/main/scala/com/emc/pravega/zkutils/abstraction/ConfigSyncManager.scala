package com.emc.pravega.zkutils.abstraction

import com.emc.pravega.zkutils.dummy.DummyZK
import com.emc.pravega.zkutils.vnest.VnestClient
import com.emc.pravega.zkutils.zkimplementation.ZookeeperClient
import org.apache.zookeeper.{WatchedEvent, Watcher}

/**
  * Created by kandha on 8/8/16.
  */
trait ConfigSyncManager {

  /**
    * Sample ZK methods. Will add more as implementation progresses
    * */
  def createEntry(path:String, value: Array[Byte]):String;
  def deleteEntry(path:String);

}

object ConfigSyncManager extends Watcher {
  def createManager(zkType: String, connectionString: String, sessionTimeout: Int): ConfigSyncManager = {
    zkType match {
      case "dummy" => new DummyZK(connectionString, sessionTimeout, this)
      case "vnest" => new VnestClient(connectionString, sessionTimeout, this)
      case _       => new ZookeeperClient(connectionString, sessionTimeout, this)
    }
  }

  override def process(event: WatchedEvent): Unit = {

  }
}
