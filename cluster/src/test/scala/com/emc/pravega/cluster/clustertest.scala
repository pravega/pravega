package com.emc.pravega.cluster

import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManagerType
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class clustertest extends WordSpec {
  "A cluster" when {
    "created with dummy manager" should {
      " after initializtion" should {
        val clust: Cluster = new ClusterImpl()
        clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
        "return 0 nodes" in assert(clust.getPravegaNodes.iterator().hasNext == false)
        "return 0 controllers" in assert(clust.getPravegaControllers.iterator().hasNext == false)
        "return 0 listeners" in assert(clust.getListeners().iterator().hasNext == false)
      }
      "after registration of a listener" should {
        "return 1 listeners" in {
          val clust: Cluster = new ClusterImpl()
          clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
          val clusterListner:DummyListener = new DummyListener
          clust.registerListener("my",clusterListner)
          assert(clust.getListeners().iterator().hasNext == true)
        }
        "after inserting a controller" should {
          "return 1 controller" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
            clust.registerPravegaController("localhost", 7000, "{\"metadata\":asd}")
            assert(clust.getPravegaControllers.iterator().hasNext == true)

          }
          "call back the registered listener" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
            val clusterListner:DummyListener = new DummyListener
            clust.registerListener("my",clusterListner)
            clust.registerPravegaController("localhost", 7000, "{\"metadata\":asd}")
            assert(clusterListner.numControllers ==1)
          }
        }
        "after inserting a node" should {
          "return 1 node" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
            clust.registerPravegaNode("localhost", 7000, "{\"metadata\":asd}")
            assert(clust.getPravegaNodes.iterator().hasNext == true)

          }
          "call back the registered listener" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
            val clusterListner:DummyListener = new DummyListener
            clust.registerListener("my",clusterListner)
            clust.registerPravegaNode("localhost", 7000, "{\"metadata\":asd}")
            assert(clusterListner.numNodes ==1)
          }
        }
        "after removing inserted controller" should {
          "return zero controllers" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
            clust.registerPravegaController("localhost", 7000, "{\"metadata\":asd}")
            clust.unregisterPravegaController("localhost", 7000)
            assert(clust.getPravegaControllers.iterator().hasNext == false)
          }

          "callback the listener" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
            val clusterListner:DummyListener = new DummyListener
            clust.registerListener("my",clusterListner)
            clust.registerPravegaController("localhost", 7000, "{\"metadata\":asd}")
            clust.unregisterPravegaController("localhost", 7000)
            assert(clusterListner.numControllers ==0)

          }
        }
        "after removing inserted node" should {
          "return zero nodes" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
            clust.registerPravegaNode("localhost", 7000, "{\"metadata\":asd}")
            clust.unregisterPravegaNode("localhost", 7000)
            assert(clust.getPravegaNodes.iterator().hasNext == false)
          }
          "callback the listener" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.DUMMY, "dummy", 0)
            val clusterListner:DummyListener = new DummyListener
            clust.registerListener("my",clusterListner)
            clust.registerPravegaNode("localhost", 7000, "{\"metadata\":asd}")
            clust.unregisterPravegaNode("localhost", 7000)
            assert(clusterListner.numNodes == 0)
          }

        }

      }
    }
    "created with ZK manager" ignore {
      " after initializtion" ignore {
        val clust: Cluster = new ClusterImpl()
        clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
        "return 0 nodes" in assert(clust.getPravegaNodes.iterator().hasNext == false)
        "return 0 controllers" in assert(clust.getPravegaControllers.iterator().hasNext == false)
        "return 0 listeners" in assert(clust.getListeners().iterator().hasNext == false)
      }
      "after registration of a listener" should {
        "return 1 listeners" ignore {
          val clust: Cluster = new ClusterImpl()
          clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
          val clusterListner:DummyListener = new DummyListener
          clust.registerListener("my",clusterListner)
          assert(clust.getListeners().iterator().hasNext == true)
        }
        "after inserting a controller" should {
          "return 1 controller" ignore {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
            clust.registerPravegaController("localhost", 7000, "{\"metadata\":asd}")
            Thread.sleep(5000)
            assert(clust.getPravegaControllers.iterator().hasNext == true)
            clust.unregisterPravegaController("localhost",7000)

          }
          "call back the registered listener" ignore {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
            val clusterListner:DummyListener = new DummyListener
            clust.registerListener("my",clusterListner)
            clust.registerPravegaController("localhost", 7000, "{\"metadata\":asd}")
            Thread.sleep(5000)
            assert(clusterListner.numControllers ==1)
            clust.unregisterPravegaController("localhost",7000)
          }
        }
        "after inserting a node" should {
          "return 1 node" ignore {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
            clust.registerPravegaNode("localhost", 7000, "{\"metadata\":asd}")
            Thread.sleep(5000)
            assert(clust.getPravegaNodes.iterator().hasNext == true)
          }
          "call back the registered listener" ignore {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
            val clusterListner:DummyListener = new DummyListener
            clust.registerListener("my",clusterListner)
            clust.registerPravegaNode("localhost", 7000, "{\"metadata\":asd}")
            Thread.sleep(5000)
            assert(clusterListner.numNodes ==1)
          }
        }
        "after removing inserted controller" should {
          "return zero controllers" ignore {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
            clust.registerPravegaController("localhost", 7000, "{\"metadata\":asd}")
            clust.unregisterPravegaController("localhost", 7000)
            Thread.sleep(5000)
            assert(clust.getPravegaControllers.iterator().hasNext == false)
          }

          "callback the listener" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
            val clusterListner:DummyListener = new DummyListener
            clust.registerListener("my",clusterListner)
            clust.registerPravegaController("localhost", 7000, "{\"metadata\":asd}")
            clust.unregisterPravegaController("localhost", 7000)
            Thread.sleep(5000)
            assert(clusterListner.numControllers ==0)

          }
        }
        "after removing inserted node" ignore {
          "return zero nodes" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
            clust.registerPravegaNode("localhost", 7000, "{\"metadata\":asd}")
            clust.unregisterPravegaNode("localhost", 7000)
            assert(clust.getPravegaNodes.iterator().hasNext == false)
          }
          "callback the listener" in {
            val clust: Cluster = new ClusterImpl()
            clust.initializeCluster(ConfigSyncManagerType.ZK, "zk1:2181", 15000)
            val clusterListner:DummyListener = new DummyListener
            clust.registerListener("my",clusterListner)
            clust.registerPravegaNode("localhost", 7000, "{\"metadata\":asd}")
            clust.unregisterPravegaNode("localhost", 7000)
            assert(clusterListner.numNodes == 0)
          }

        }

      }

    }
  }
}


class DummyListener extends ClusterListener {
var numControllers:Int = 0;
var numNodes:Int = 0;

override def controllerAdded(controller: PravegaController): Unit = numControllers=numControllers+1;

override def nodeRemoved(node: PravegaNode): Unit = numNodes=numNodes-1;

override def controllerRemoved(controller: PravegaController): Unit = numControllers=numControllers-1;

override def nodeAdded(node: PravegaNode): Unit = numNodes=numNodes+1;
}