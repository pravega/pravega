package com.emc.pravega.cluster

import org.junit.{Assert, Test}
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, FreeSpec, WordSpec}
import org.scalatest.junit.JUnitRunner

/**
  * Created by kandha on 8/16/16.
  */


@RunWith(classOf[JUnitRunner])
class clustertest extends WordSpec {
 "A cluster" when {
   var clust: cluster = new cluster
   "created with dummy" should {
     clust.initializeCluster("dummy", "dummy", 0)
	   "return 0 nodes" in assert(clust.nodes.size == 0)
     "return 0 controllers" in assert(clust.controllers.size == 0)

     "After adding a controller" should {
       clust.manager.createEntry(clust.manager.controllerInfoRoot+"/"+
       "192.168.1.1:2000"+"/","jsondata".getBytes())

       "return 1 controller" in assert(clust.controllers.size == 1)
     }
        
   } 
 }

}
