package com.emc.pravega.cluster

/**
  * Created by kandha on 8/4/16.
  */
class PravegaController (val endpoint: Endpoint) {

}

object PravegaController {
  /**
    * Creates a controller object from a given configuration
    * @param jsonString Config string. Can be read from zookeeper abstraction
    * @return a new PravegaController object
    */
  def createController(jsonString: String):PravegaController = {
    //TODO: parse the string to get other controller specific config
    val endpoint:Array[String]  = jsonString.split(':')
    new PravegaController(new Endpoint(endpoint(0),endpoint(1).toInt))

  }
}
