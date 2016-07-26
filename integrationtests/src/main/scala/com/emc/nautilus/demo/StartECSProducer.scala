package com.emc.nautilus.demo;

import com.emc.nautilus.streaming.Producer;
import com.emc.nautilus.streaming.ProducerConfig;
import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.impl.DirectToECSStreamManagerImpl;
import com.emc.nautilus.streaming.impl.JavaSerializer;
import lombok.Cleanup;

/**
 * Created by kandha on 7/21/16.
 */
class StartECSProducer {

}

object StartECSProducer {
  def main(args : Array [String] ): Unit = {
    val endpoint: String= {
      if (args.length == 0) "localhost"
      else args(0)
    }
    val accessKey:String= {
      if (args.length < 1) "localhost"
      else args(1)
    }
    val secretKey:String= {
      if (args.length < 2) "localhost"
      else args(2)
    }
    val scope: String = "Scope1"
    val streamName: String = "Stream1"
    val testString: String = "Hello world: "
    val streamManager: DirectToECSStreamManagerImpl  = new DirectToECSStreamManagerImpl(endpoint, accessKey, secretKey,
       scope)
    val stream: Stream  = streamManager.createStream(streamName, null)
    val producer: Producer [String] = stream.createProducer(new JavaSerializer[](), new ProducerConfig(null))
    val i:Int = 0;
    for (i <- 0 until 10000) {
      producer.publish(null, testString + i + "\n");
    }
    producer.flush();
  }
}