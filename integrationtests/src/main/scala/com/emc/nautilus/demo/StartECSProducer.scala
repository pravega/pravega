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
    val endpoint: String= "localhost"
    val port: Int = 12345
    val scope: String = "Scope1"
    val streamName: String = "Stream1"
    val testString: String = "Hello world: "
    val streamManager: DirectToECSStreamManagerImpl  = new DirectToECSStreamManagerImpl(endpoint, port, scope)
    val stream: Stream  = streamManager.createStream(streamName, null)
    val producer: Producer [String] = stream.createProducer(new JavaSerializer[](), new ProducerConfig(null));
    val i:Int = 0;
    for (i <- 0 until 10000) {
      producer.publish(null, testString + i + "\n");
    }
    producer.flush();
  }
}