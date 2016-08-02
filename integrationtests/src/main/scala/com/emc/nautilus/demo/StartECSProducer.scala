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

package com.emc.nautilus.demo;

import com.emc.nautilus.stream.Producer;
import com.emc.nautilus.stream.ProducerConfig;
import com.emc.nautilus.stream.Stream;
import com.emc.nautilus.streaming.impl.DirectToECSStreamManagerImpl;
import com.emc.nautilus.stream.impl.JavaSerializer;
import lombok.Cleanup;

/**
 * Created by kandha on 7/21/16.
 */
class StartECSProducer {

}

object StartECSProducer {

  /**
    * Entry point for the dummy producer run
    * @param args
    */
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
    val producer: Producer [String] = stream.createProducer(new JavaSerializer[String](), new ProducerConfig(null))
    val i:Int = 0;
    for (i <- 0 until 10000) {
      producer.publish(null, testString + i + "\n");
    }
    producer.flush();
  }
}

