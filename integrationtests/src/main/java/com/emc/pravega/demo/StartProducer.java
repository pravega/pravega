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
package com.emc.pravega.demo;

import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.ApiController;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.SingleSegmentStreamManagerImpl;

import java.util.concurrent.ExecutionException;

public class StartProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String endpoint = "localhost";
        int port = 9090;
        String scope = "Scope1";
        String streamName = "Stream1";
        String testString = "Hello world: ";

        ApiController apiController = new ApiController(endpoint, port);
        SingleSegmentStreamManagerImpl streamManager = new SingleSegmentStreamManagerImpl(apiController, apiController, apiController, scope);
        Stream stream = streamManager.createStream(streamName, null);
        // TODO: remove sleep. It ensures pravega host handles createsegment call from controller before we publish.
        Thread.sleep(1000);

//        @Cleanup
        Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
        for (int i = 0; i < 10000; i++) {
            producer.publish(null, testString + i + "\n");
        }
        producer.flush();
    }
}
