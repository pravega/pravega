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


import com.emc.pravega.stream.*;
import com.emc.pravega.stream.impl.*;
import lombok.Cleanup;

import java.util.concurrent.ExecutionException;

public class StartProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        String endpoint = "localhost";
//        int port = 12345;
        String scope = "Scope1";
        String streamName = "Stream1";
        String testString = "Hello world: ";
        ApiAdmin admin = new ApiAdmin("localhost", 9090);
        admin.createStream(new StreamConfigurationImpl(streamName,
                new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0, 0, 1))).get();

        ApiProducer apiProducer = new ApiProducer("localhost", 9090);
        StreamSegments segments = apiProducer.getCurrentSegments(streamName).get();
        SegmentId singleSegment = segments.getSegments().get(0);
        @Cleanup
        SingleSegmentStreamManagerImpl streamManager = new SingleSegmentStreamManagerImpl(singleSegment.getEndpoint(), singleSegment.getPort(), scope);
        Stream stream = streamManager.createStream(streamName, null);
        @Cleanup
        Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
        for (int i = 0; i < 10000; i++) {
            producer.publish(null, testString + i + "\n");
        }
        producer.flush();
    }

}
