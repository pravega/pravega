/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientManager;

import lombok.Cleanup;

public class StartProducer {

    public static void main(String[] args) throws Exception {
        MockClientManager clientManager = new MockClientManager(StartLocalService.SCOPE,
                                                                "localhost",
                                                                StartLocalService.PORT);
        clientManager.createStream(StartLocalService.STREAM_NAME, null);
        @Cleanup
        Producer<String> producer = clientManager.createProducer(StartLocalService.STREAM_NAME,
                                                                new JavaSerializer<>(),
                                                                new ProducerConfig(null));
        Transaction<String> transaction = producer.startTransaction(60000);
        for (int i = 0; i < 10; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            transaction.publish("", event);
            transaction.flush();
            Thread.sleep(500);
        }
        for (int i = 0; i < 10; i++) {
            String event = "\n Non-transactional Publish \n";
            System.err.println("Producing event: " + event);
            producer.publish("", event);
            producer.flush();
            Thread.sleep(500);
        }
        transaction.commit();
        System.exit(0);
    }
}
