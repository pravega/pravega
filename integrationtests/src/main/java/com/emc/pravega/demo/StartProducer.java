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

import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;

import lombok.Cleanup;

public class StartProducer {

    public static void main(String[] args) throws Exception {
        MockClientFactory clientFactory = new MockClientFactory(StartLocalService.SCOPE,
                                                                "localhost",
                                                                StartLocalService.PORT);
        clientFactory.createStream(StartLocalService.STREAM_NAME, null);
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(StartLocalService.STREAM_NAME,
                                                                new JavaSerializer<>(),
                                                                new EventWriterConfig(null));
        Transaction<String> transaction = producer.beginTxn(60000);
        for (int i = 0; i < 10; i++) {
            String event = "\n Transactional Publish \n";
            System.err.println("Producing event: " + event);
            transaction.writeEvent("", event);
            transaction.flush();
            Thread.sleep(500);
        }
        for (int i = 0; i < 10; i++) {
            String event = "\n Non-transactional Publish \n";
            System.err.println("Producing event: " + event);
            producer.writeEvent("", event);
            producer.flush();
            Thread.sleep(500);
        }
        transaction.commit();
        System.exit(0);
    }
}
