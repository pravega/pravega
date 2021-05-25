/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.demo;

import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;

import lombok.Cleanup;

import java.net.InetAddress;

public class StartWriter {

    public static void main(String[] args) throws Exception {
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(StartLocalService.SCOPE,
                                                                InetAddress.getLocalHost().getHostAddress(),
                                                                StartLocalService.SERVICE_PORT);
        streamManager.createScope(StartLocalService.SCOPE);
        streamManager.createStream(StartLocalService.SCOPE, StartLocalService.STREAM_NAME, null);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(StartLocalService.STREAM_NAME, new JavaSerializer<>(),
                                                                           EventWriterConfig.builder()
                                                                                            .transactionTimeoutTime(60000)
                                                                                            .build());
        @Cleanup
        TransactionalEventStreamWriter<String> txnWriter = clientFactory.createTransactionalEventWriter("writer", StartLocalService.STREAM_NAME,
                                                                                                        new JavaSerializer<>(),
                                                                                                        EventWriterConfig.builder()
                                                                                                                         .transactionTimeoutTime(60000)
                                                                                                                         .build());
        Transaction<String> transaction = txnWriter.beginTxn();

        for (int i = 0; i < 10; i++) {
            String event = "\n Transactional write \n";
            System.err.println("Writing event: " + event);
            transaction.writeEvent(event);
            transaction.flush();
            Thread.sleep(500);
        }
        for (int i = 0; i < 10; i++) {
            String event = "\n Non-transactional Publish \n";
            System.err.println("Writing event: " + event);
            writer.writeEvent(event);
            writer.flush();
            Thread.sleep(500);
        }
        transaction.commit();
        System.exit(0);
    }
}
