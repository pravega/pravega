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

import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.mock.MockClientFactory;

import lombok.Cleanup;

public class StartConsumer {

    /**
     * Reads and prints 20 events from a test stream.
     * @param args cli inputs. not used for now.
     * @throws Exception in case of failure.
     */
    public static void main(String[] args) throws Exception {
        MockClientFactory clientFactory = new MockClientFactory(StartLocalService.SCOPE,
                "localhost",
                StartLocalService.PORT);
        clientFactory.createStream(StartLocalService.STREAM_NAME, null);
        @Cleanup
        EventStreamReader<String> consumer = clientFactory.createReader(StartLocalService.STREAM_NAME,
                                     new JavaSerializer<>(),
                            new ReaderConfig(),
                            clientFactory.getInitialPosition(StartLocalService.STREAM_NAME));        
        for (int i = 0; i < 20; i++) {
            String event = consumer.readNextEvent(60000).getEvent();
            System.err.println("Read event: " + event);
        }
        System.exit(0);
    }
}
