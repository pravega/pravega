/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.stream.EventStreamReader;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.mock.MockStreamManager;

import java.util.Collections;
import java.util.UUID;

import lombok.Cleanup;

public class StartReader {

    private static final String READER_GROUP = "ExampleReaderGroup";

    public static void main(String[] args) throws Exception {
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(StartLocalService.SCOPE,
                                                                "localhost",
                                                                StartLocalService.PORT);
        streamManager.createScope(StartLocalService.SCOPE);
        streamManager.createStream(StartLocalService.SCOPE, StartLocalService.STREAM_NAME, null);
        streamManager.createReaderGroup(READER_GROUP,
                                        ReaderGroupConfig.builder().startingTime(0).build(),
                                        Collections.singleton(StartLocalService.STREAM_NAME));
        EventStreamReader<String> reader = streamManager.getClientFactory().createReader(UUID.randomUUID().toString(),
                                                                                         READER_GROUP,
                                                                                         new JavaSerializer<>(),
                                                                                         ReaderConfig.builder().build());
        for (int i = 0; i < 20; i++) {
            String event = reader.readNextEvent(60000).getEvent();
            System.err.println("Read event: " + event);
        }
        reader.close();
        System.exit(0);
    }
}
