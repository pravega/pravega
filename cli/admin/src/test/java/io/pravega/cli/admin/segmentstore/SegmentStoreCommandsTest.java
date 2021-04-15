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
package io.pravega.cli.admin.segmentstore;

import io.pravega.cli.admin.AbstractAdminCommandTest;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SegmentStoreCommandsTest extends AbstractAdminCommandTest {

    @Test
    public void testGetSegmentInfoCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getinfo", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore get-segment-info segmentstore/getinfo/0.#epoch.0 localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Failed"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_abortStream/0.#epoch.0 localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Failed"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info not/exists/0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("Failed"));
        Assert.assertNotNull(GetSegmentInfoCommand.descriptor());
    }

    @Test
    public void testReadSegmentRangeCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "readsegment", StreamConfiguration.builder().build());
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(SETUP_UTILS.getControllerUri()).build();
        @Cleanup
        EventStreamClientFactory factory = EventStreamClientFactory.withScope("segmentstore", clientConfig);
        @Cleanup
        EventStreamWriter<String> writer = factory.createEventWriter("readsegment", new JavaSerializer<>(), EventWriterConfig.builder().build());
        writer.writeEvents("rk", Arrays.asList("a", "2", "3"));
        writer.flush();
        String commandResult = TestUtils.executeCommand("segmentstore read-segment segmentstore/readsegment/0.#epoch.0 0 8 localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Error"));
        commandResult = TestUtils.executeCommand("segmentstore read-segment not/exists/0 0 1 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("Error"));
        Assert.assertNotNull(GetSegmentInfoCommand.descriptor());
    }

    @Test
    public void testGetSegmentAttributeCommand() throws Exception {
        // TODO: Maybe we need listing attributes first
    }
}
