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
import io.pravega.segmentstore.contracts.Attributes;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

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
        Assert.assertNotNull(ReadSegmentRangeCommand.descriptor());
    }

    @Test
    public void testGetSegmentAttributeCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getattribute", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/getattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Error"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Error"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute not/exists/0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("Error"));
        Assert.assertNotNull(GetSegmentAttributeCommand.descriptor());
    }

    @Test
    public void testUpdateSegmentAttributeCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "updateattribute", StreamConfiguration.builder().build());
        // First, get the existing value of that attribute for the segment.
        String commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Error"));
        long oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertNotEquals(0L, oldValue);
        // Update the Segment to a value of 0.
        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " 0 " + oldValue +" localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Error"));
        // Check that the value has been updated.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " localhost", STATE.get());
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertEquals(0L, oldValue);

        // Do the same for an internal segment.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Error"));
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertNotEquals(0L, oldValue);
        // Update the Segment to a value of 0.
        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " 0 " + oldValue +" localhost", STATE.get());
        Assert.assertFalse(commandResult.contains("Error"));
        // Check that the value has been updated.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " localhost", STATE.get());
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertEquals(0L, oldValue);

        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute not/exists/0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0).toString() + " 0 0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("Error"));
        Assert.assertNotNull(UpdateSegmentAttributeCommand.descriptor());
    }
}
