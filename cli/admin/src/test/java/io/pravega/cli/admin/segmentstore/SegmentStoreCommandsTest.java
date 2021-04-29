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
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.test.common.AssertExtensions;
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
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_abortStream/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_requeststream/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGcommitStreamReaders/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGscaleGroup/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGkvtStreamReaders/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/_RGabortStreamReaders/0.#epoch.0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-info _system/containers/metadata_0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("StreamSegmentInfo:"));
        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore get-segment-info not/exists/0 localhost", STATE.get()));
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
        Assert.assertTrue(commandResult.contains("ReadSegment:"));
        commandResult = TestUtils.executeCommand("segmentstore read-segment _system/_RGcommitStreamReaders/0.#epoch.0 0 8 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("ReadSegment:"));
        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore read-segment not/exists/0 0 1 localhost", STATE.get()));
        Assert.assertNotNull(ReadSegmentRangeCommand.descriptor());
    }

    @Test
    public void testGetSegmentAttributeCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "getattribute", StreamConfiguration.builder().build());
        String commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/getattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore get-segment-attribute not/exists/0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get()));
        Assert.assertNotNull(GetSegmentAttributeCommand.descriptor());
    }

    @Test
    public void testUpdateSegmentAttributeCommand() throws Exception {
        TestUtils.createScopeStream(SETUP_UTILS.getController(), "segmentstore", "updateattribute", StreamConfiguration.builder().build());
        // First, get the existing value of that attribute for the segment.
        String commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
        long oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertNotEquals(0L, oldValue);
        // Update the Segment to a value of 0.
        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 " + oldValue + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("UpdateSegmentAttribute:"));
        // Check that the value has been updated.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute segmentstore/updateattribute/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertEquals(0L, oldValue);

        // Do the same for an internal segment.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("GetSegmentAttribute:"));
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertNotEquals(0L, oldValue);
        // Update the Segment to a value of 0.
        commandResult = TestUtils.executeCommand("segmentstore update-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 " + oldValue + " localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("UpdateSegmentAttribute:"));
        // Check that the value has been updated.
        commandResult = TestUtils.executeCommand("segmentstore get-segment-attribute _system/_abortStream/0.#epoch.0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " localhost", STATE.get());
        oldValue = Long.parseLong(commandResult.substring(commandResult.lastIndexOf("=") + 1, commandResult.indexOf(")")));
        Assert.assertEquals(0L, oldValue);

        AssertExtensions.assertThrows(WireCommandFailedException.class, () -> TestUtils.executeCommand("segmentstore update-segment-attribute not/exists/0 "
                + new UUID(Attributes.CORE_ATTRIBUTE_ID_PREFIX, 0) + " 0 0 localhost", STATE.get()));
        Assert.assertNotNull(UpdateSegmentAttributeCommand.descriptor());
    }

}
