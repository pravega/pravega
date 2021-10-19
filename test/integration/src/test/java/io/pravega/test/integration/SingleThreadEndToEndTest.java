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
package io.pravega.test.integration;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.integration.utils.SetupUtils;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * This runs a basic end to end test with a single thread in the thread pool to make sure we don't
 * block anything on it.
 */
public class SingleThreadEndToEndTest {

    @Test(timeout = 60000)
    public void testReadWrite() throws Exception {
        @Cleanup("stopAllServices")
        SetupUtils setupUtils = new SetupUtils();
        setupUtils.startAllServices(1);
        setupUtils.createTestStream("stream", 1);
        @Cleanup
        EventStreamWriter<Integer> writer = setupUtils.getIntegerWriter("stream");
        writer.writeEvent(1);
        writer.flush();
        @Cleanup
        val rgm = setupUtils.createReaderGroupManager("stream");
        @Cleanup
        EventStreamReader<Integer> reader = setupUtils.getIntegerReader("stream", rgm);
        EventRead<Integer> event = reader.readNextEvent(10000);
        Assert.assertEquals(1, (int) event.getEvent());
    }
    
    @Test(timeout = 60000)
    public void testSealedStream() throws Exception {
        @Cleanup("stopAllServices")
        SetupUtils setupUtils = new SetupUtils();
        setupUtils.startAllServices(1);
        @Cleanup
        StreamManager streamManager = StreamManager.create(setupUtils.getClientConfig());
        streamManager.createScope("scope");
        streamManager.createStream("scope",
                                   "stream",
                                   StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());

        @Cleanup
        EventStreamWriter<byte[]> writer = setupUtils.getClientFactory()
            .createEventWriter("stream",
                               new ByteArraySerializer(),
                               EventWriterConfig.builder().retryAttempts(2).enableLargeEvents(true).build());
        writer.writeEvent(new byte[Serializer.MAX_EVENT_SIZE + 1]).join();
        writer.flush();
        assertTrue(streamManager.sealStream("scope", "stream"));
        AssertExtensions.assertThrows(SegmentSealedException.class,
                                      () -> writer.writeEvent(new byte[Serializer.MAX_EVENT_SIZE + 1]).join());
        AssertExtensions.assertThrows(IllegalStateException.class, () -> writer.writeEvent(new byte[1]).join());
        writer.flush();
    }

}
