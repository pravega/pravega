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
package io.pravega.shared.controller.event;

import java.util.UUID;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import java.util.function.Supplier;
import com.google.common.collect.ImmutableSet;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import io.pravega.shared.controller.event.kvtable.DeleteTableEvent;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ControllerEventSerializer} class.
 */
public class ControllerEventSerializerTests {
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private static final String KVTABLE = "kvtable";
    private static final String READER_GROUP = "readergroup";

    @Test
    public void testAbortEvent() {
        testClass(() -> new AbortEvent(SCOPE, STREAM, 123, UUID.randomUUID()));
    }

    @Test
    public void testAutoScaleEvent() {
        testClass(() -> new AutoScaleEvent(SCOPE, STREAM, 12345L, AutoScaleEvent.DOWN, 434L, 2, true, 684L));
    }

    @Test
    public void testCommitEvent() {
        testClass(() -> new CommitEvent(SCOPE, STREAM, 123));
    }

    @Test
    public void testDeleteStreamEvent() {
        testClass(() -> new DeleteStreamEvent(SCOPE, STREAM, 123L, 345L));
    }

    @Test
    public void testScaleOpEvent() {
        testClass(() -> new ScaleOpEvent(SCOPE, STREAM, Arrays.asList(1L, 2L, 3L),
                new ArrayList<>(Collections.singletonMap(10.0, 20.0).entrySet()),
                true, 45L, 654L));
    }

    @Test
    public void testSealStreamEvent() {
        testClass(() -> new SealStreamEvent(SCOPE, STREAM, 123L));
    }

    @Test
    public void testTruncateStreamEvent() {
        testClass(() -> new TruncateStreamEvent(SCOPE, STREAM, 123L));
    }

    @Test
    public void testUpdateStreamEvent() {
        testClass(() -> new UpdateStreamEvent(SCOPE, STREAM, 123L));
    }

    @Test
    public void testCreateTableEvent() {
        testClass(() -> new CreateTableEvent(SCOPE, KVTABLE, 3, System.currentTimeMillis(),
                                            123L, UUID.randomUUID()));
    }

    @Test
    public void testDeleteTableEvent() {
        testClass(() -> new DeleteTableEvent(SCOPE, KVTABLE, 3, UUID.randomUUID()));
    }

    @Test
    public void testCreateReaderGroupEvent() {
        Map<String, RGStreamCutRecord> testMap = new HashMap<String, RGStreamCutRecord>(1);
        testClass(() ->
                new CreateReaderGroupEvent(111L, SCOPE, READER_GROUP,
                        123L, 456L, 10,
                        1, 0L, UUID.randomUUID(), testMap, testMap, System.currentTimeMillis()));
    }

    @Test
    public void testDeleteReaderGroupEvent() {
        testClass(() -> new DeleteReaderGroupEvent(SCOPE, READER_GROUP, 123L, UUID.randomUUID()));
    }

    @Test
    public void testUpdateReaderGroupEvent() {
        testClass(() -> new UpdateReaderGroupEvent(SCOPE, READER_GROUP, 123L, UUID.randomUUID(), 0L, false, ImmutableSet.of()));
    }

    private <T extends ControllerEvent> void testClass(Supplier<T> generateInstance) {
        val s = new ControllerEventSerializer();
        T baseInstance = generateInstance.get();
        val serialization = s.toByteBuffer(baseInstance);
        @SuppressWarnings("unchecked")
        T newInstance = (T) s.fromByteBuffer(serialization);
        Assert.assertEquals(baseInstance, newInstance);
    }
}
