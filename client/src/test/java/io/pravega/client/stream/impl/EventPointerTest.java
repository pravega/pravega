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
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventPointer;
import java.io.IOException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventPointerTest {

    /**
     * Simple exercise of event pointers. The test case
     * creates an impl instance with some arbitrary values,
     * serializes the pointer, deserializes it, and asserts
     * that the values obtained are the expected ones.
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    @Test
    public void testEventPointerImpl() throws IOException, ClassNotFoundException {
        String scope = "testScope";
        String stream = "testStream";
        int segmentId = 1;
        Segment segment = new Segment(scope, stream, segmentId);
        EventPointer pointer = new EventPointerImpl(segment, 10L, 10);
        EventPointer pointerRead = EventPointer.fromBytes(pointer.toBytes());
        assertEquals(pointer, pointerRead);

        StringBuilder name = new StringBuilder();
        name.append(scope);
        name.append("/");
        name.append(stream);
        assertEquals(name.toString(), pointerRead.asImpl().getSegment().getScopedStreamName());

        name.append("/");
        name.append(segmentId);
        name.append(".#epoch.0");
        assertEquals(name.toString(), pointerRead.asImpl().getSegment().getScopedName());

        assertTrue(pointerRead.asImpl().getEventStartOffset() == 10L);
        assertTrue(pointerRead.asImpl().getEventLength() == 10);
    }
}
