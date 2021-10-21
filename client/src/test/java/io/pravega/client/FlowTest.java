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
package io.pravega.client;


import io.pravega.client.connection.impl.Flow;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FlowTest {

    @Test
    public void testNextSequenceNumber() {
        Flow id = Flow.create();
        assertEquals(id.getFlowId(), (int) (id.asLong() >> 32));
        assertEquals("SequenceNumber should be 0 for " + id, 0, (int) id.asLong());
        assertEquals("SequenceNumber should be incremented", (int) id.asLong() + 1, (int) id.getNextSequenceNumber());
        assertEquals(id, Flow.from(id.asLong()));
    }

    @Test
    public void testSequenceNumberOverflow() {
        Flow id = new Flow(Integer.MAX_VALUE, Integer.MAX_VALUE);
        assertEquals(0,  (int) id.getNextSequenceNumber());
    }
}