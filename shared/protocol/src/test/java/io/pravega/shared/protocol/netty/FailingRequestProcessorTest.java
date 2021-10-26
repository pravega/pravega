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
package io.pravega.shared.protocol.netty;

import io.netty.buffer.Unpooled;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.GetSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentPolicy;
import io.pravega.shared.protocol.netty.WireCommands.CreateTableSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteTableSegment;
import io.pravega.shared.protocol.netty.WireCommands.UpdateTableEntries;
import io.pravega.shared.protocol.netty.WireCommands.RemoveTableKeys;
import io.pravega.shared.protocol.netty.WireCommands.ReadTable;
import io.pravega.shared.protocol.netty.WireCommands.ReadTableKeys;
import io.pravega.shared.protocol.netty.WireCommands.ReadTableEntries;
import io.pravega.shared.protocol.netty.WireCommands.MergeSegments;
import io.pravega.shared.protocol.netty.WireCommands.SealSegment;
import io.pravega.shared.protocol.netty.WireCommands.TruncateSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.ReadTableEntriesDelta;
import io.pravega.shared.protocol.netty.WireCommands.CreateTransientSegment;

import org.junit.Test;

import java.util.UUID;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public class FailingRequestProcessorTest {

    private static final Event EMPTY_EVENT = new Event(Unpooled.wrappedBuffer(new byte[1]));

    private final RequestProcessor rp = new FailingRequestProcessor();

    @Test
    public void testEverythingThrows() {
        assertThrows(IllegalStateException.class, () -> rp.hello(new Hello(0, 0)));
        assertThrows(IllegalStateException.class, () -> rp.setupAppend(new SetupAppend(0, null, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.append(new Append("", null, 0, EMPTY_EVENT, 0)));
        assertThrows(IllegalStateException.class, () -> rp.readSegment(new ReadSegment("", 0, 0, "", 0)));
        assertThrows(IllegalStateException.class, () -> rp.updateSegmentAttribute(new UpdateSegmentAttribute(0, "", null, 0, 0, "")));
        assertThrows(IllegalStateException.class, () -> rp.getSegmentAttribute(new GetSegmentAttribute(0, "", null, "")));
        assertThrows(IllegalStateException.class, () -> rp.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(0, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.createSegment(new CreateSegment(0, "", (byte) 0, 0, "", 0)));
        assertThrows(IllegalStateException.class, () -> rp.updateSegmentPolicy(new UpdateSegmentPolicy(0, "", (byte) 0, 0, "")));
        assertThrows(IllegalStateException.class, () -> rp.createTableSegment(new CreateTableSegment(0, "", false, 0, "", 0)));
        assertThrows(IllegalStateException.class, () -> rp.deleteTableSegment(new DeleteTableSegment(0, "", false,  "")));
        assertThrows(IllegalStateException.class, () -> rp.updateTableEntries(new UpdateTableEntries(0, "", "", null, 0)));
        assertThrows(IllegalStateException.class, () -> rp.removeTableKeys(new RemoveTableKeys(0, "", "", null, 0)));
        assertThrows(IllegalStateException.class, () -> rp.readTable(new ReadTable(0, "", "", null)));
        assertThrows(IllegalStateException.class, () -> rp.readTableKeys(new ReadTableKeys(0, "", "", 0, null)));
        assertThrows(IllegalStateException.class, () -> rp.readTableEntries(new ReadTableEntries(0, "", "", 0, null)));
        assertThrows(IllegalStateException.class, () -> rp.mergeSegments(new MergeSegments(0, "", "", "")));
        assertThrows(IllegalStateException.class, () -> rp.sealSegment(new SealSegment(0, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.truncateSegment(new TruncateSegment(0, "", 0, "")));
        assertThrows(IllegalStateException.class, () -> rp.deleteSegment(new DeleteSegment(0, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.readTableEntries(new ReadTableEntries(0, "", "", 0, null)));
        assertThrows(IllegalStateException.class, () -> rp.createTableSegment(new CreateTableSegment(0, "", false, 0, "", 0)));
        assertThrows(IllegalStateException.class, () -> rp.readTableEntriesDelta(new ReadTableEntriesDelta(0, "", "", 0, 0)));
        assertThrows(IllegalStateException.class, () -> rp.createTransientSegment(new CreateTransientSegment(0, new UUID(0, 0), "", "")));
        assertThrows(IllegalStateException.class, () -> rp.connectionDropped());
    }

}
