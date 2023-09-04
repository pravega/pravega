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

import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.UUID;

public class DelegatingRequestProcessorTest {

    private static final Event EMPTY_EVENT = new WireCommands.Event(Unpooled.wrappedBuffer(new byte[1]));

    private DelegatingRequestProcessor rp = new DelegatingRequestProcessor() {

        private RequestProcessor mrp = mock(RequestProcessor.class);

        @Override
        public RequestProcessor getNextRequestProcessor() {
            return mrp;
        }

        @Override
        public void hello(WireCommands.Hello hello) {
            mrp.hello(hello);
        }
    };

    @Test
    public void testEverythingCalled() {
        rp.hello(new WireCommands.Hello(0, 0));
        rp.setupAppend(new WireCommands.SetupAppend(0, null, "", ""));
        rp.append(new Append("", null, 0, EMPTY_EVENT, 0));
        rp.readSegment(new WireCommands.ReadSegment("", 0, 0, "", 0));
        rp.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(0, "", null, 0, 0, ""));
        rp.getSegmentAttribute(new WireCommands.GetSegmentAttribute(0, "", null, ""));
        rp.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(0, "", ""));
        rp.createSegment(new WireCommands.CreateSegment(0, "", (byte) 0, 0, "", 0));
        rp.updateSegmentPolicy(new WireCommands.UpdateSegmentPolicy(0, "", (byte) 0, 0, ""));
        rp.deleteTableSegment(new WireCommands.DeleteTableSegment(0, "", false,  ""));
        rp.keepAlive(new WireCommands.KeepAlive());
        rp.updateTableEntries(new WireCommands.UpdateTableEntries(0, "", "", null, 0));
        rp.removeTableKeys(new WireCommands.RemoveTableKeys(0, "", "", null, 0));
        rp.readTable(new WireCommands.ReadTable(0, "", "", null));
        rp.readTableKeys(new WireCommands.ReadTableKeys(0, "", "", 0, null));
        rp.readTableEntries(new WireCommands.ReadTableEntries(0, "", "", 0, null));
        rp.mergeSegments(new WireCommands.MergeSegments(0, "", "", ""));
        rp.sealSegment(new WireCommands.SealSegment(0, "", ""));
        rp.truncateSegment(new WireCommands.TruncateSegment(0, "", 0, ""));
        rp.deleteSegment(new WireCommands.DeleteSegment(0, "", ""));
        rp.createTableSegment(new WireCommands.CreateTableSegment(0, "", false, 0, "", 0));
        rp.readTableEntriesDelta(new WireCommands.ReadTableEntriesDelta(0, "", "", 0, 0));
        rp.createTransientSegment(new WireCommands.CreateTransientSegment(0, new UUID(0, 0), "", null));
        rp.locateOffset(new WireCommands.LocateOffset(0, "", 0, null));
        rp.connectionDropped();

        verify(rp.getNextRequestProcessor(), times(1)).hello(any());
        verify(rp.getNextRequestProcessor(), times(1)).setupAppend(any());
        verify(rp.getNextRequestProcessor(), times(1)).append(any());
        verify(rp.getNextRequestProcessor(), times(1)).readSegment(any());
        verify(rp.getNextRequestProcessor(), times(1)).updateSegmentAttribute(any());
        verify(rp.getNextRequestProcessor(), times(1)).getSegmentAttribute(any());
        verify(rp.getNextRequestProcessor(), times(1)).getStreamSegmentInfo(any());
        verify(rp.getNextRequestProcessor(), times(1)).createSegment(any());
        verify(rp.getNextRequestProcessor(), times(1)).updateSegmentPolicy(any());
        verify(rp.getNextRequestProcessor(), times(1)).createTableSegment(any());
        verify(rp.getNextRequestProcessor(), times(1)).deleteTableSegment(any());
        verify(rp.getNextRequestProcessor(), times(1)).keepAlive(any());
        verify(rp.getNextRequestProcessor(), times(1)).updateTableEntries(any());
        verify(rp.getNextRequestProcessor(), times(1)).removeTableKeys(any());
        verify(rp.getNextRequestProcessor(), times(1)).readTable(any());
        verify(rp.getNextRequestProcessor(), times(1)).readTableKeys(any());
        verify(rp.getNextRequestProcessor(), times(1)).mergeSegments(any());
        verify(rp.getNextRequestProcessor(), times(1)).sealSegment(any());
        verify(rp.getNextRequestProcessor(), times(1)).truncateSegment(any());
        verify(rp.getNextRequestProcessor(), times(1)).deleteSegment(any());
        verify(rp.getNextRequestProcessor(), times(1)).readTableEntries(any());
        verify(rp.getNextRequestProcessor(), times(1)).readTableEntriesDelta(any());
        verify(rp.getNextRequestProcessor(), times(1)).createTransientSegment(any());
        verify(rp.getNextRequestProcessor(), times(1)).locateOffset(any());
        verify(rp.getNextRequestProcessor(), times(1)).connectionDropped();
    }
}
