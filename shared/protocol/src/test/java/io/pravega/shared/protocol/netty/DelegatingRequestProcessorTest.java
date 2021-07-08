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

public class DelegatingRequestProcessorTest {

    private static final Event EMPTY_EVENT = new WireCommands.Event(Unpooled.wrappedBuffer(new byte[1]));

    private MockRequestProcessor rp = new MockRequestProcessor();

    @Test
    public void testEverythingCalled() {
        rp.hello(new WireCommands.Hello(0, 0));
        rp.setupAppend(new WireCommands.SetupAppend(0, null, "", ""));
        rp.append(new Append("", null, 0, EMPTY_EVENT, 0));
        rp.readSegment(new WireCommands.ReadSegment("", 0, 0, "", 0));
        rp.updateSegmentAttribute(new WireCommands.UpdateSegmentAttribute(0, "", null, 0, 0, ""));
        rp.getSegmentAttribute(new WireCommands.GetSegmentAttribute(0, "", null, ""));
        rp.getStreamSegmentInfo(new WireCommands.GetStreamSegmentInfo(0, "", ""));
        rp.createSegment(new WireCommands.CreateSegment(0, "", (byte) 0, 0, ""));
        rp.updateSegmentPolicy(new WireCommands.UpdateSegmentPolicy(0, "", (byte) 0, 0, ""));
        rp.createTableSegment(new WireCommands.CreateTableSegment(0, "", false, ""));
        rp.deleteTableSegment(new WireCommands.DeleteTableSegment(0, "", false,  ""));
        rp.updateTableEntries(new WireCommands.UpdateTableEntries(0, "", "", null, 0));
        rp.removeTableKeys(new WireCommands.RemoveTableKeys(0, "", "", null, 0));
        rp.readTable(new WireCommands.ReadTable(0, "", "", null));
        rp.readTableKeys(new WireCommands.ReadTableKeys(0, "", "", 0, null, null));
        rp.readTableEntries(new WireCommands.ReadTableEntries(0, "", "", 0, null, null));
        rp.mergeSegments(new WireCommands.MergeSegments(0, "", "", ""));
        rp.mergeTableSegments(new WireCommands.MergeTableSegments(0, "", "", ""));
        rp.sealSegment(new WireCommands.SealSegment(0, "", ""));
        rp.sealTableSegment(new WireCommands.SealTableSegment(0, "", ""));
        rp.truncateSegment(new WireCommands.TruncateSegment(0, "", 0, ""));
        rp.deleteSegment(new WireCommands.DeleteSegment(0, "", ""));
        rp.readTableEntries(new WireCommands.ReadTableEntries(0, "", "", 0, null, null));
        rp.createTableSegment(new WireCommands.CreateTableSegment(0, "", false, ""));
        rp.readTableEntriesDelta(new WireCommands.ReadTableEntriesDelta(0, "", "", 0, 0));
        rp.createTransientSegment(new WireCommands.CreateTransientSegment(0, "", ""));
    }

    private class EmptyRequestProcessor implements RequestProcessor {
        @Override
        public void hello(WireCommands.Hello hello) {

        }

        @Override
        public void setupAppend(WireCommands.SetupAppend setupAppend) {

        }

        @Override
        public void append(Append append) {

        }

        @Override
        public void readSegment(WireCommands.ReadSegment readSegment) {

        }

        @Override
        public void updateSegmentAttribute(WireCommands.UpdateSegmentAttribute updateSegmentAttribute) {

        }

        @Override
        public void getSegmentAttribute(WireCommands.GetSegmentAttribute getSegmentAttribute) {

        }

        @Override
        public void getStreamSegmentInfo(WireCommands.GetStreamSegmentInfo getStreamInfo) {

        }

        @Override
        public void createSegment(WireCommands.CreateSegment createSegment) {

        }

        @Override
        public void mergeSegments(WireCommands.MergeSegments mergeSegments) {

        }

        @Override
        public void mergeTableSegments(WireCommands.MergeTableSegments mergeSegments) {

        }

        @Override
        public void sealSegment(WireCommands.SealSegment sealSegment) {

        }

        @Override
        public void sealTableSegment(WireCommands.SealTableSegment sealTableSegment) {

        }

        @Override
        public void truncateSegment(WireCommands.TruncateSegment truncateSegment) {

        }

        @Override
        public void deleteSegment(WireCommands.DeleteSegment deleteSegment) {

        }

        @Override
        public void keepAlive(WireCommands.KeepAlive keepAlive) {

        }

        @Override
        public void updateSegmentPolicy(WireCommands.UpdateSegmentPolicy updateSegmentPolicy) {

        }

        @Override
        public void createTableSegment(WireCommands.CreateTableSegment createTableSegment) {

        }

        @Override
        public void deleteTableSegment(WireCommands.DeleteTableSegment deleteSegment) {

        }

        @Override
        public void updateTableEntries(WireCommands.UpdateTableEntries tableEntries) {

        }

        @Override
        public void removeTableKeys(WireCommands.RemoveTableKeys tableKeys) {

        }

        @Override
        public void readTable(WireCommands.ReadTable readTable) {

        }

        @Override
        public void readTableKeys(WireCommands.ReadTableKeys readTableKeys) {

        }

        @Override
        public void readTableEntries(WireCommands.ReadTableEntries readTableEntries) {

        }

        @Override
        public void readTableEntriesDelta(WireCommands.ReadTableEntriesDelta readTableEntriesDelta) {

        }

        @Override
        public void createTransientSegment(WireCommands.CreateTransientSegment createTransientSegment) {

        }
    }

    private class MockRequestProcessor extends DelegatingRequestProcessor {
        RequestProcessor empty = new EmptyRequestProcessor();

        @Override
        public RequestProcessor getNextRequestProcessor() {
            return empty;
        }

        @Override
        public void hello(WireCommands.Hello hello) {

        }
    }
}
