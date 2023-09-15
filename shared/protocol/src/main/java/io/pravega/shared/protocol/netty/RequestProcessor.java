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

import io.pravega.shared.protocol.netty.WireCommands.CreateTableSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteTableSegment;
import io.pravega.shared.protocol.netty.WireCommands.MergeSegments;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.GetSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.RemoveTableKeys;
import io.pravega.shared.protocol.netty.WireCommands.UpdateTableEntries;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SealSegment;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.TruncateSegment;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentPolicy;

/**
 * A class that handles each type of Request. (Visitor pattern)
 */
public interface RequestProcessor {
    void hello(Hello hello);
    
    void setupAppend(SetupAppend setupAppend);

    void append(Append append);

    void readSegment(ReadSegment readSegment);
    
    void updateSegmentAttribute(UpdateSegmentAttribute updateSegmentAttribute);
    
    void getSegmentAttribute(GetSegmentAttribute getSegmentAttribute);

    void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo);

    void createSegment(CreateSegment createSegment);

    void mergeSegments(MergeSegments mergeSegments);
    
    void mergeSegmentsBatch(WireCommands.MergeSegmentsBatch mergeSegments);

    void sealSegment(SealSegment sealSegment);

    void truncateSegment(TruncateSegment truncateSegment);

    void deleteSegment(DeleteSegment deleteSegment);

    void keepAlive(KeepAlive keepAlive);

    void updateSegmentPolicy(UpdateSegmentPolicy updateSegmentPolicy);

    void getTableSegmentInfo(WireCommands.GetTableSegmentInfo getInfo);

    void createTableSegment(CreateTableSegment createTableSegment);

    void deleteTableSegment(DeleteTableSegment deleteSegment);

    void updateTableEntries(UpdateTableEntries tableEntries);

    void removeTableKeys(RemoveTableKeys tableKeys);

    void readTable(WireCommands.ReadTable readTable);

    void readTableKeys(WireCommands.ReadTableKeys readTableKeys);

    void readTableEntries(WireCommands.ReadTableEntries readTableEntries);

    void readTableEntriesDelta(WireCommands.ReadTableEntriesDelta readTableEntriesDelta);

    void createTransientSegment(WireCommands.CreateTransientSegment createTransientSegment);
    
    void locateOffset(WireCommands.LocateOffset locateOffset);

    void connectionDropped();
}
