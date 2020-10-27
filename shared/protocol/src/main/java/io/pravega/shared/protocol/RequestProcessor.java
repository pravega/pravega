/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol;

import io.pravega.shared.protocol.WireCommands.CreateSegment;
import io.pravega.shared.protocol.WireCommands.CreateTableSegment;
import io.pravega.shared.protocol.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.WireCommands.DeleteTableSegment;
import io.pravega.shared.protocol.WireCommands.GetSegmentAttribute;
import io.pravega.shared.protocol.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.WireCommands.Hello;
import io.pravega.shared.protocol.WireCommands.KeepAlive;
import io.pravega.shared.protocol.WireCommands.MergeSegments;
import io.pravega.shared.protocol.WireCommands.MergeTableSegments;
import io.pravega.shared.protocol.WireCommands.ReadSegment;
import io.pravega.shared.protocol.WireCommands.RemoveTableKeys;
import io.pravega.shared.protocol.WireCommands.SealSegment;
import io.pravega.shared.protocol.WireCommands.SealTableSegment;
import io.pravega.shared.protocol.WireCommands.SetupAppend;
import io.pravega.shared.protocol.WireCommands.TruncateSegment;
import io.pravega.shared.protocol.WireCommands.UpdateSegmentAttribute;
import io.pravega.shared.protocol.WireCommands.UpdateSegmentPolicy;
import io.pravega.shared.protocol.WireCommands.UpdateTableEntries;

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

    void mergeTableSegments(MergeTableSegments mergeSegments);

    void sealSegment(SealSegment sealSegment);

    void sealTableSegment(SealTableSegment sealTableSegment);

    void truncateSegment(TruncateSegment truncateSegment);

    void deleteSegment(DeleteSegment deleteSegment);

    void keepAlive(KeepAlive keepAlive);

    void updateSegmentPolicy(UpdateSegmentPolicy updateSegmentPolicy);

    void createTableSegment(CreateTableSegment createTableSegment);

    void deleteTableSegment(DeleteTableSegment deleteSegment);

    void updateTableEntries(UpdateTableEntries tableEntries);

    void removeTableKeys(RemoveTableKeys tableKeys);

    void readTable(WireCommands.ReadTable readTable);

    void readTableKeys(WireCommands.ReadTableKeys readTableKeys);

    void readTableEntries(WireCommands.ReadTableEntries readTableEntries);

    void readTableEntriesDelta(WireCommands.ReadTableEntriesDelta readTableEntriesDelta);
}
