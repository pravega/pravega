/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.GetSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SealSegment;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.TruncateSegment;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentPolicy;
import lombok.extern.slf4j.Slf4j;

/**
 * A RequestProcessor that throws on every method. (Useful to subclass)
 */
@Slf4j
public class FailingRequestProcessor implements RequestProcessor {
    
    @Override
    public void hello(Hello hello) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void setupAppend(SetupAppend setupAppend) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void append(Append appendData) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void readSegment(ReadSegment readSegment) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void updateSegmentAttribute(UpdateSegmentAttribute updateSegmentAttribute) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void getSegmentAttribute(GetSegmentAttribute getSegmentAttribute) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void createSegment(CreateSegment createStreamsSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void updateSegmentPolicy(UpdateSegmentPolicy updateSegmentPolicy) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void createTableSegment(WireCommands.CreateTableSegment createTableSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void deleteTableSegment(WireCommands.DeleteTableSegment deleteSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void updateTableEntries(WireCommands.UpdateTableEntries tableEntries) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void removeTableKeys(WireCommands.RemoveTableKeys tableKeys) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void readTable(WireCommands.ReadTable readTable) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void readTableKeys(WireCommands.ReadTableKeys readTableKeys) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void readTableEntries(WireCommands.ReadTableEntries readTableEntries) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void mergeSegments(WireCommands.MergeSegments mergeSegments) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void mergeTableSegments(WireCommands.MergeTableSegments mergeSegments) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void sealSegment(SealSegment sealSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void sealTableSegment(WireCommands.SealTableSegment sealTableSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void truncateSegment(TruncateSegment truncateSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void deleteSegment(DeleteSegment deleteSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        // This method intentionally left blank.
    }

}
