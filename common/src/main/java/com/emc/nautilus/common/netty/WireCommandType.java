/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.nautilus.common.netty;

import java.io.DataInput;
import java.io.IOException;

import com.emc.nautilus.common.netty.WireCommands.AppendBlock;
import com.emc.nautilus.common.netty.WireCommands.AppendBlockEnd;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.BatchCreated;
import com.emc.nautilus.common.netty.WireCommands.BatchMerged;
import com.emc.nautilus.common.netty.WireCommands.Constructor;
import com.emc.nautilus.common.netty.WireCommands.CreateBatch;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.DeleteSegment;
import com.emc.nautilus.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.MergeBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.Padding;
import com.emc.nautilus.common.netty.WireCommands.PartialEvent;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SealSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.SegmentDeleted;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SegmentSealed;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;
import com.google.common.base.Preconditions;

enum WireCommandType {
    PADDING(-1, Padding::readFrom),

    PARTIAL_EVENT(-2, PartialEvent::readFrom),

    APPEND(0, null), // Does not go over the wire, is converted to an event.
    EVENT(0, null), // Is read manually.

    SETUP_APPEND(1, SetupAppend::readFrom),
    APPEND_SETUP(2, AppendSetup::readFrom),

    APPEND_BLOCK(3, AppendBlock::readFrom),
    APPEND_BLOCK_END(4, AppendBlockEnd::readFrom),

    DATA_APPENDED(5, DataAppended::readFrom),

    READ_SEGMENT(6, ReadSegment::readFrom),
    SEGMENT_READ(7, null), // Is read manually.

    GET_STREAM_SEGMENT_INFO(8, GetStreamSegmentInfo::readFrom),
    STREAM_SEGMENT_INFO(9, StreamSegmentInfo::readFrom),

    CREATE_SEGMENT(20, CreateSegment::readFrom),
    SEGMENT_CREATED(21, SegmentCreated::readFrom),

    CREATE_BATCH(22, CreateBatch::readFrom),
    BATCH_CREATED(23, BatchCreated::readFrom),

    MERGE_BATCH(24, MergeBatch::readFrom),
    BATCH_MERGED(25, BatchMerged::readFrom),

    SEAL_SEGMENT(26, SealSegment::readFrom),
    SEGMENT_SEALED(27, SegmentSealed::readFrom),

    DELETE_SEGMENT(28, DeleteSegment::readFrom),
    SEGMENT_DELETED(29, SegmentDeleted::readFrom),

    WRONG_HOST(50, WrongHost::readFrom),
    SEGMENT_IS_SEALED(51, SegmentIsSealed::readFrom),
    SEGMENT_ALREADY_EXISTS(52, SegmentAlreadyExists::readFrom),
    NO_SUCH_SEGMENT(53, NoSuchSegment::readFrom),
    NO_SUCH_BATCH(54, NoSuchBatch::readFrom),

    KEEP_ALIVE(100, KeepAlive::readFrom);

    private final int code;
    private final Constructor factory;

    WireCommandType(int code, Constructor factory) {
        Preconditions.checkArgument(code <= 127 && code >= -127, "All codes should fit in a byte.");
        this.code = code;
        this.factory = factory;
    }

    public int getCode() {
        return code;
    }

    public WireCommand readFrom(DataInput in, int length) throws IOException {
        return factory.readFrom(in, length);
    }
}