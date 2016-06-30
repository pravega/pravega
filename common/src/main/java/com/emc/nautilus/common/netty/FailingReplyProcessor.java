/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.BatchCreated;
import com.emc.nautilus.common.netty.WireCommands.BatchMerged;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.NoSuchBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.SegmentDeleted;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.netty.WireCommands.SegmentSealed;
import com.emc.nautilus.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

public class FailingReplyProcessor implements ReplyProcessor {

    @Override
    public void wrongHost(WrongHost wrongHost) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void noSuchSegment(NoSuchSegment noSuchSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void noSuchBatch(NoSuchBatch noSuchBatch) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void appendSetup(AppendSetup appendSetup) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void dataAppended(DataAppended dataAppended) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void segmentRead(SegmentRead data) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void segmentCreated(SegmentCreated streamsSegmentCreated) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void batchCreated(BatchCreated batchCreated) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void batchMerged(BatchMerged batchMerged) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void segmentSealed(SegmentSealed segmentSealed) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void segmentDeleted(SegmentDeleted segmentDeleted) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        throw new IllegalStateException("Unexpected operation");
    }

}
