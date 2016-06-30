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

public abstract class DelegatingReplyProcessor implements ReplyProcessor {

    public abstract ReplyProcessor getNextReplyProcessor();

    @Override
    public void wrongHost(WrongHost wrongHost) {
        getNextReplyProcessor().wrongHost(wrongHost);
    }

    @Override
    public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
        getNextReplyProcessor().segmentIsSealed(segmentIsSealed);
    }

    @Override
    public void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists) {
        getNextReplyProcessor().segmentAlreadyExists(segmentAlreadyExists);
    }

    @Override
    public void noSuchSegment(NoSuchSegment noSuchSegment) {
        getNextReplyProcessor().noSuchSegment(noSuchSegment);
    }

    @Override
    public void noSuchBatch(NoSuchBatch noSuchBatch) {
        getNextReplyProcessor().noSuchBatch(noSuchBatch);
    }

    @Override
    public void appendSetup(AppendSetup appendSetup) {
        getNextReplyProcessor().appendSetup(appendSetup);
    }

    @Override
    public void dataAppended(DataAppended dataAppended) {
        getNextReplyProcessor().dataAppended(dataAppended);
    }

    @Override
    public void segmentRead(SegmentRead data) {
        getNextReplyProcessor().segmentRead(data);
    }

    @Override
    public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
        getNextReplyProcessor().streamSegmentInfo(streamInfo);
    }

    @Override
    public void segmentCreated(SegmentCreated streamsSegmentCreated) {
        getNextReplyProcessor().segmentCreated(streamsSegmentCreated);
    }

    @Override
    public void batchCreated(BatchCreated batchCreated) {
        getNextReplyProcessor().batchCreated(batchCreated);
    }

    @Override
    public void batchMerged(BatchMerged batchMerged) {
        getNextReplyProcessor().batchMerged(batchMerged);
    }

    @Override
    public void segmentSealed(SegmentSealed segmentSealed) {
        getNextReplyProcessor().segmentSealed(segmentSealed);
    }

    @Override
    public void segmentDeleted(SegmentDeleted segmentDeleted) {
        getNextReplyProcessor().segmentDeleted(segmentDeleted);
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        getNextReplyProcessor().keepAlive(keepAlive);
    }

}
