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
package com.emc.pravega.common.netty;

import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.BatchCreated;
import com.emc.pravega.common.netty.WireCommands.BatchMerged;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.NoSuchBatch;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.SegmentDeleted;
import com.emc.pravega.common.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.SegmentSealed;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;

/**
 * A class that handles each type of reply. (Visitor pattern)
 */
public interface ReplyProcessor {
    void wrongHost(WrongHost wrongHost);

    void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists);

    void segmentIsSealed(SegmentIsSealed segmentIsSealed);

    void noSuchSegment(NoSuchSegment noSuchSegment);

    void noSuchBatch(NoSuchBatch noSuchBatch);

    void appendSetup(AppendSetup appendSetup);

    void dataAppended(DataAppended dataAppended);

    void segmentRead(SegmentRead segmentRead);

    void streamSegmentInfo(StreamSegmentInfo streamInfo);

    void segmentCreated(SegmentCreated segmentCreated);

    void batchCreated(BatchCreated batchCreated);

    void batchMerged(BatchMerged batchMerged);

    void segmentSealed(SegmentSealed segmentSealed);

    void segmentDeleted(SegmentDeleted segmentDeleted);

    void keepAlive(KeepAlive keepAlive);
}
