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
import com.emc.pravega.common.netty.WireCommands.ConditionalCheckFailed;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.NoSuchTransaction;
import com.emc.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.SegmentDeleted;
import com.emc.pravega.common.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.SegmentSealed;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.TransactionCommitted;
import com.emc.pravega.common.netty.WireCommands.TransactionCreated;
import com.emc.pravega.common.netty.WireCommands.TransactionAborted;
import com.emc.pravega.common.netty.WireCommands.TransactionInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;

/**
 * A class that handles each type of reply. (Visitor pattern)
 */
public interface ReplyProcessor {
    /**
     * A reply to notify requested host is wrong, and sends back correct host for the segment.
     * @param wrongHost A wire command to construct a reply.
     */
    void wrongHost(WrongHost wrongHost);

    /**
     * A reply to notify segment already exists.
     * @param segmentAlreadyExists A wire command to construct a reply.
     */
    void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists);

    /**
     * A reply to notify segment is sealed.
     * @param segmentIsSealed A wire command to construct a reply.
     */
    void segmentIsSealed(SegmentIsSealed segmentIsSealed);

    /**
     * A reply to notify there is not such segment.
     * @param noSuchSegment A wire command to construct a reply.
     */
    void noSuchSegment(NoSuchSegment noSuchSegment);

    /**
     * A reply to notify that there is no such batch.
     * @param noSuchBatch A wire command to construct a reply.
     */
    void noSuchBatch(NoSuchTransaction noSuchBatch);

    /**
     * A reply to append setup request.
     * @param appendSetup A wire command to construct a reply.
     */
    void appendSetup(AppendSetup appendSetup);

    /**
     * A reply to notify data is appended successfully.
     * @param dataAppended A wire command to construct a reply.
     */
    void dataAppended(DataAppended dataAppended);

    /**
     * A reply to notify that data is not appended to a segment.
     * @param dataNotAppended A wire command to construct a reply.
     */
    void conditionalCheckFailed(ConditionalCheckFailed dataNotAppended);

    /**
     * A reply to read segment request.
     * @param segmentRead A wire command to construct a reply.
     */
    void segmentRead(SegmentRead segmentRead);

    /**
     * A reply with stream information.
     * @param streamInfo A wire command to construct a reply.
     */
    void streamSegmentInfo(StreamSegmentInfo streamInfo);

    /**
     * A reply with transaction information.
     * @param transactionInfo A wire command to construct a reply.
     */
    void transactionInfo(TransactionInfo transactionInfo);

    /**
     * A reply to notify segment is sealed.
     * @param segmentCreated A wire command to construct a reply.
     */
    void segmentCreated(SegmentCreated segmentCreated);

    /**
     * A reply to notify transaction is created.
     * @param transactionCreated A wire command  to construct a reply.
     */
    void transactionCreated(TransactionCreated transactionCreated);

    /**
     * A reply to notify transaction is committed.
     * @param transactionCommitted A wire command to construct a reply.
     */
    void transactionCommitted(TransactionCommitted transactionCommitted);

    /**
     * A reply to notify transaction is aborted.
     * @param transactionAborted A wire command to construct a reply.
     */
    void transactionAborted(TransactionAborted transactionAborted);

    /**
     * A reply to notify segment is sealed.
     * @param segmentSealed A wire command to construct a reply.
     */
    void segmentSealed(SegmentSealed segmentSealed);

    /**
     * A reply to notify segment is deleted.
     * @param segmentDeleted A wire command to notify a segment is deleted.
     */
    void segmentDeleted(SegmentDeleted segmentDeleted);

    /**
     * A reply for keep alive.
     * @param keepAlive A wire command for keep alive.
     */
    void keepAlive(KeepAlive keepAlive);

    /**
     * A connection is dropped.
     */
    void connectionDropped();
}
