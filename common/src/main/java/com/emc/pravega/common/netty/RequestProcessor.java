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

import com.emc.pravega.common.netty.WireCommands.AbortTransaction;
import com.emc.pravega.common.netty.WireCommands.CommitTransaction;
import com.emc.pravega.common.netty.WireCommands.CreateSegment;
import com.emc.pravega.common.netty.WireCommands.CreateTransaction;
import com.emc.pravega.common.netty.WireCommands.DeleteSegment;
import com.emc.pravega.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.GetTransactionInfo;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SealSegment;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;

/**
 * A class that handles each type of Request. (Visitor pattern)
 */
public interface RequestProcessor {

    /**
     * A request to setup an append.
     *
     * @param setupAppend A wire command to setup an append.
     */
    void setupAppend(SetupAppend setupAppend);

    /**
     * A request to append to a segment.
     *
     * @param append A wire command to append to a segment.
     */
    void append(Append append);

    /**
     * A request to read a segment.
     *
     * @param readSegment A wire command to read a segment.
     */
    void readSegment(ReadSegment readSegment);

    /**
     * A request to get a stream information.
     *
     * @param getStreamInfo A wire command to get a stream info.
     */
    void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo);

    /**
     * A request to get a transaction information.
     *
     * @param getTransactionInfo A wire command to get a transaction info.
     */
    void getTransactionInfo(GetTransactionInfo getTransactionInfo);

    /**
     * A request to create a segment.
     *
     * @param createSegment A wire command to create a segment.
     */
    void createSegment(CreateSegment createSegment);

    /**
     * A request to create a transaction.
     *
     * @param createTransaction A wire command to create a Transaction
     */
    void createTransaction(CreateTransaction createTransaction);

    /**
     * A request to commit a transaction.
     *
     * @param commitTransaction A wire command to commit a transaction.
     */
    void commitTransaction(CommitTransaction commitTransaction);

    /**
     * A request to drop a transaction.
     *
     * @param dropTransaction A wire command to drop a transaction.
     */
    void abortTransaction(AbortTransaction abortTransaction);

    /**
     * A request to seal a segment.
     *
     * @param sealSegment A wire command to seal a segment
     */
    void sealSegment(SealSegment sealSegment);

    /**
     * A request to delete a segment.
     *
     * @param deleteSegment A wire command to delete a segment
     */
    void deleteSegment(DeleteSegment deleteSegment);

    /**
     * Keep Alive request type.
     *
     * @param keepAlive a wire command for keep alive.
     */
    void keepAlive(KeepAlive keepAlive);
}
