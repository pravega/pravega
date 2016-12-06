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

import com.emc.pravega.common.netty.WireCommands.CommitTransaction;
import com.emc.pravega.common.netty.WireCommands.CreateSegment;
import com.emc.pravega.common.netty.WireCommands.CreateTransaction;
import com.emc.pravega.common.netty.WireCommands.DeleteSegment;
import com.emc.pravega.common.netty.WireCommands.DropTransaction;
import com.emc.pravega.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.GetTransactionInfo;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SealSegment;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;

import lombok.extern.slf4j.Slf4j;

/**
 * A RequestProcessor that throws on every method. (Useful to subclass)
 */
@Slf4j
public class FailingRequestProcessor implements RequestProcessor {

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
    public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void getTransactionInfo(GetTransactionInfo getTransactionInfo) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void createSegment(CreateSegment createStreamsSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void createTransaction(CreateTransaction createTransaction) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void commitTransaction(CommitTransaction commitTransaction) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void dropTransaction(DropTransaction dropTransaction) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void sealSegment(SealSegment sealSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void deleteSegment(DeleteSegment deleteSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        log.debug("Received KeepAlive");
    }

}
