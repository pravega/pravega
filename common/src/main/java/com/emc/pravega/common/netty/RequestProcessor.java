/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
    void setupAppend(SetupAppend setupAppend);

    void append(Append append);

    void readSegment(ReadSegment readSegment);

    void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo);

    void getTransactionInfo(GetTransactionInfo getTransactionInfo);

    void createSegment(CreateSegment createSegment);

    void createTransaction(CreateTransaction createTransaction);

    void commitTransaction(CommitTransaction commitTransaction);
    
    void abortTransaction(AbortTransaction abortTransaction);

    void sealSegment(SealSegment sealSegment);

    void deleteSegment(DeleteSegment deleteSegment);

    void keepAlive(KeepAlive keepAlive);
}
