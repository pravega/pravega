/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.netty;

import io.pravega.common.netty.WireCommands.AbortTransaction;
import io.pravega.common.netty.WireCommands.CommitTransaction;
import io.pravega.common.netty.WireCommands.CreateSegment;
import io.pravega.common.netty.WireCommands.CreateTransaction;
import io.pravega.common.netty.WireCommands.DeleteSegment;
import io.pravega.common.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.common.netty.WireCommands.GetTransactionInfo;
import io.pravega.common.netty.WireCommands.KeepAlive;
import io.pravega.common.netty.WireCommands.ReadSegment;
import io.pravega.common.netty.WireCommands.SealSegment;
import io.pravega.common.netty.WireCommands.SetupAppend;
import io.pravega.common.netty.WireCommands.UpdateSegmentPolicy;

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

    void updateSegmentPolicy(UpdateSegmentPolicy updateSegmentPolicy);
}
