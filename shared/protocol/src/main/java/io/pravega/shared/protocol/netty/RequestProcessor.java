/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

import io.pravega.shared.protocol.netty.WireCommands.AbortTransaction;
import io.pravega.shared.protocol.netty.WireCommands.CommitTransaction;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.CreateTransaction;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.GetStreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.GetTransactionInfo;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SealSegment;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.shared.protocol.netty.WireCommands.UpdateSegmentPolicy;

/**
 * A class that handles each type of Request. (Visitor pattern)
 */
public interface RequestProcessor {
    void hello(Hello hello);
    
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
