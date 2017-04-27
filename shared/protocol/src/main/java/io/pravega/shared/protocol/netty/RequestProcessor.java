/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

/**
 * A class that handles each type of Request. (Visitor pattern)
 */
public interface RequestProcessor {
    void setupAppend(WireCommands.SetupAppend setupAppend);

    void append(Append append);

    void readSegment(WireCommands.ReadSegment readSegment);

    void getStreamSegmentInfo(WireCommands.GetStreamSegmentInfo getStreamInfo);

    void getTransactionInfo(WireCommands.GetTransactionInfo getTransactionInfo);

    void createSegment(WireCommands.CreateSegment createSegment);

    void createTransaction(WireCommands.CreateTransaction createTransaction);

    void commitTransaction(WireCommands.CommitTransaction commitTransaction);
    
    void abortTransaction(WireCommands.AbortTransaction abortTransaction);

    void sealSegment(WireCommands.SealSegment sealSegment);

    void deleteSegment(WireCommands.DeleteSegment deleteSegment);

    void keepAlive(WireCommands.KeepAlive keepAlive);

    void updateSegmentPolicy(WireCommands.UpdateSegmentPolicy updateSegmentPolicy);
}
