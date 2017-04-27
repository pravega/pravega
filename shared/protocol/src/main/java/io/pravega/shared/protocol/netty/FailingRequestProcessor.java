/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

import lombok.extern.slf4j.Slf4j;

/**
 * A RequestProcessor that throws on every method. (Useful to subclass)
 */
@Slf4j
public class FailingRequestProcessor implements RequestProcessor {

    @Override
    public void setupAppend(WireCommands.SetupAppend setupAppend) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void append(Append appendData) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void readSegment(WireCommands.ReadSegment readSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void getStreamSegmentInfo(WireCommands.GetStreamSegmentInfo getStreamInfo) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void getTransactionInfo(WireCommands.GetTransactionInfo getTransactionInfo) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void createSegment(WireCommands.CreateSegment createStreamsSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void updateSegmentPolicy(WireCommands.UpdateSegmentPolicy updateSegmentPolicy) {
        throw new IllegalStateException("Unexpected operation");
    }


    @Override
    public void createTransaction(WireCommands.CreateTransaction createTransaction) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void commitTransaction(WireCommands.CommitTransaction commitTransaction) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void abortTransaction(WireCommands.AbortTransaction abortTransaction) {
        throw new IllegalStateException("Unexpected operation");
    }
    
    @Override
    public void sealSegment(WireCommands.SealSegment sealSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void deleteSegment(WireCommands.DeleteSegment deleteSegment) {
        throw new IllegalStateException("Unexpected operation");
    }

    @Override
    public void keepAlive(WireCommands.KeepAlive keepAlive) {
        log.debug("Received KeepAlive");
    }

}
