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

/**
 * A RequestProcessor that hands off all implementation to another RequestProcessor.
 * This is useful for creating subclasses that only handle a subset of Commands.
 */
public abstract class DelegatingRequestProcessor implements RequestProcessor {

    public abstract RequestProcessor getNextRequestProcessor();

    @Override
    public void setupAppend(SetupAppend setupAppend) {
        getNextRequestProcessor().setupAppend(setupAppend);
    }

    @Override
    public void append(Append append) {
        getNextRequestProcessor().append(append);
    }

    @Override
    public void readSegment(ReadSegment readSegment) {
        getNextRequestProcessor().readSegment(readSegment);
    }

    @Override
    public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo) {
        getNextRequestProcessor().getStreamSegmentInfo(getStreamInfo);
    }
    
    @Override
    public void getTransactionInfo(GetTransactionInfo getTransactionInfo) {
        getNextRequestProcessor().getTransactionInfo(getTransactionInfo);
    }
    
    @Override
    public void createSegment(CreateSegment createStreamsSegment) {
        getNextRequestProcessor().createSegment(createStreamsSegment);
    }

    @Override
    public void updateSegmentPolicy(WireCommands.UpdateSegmentPolicy updateSegmentPolicy) {
        getNextRequestProcessor().updateSegmentPolicy(updateSegmentPolicy);
    }


    @Override
    public void createTransaction(CreateTransaction createTransaction) {
        getNextRequestProcessor().createTransaction(createTransaction);
    }

    @Override
    public void commitTransaction(CommitTransaction commitTransaction) {
        getNextRequestProcessor().commitTransaction(commitTransaction);
    }

    @Override
    public void abortTransaction(AbortTransaction abortTransaction) {
        getNextRequestProcessor().abortTransaction(abortTransaction);
    }
    
    @Override
    public void sealSegment(SealSegment sealSegment) {
        getNextRequestProcessor().sealSegment(sealSegment);
    }

    @Override
    public void deleteSegment(DeleteSegment deleteSegment) {
        getNextRequestProcessor().deleteSegment(deleteSegment);
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        getNextRequestProcessor().keepAlive(keepAlive);
    }

}
