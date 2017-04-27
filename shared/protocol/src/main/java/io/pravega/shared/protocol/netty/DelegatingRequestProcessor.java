/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

/**
 * A RequestProcessor that hands off all implementation to another RequestProcessor.
 * This is useful for creating subclasses that only handle a subset of Commands.
 */
public abstract class DelegatingRequestProcessor implements RequestProcessor {

    public abstract RequestProcessor getNextRequestProcessor();

    @Override
    public void setupAppend(WireCommands.SetupAppend setupAppend) {
        getNextRequestProcessor().setupAppend(setupAppend);
    }

    @Override
    public void append(Append append) {
        getNextRequestProcessor().append(append);
    }

    @Override
    public void readSegment(WireCommands.ReadSegment readSegment) {
        getNextRequestProcessor().readSegment(readSegment);
    }

    @Override
    public void getStreamSegmentInfo(WireCommands.GetStreamSegmentInfo getStreamInfo) {
        getNextRequestProcessor().getStreamSegmentInfo(getStreamInfo);
    }
    
    @Override
    public void getTransactionInfo(WireCommands.GetTransactionInfo getTransactionInfo) {
        getNextRequestProcessor().getTransactionInfo(getTransactionInfo);
    }
    
    @Override
    public void createSegment(WireCommands.CreateSegment createStreamsSegment) {
        getNextRequestProcessor().createSegment(createStreamsSegment);
    }

    @Override
    public void updateSegmentPolicy(WireCommands.UpdateSegmentPolicy updateSegmentPolicy) {
        getNextRequestProcessor().updateSegmentPolicy(updateSegmentPolicy);
    }


    @Override
    public void createTransaction(WireCommands.CreateTransaction createTransaction) {
        getNextRequestProcessor().createTransaction(createTransaction);
    }

    @Override
    public void commitTransaction(WireCommands.CommitTransaction commitTransaction) {
        getNextRequestProcessor().commitTransaction(commitTransaction);
    }

    @Override
    public void abortTransaction(WireCommands.AbortTransaction abortTransaction) {
        getNextRequestProcessor().abortTransaction(abortTransaction);
    }
    
    @Override
    public void sealSegment(WireCommands.SealSegment sealSegment) {
        getNextRequestProcessor().sealSegment(sealSegment);
    }

    @Override
    public void deleteSegment(WireCommands.DeleteSegment deleteSegment) {
        getNextRequestProcessor().deleteSegment(deleteSegment);
    }

    @Override
    public void keepAlive(WireCommands.KeepAlive keepAlive) {
        getNextRequestProcessor().keepAlive(keepAlive);
    }

}
