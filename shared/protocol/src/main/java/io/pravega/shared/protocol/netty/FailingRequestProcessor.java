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
import lombok.extern.slf4j.Slf4j;

/**
 * A RequestProcessor that throws on every method. (Useful to subclass)
 */
@Slf4j
public class FailingRequestProcessor implements RequestProcessor {
    
    @Override
    public void hello(Hello hello) {
        throw new IllegalStateException("Unexpected operation");
    }

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
    public void updateSegmentPolicy(UpdateSegmentPolicy updateSegmentPolicy) {
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
    public void abortTransaction(AbortTransaction abortTransaction) {
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
