/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.netty;

import io.pravega.common.netty.WireCommands.AppendSetup;
import io.pravega.common.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.common.netty.WireCommands.DataAppended;
import io.pravega.common.netty.WireCommands.KeepAlive;
import io.pravega.common.netty.WireCommands.NoSuchSegment;
import io.pravega.common.netty.WireCommands.NoSuchTransaction;
import io.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.common.netty.WireCommands.SegmentCreated;
import io.pravega.common.netty.WireCommands.SegmentDeleted;
import io.pravega.common.netty.WireCommands.SegmentIsSealed;
import io.pravega.common.netty.WireCommands.SegmentRead;
import io.pravega.common.netty.WireCommands.SegmentSealed;
import io.pravega.common.netty.WireCommands.StreamSegmentInfo;
import io.pravega.common.netty.WireCommands.TransactionCommitted;
import io.pravega.common.netty.WireCommands.TransactionCreated;
import io.pravega.common.netty.WireCommands.TransactionAborted;
import io.pravega.common.netty.WireCommands.TransactionInfo;
import io.pravega.common.netty.WireCommands.WrongHost;
import io.pravega.common.netty.WireCommands.SegmentPolicyUpdated;

/**
 * A ReplyProcessor that throws on every method. (Useful to subclass)
 */
public abstract class FailingReplyProcessor implements ReplyProcessor {

    @Override
    public void wrongHost(WrongHost wrongHost) {
        throw new IllegalStateException("Wrong host. Segment: " + wrongHost.segment + " is on "
                + wrongHost.correctHost);
    }

    @Override
    public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
        throw new IllegalStateException("Segment is sealed: " + segmentIsSealed.segment);
    }

    @Override
    public void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists) {
        throw new IllegalStateException("Segment already exists: " + segmentAlreadyExists.segment);
    }

    @Override
    public void noSuchSegment(NoSuchSegment noSuchSegment) {
        throw new IllegalStateException("No such segment: " + noSuchSegment.segment);
    }

    @Override
    public void noSuchBatch(NoSuchTransaction noSuchTxn) {
        throw new IllegalStateException("No such Transaction: " + noSuchTxn.txn);
    }

    @Override
    public void appendSetup(AppendSetup appendSetup) {
        throw new IllegalStateException("Unexpected operation: " + appendSetup);
    }

    @Override
    public void dataAppended(DataAppended dataAppended) {
        throw new IllegalStateException("Unexpected operation: " + dataAppended);
    }
    
    @Override
    public void conditionalCheckFailed(ConditionalCheckFailed dataNotAppended) {
        throw new IllegalStateException("Conditional check failed for event: " + dataNotAppended.eventNumber);
    }

    @Override
    public void segmentRead(SegmentRead data) {
        throw new IllegalStateException("Unexpected operation: " + data);
    }

    @Override
    public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
        throw new IllegalStateException("Unexpected operation: " + streamInfo);
    }

    @Override
    public void transactionInfo(TransactionInfo transactionInfo) {
        throw new IllegalStateException("Unexpected operation: " + transactionInfo);
    }

    
    @Override
    public void segmentCreated(SegmentCreated streamsSegmentCreated) {
        throw new IllegalStateException("Unexpected operation: " + streamsSegmentCreated);
    }

    @Override
    public void transactionCreated(TransactionCreated transactionCreated) {
        throw new IllegalStateException("Unexpected operation: " + transactionCreated);
    }

    @Override
    public void transactionCommitted(TransactionCommitted transactionCommitted) {
        throw new IllegalStateException("Unexpected operation: " + transactionCommitted);
    }

    @Override
    public void transactionAborted(TransactionAborted transactionAborted) {
        throw new IllegalStateException("Unexpected operation: " + transactionAborted);
    }
    
    @Override
    public void segmentSealed(SegmentSealed segmentSealed) {
        throw new IllegalStateException("Unexpected operation: " + segmentSealed);
    }

    @Override
    public void segmentDeleted(SegmentDeleted segmentDeleted) {
        throw new IllegalStateException("Unexpected operation: " + segmentDeleted);
    }

    @Override
    public void segmentPolicyUpdated(SegmentPolicyUpdated segment) {
        throw new IllegalStateException("Unexpected operation: " + segment);
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        throw new IllegalStateException("Unexpected operation: " + keepAlive);
    }

}
