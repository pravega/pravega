/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

/**
 * A ReplyProcessor that throws on every method. (Useful to subclass)
 */
public abstract class FailingReplyProcessor implements ReplyProcessor {

    @Override
    public void wrongHost(WireCommands.WrongHost wrongHost) {
        throw new IllegalStateException("Wrong host. Segment: " + wrongHost.segment + " is on "
                + wrongHost.correctHost);
    }

    @Override
    public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
        throw new IllegalStateException("Segment is sealed: " + segmentIsSealed.segment);
    }

    @Override
    public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
        throw new IllegalStateException("Segment already exists: " + segmentAlreadyExists.segment);
    }

    @Override
    public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
        throw new IllegalStateException("No such segment: " + noSuchSegment.segment);
    }

    @Override
    public void noSuchBatch(WireCommands.NoSuchTransaction noSuchTxn) {
        throw new IllegalStateException("No such Transaction: " + noSuchTxn.txn);
    }

    @Override
    public void appendSetup(WireCommands.AppendSetup appendSetup) {
        throw new IllegalStateException("Unexpected operation: " + appendSetup);
    }

    @Override
    public void dataAppended(WireCommands.DataAppended dataAppended) {
        throw new IllegalStateException("Unexpected operation: " + dataAppended);
    }
    
    @Override
    public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {
        throw new IllegalStateException("Conditional check failed for event: " + dataNotAppended.eventNumber);
    }

    @Override
    public void segmentRead(WireCommands.SegmentRead data) {
        throw new IllegalStateException("Unexpected operation: " + data);
    }

    @Override
    public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {
        throw new IllegalStateException("Unexpected operation: " + streamInfo);
    }

    @Override
    public void transactionInfo(WireCommands.TransactionInfo transactionInfo) {
        throw new IllegalStateException("Unexpected operation: " + transactionInfo);
    }

    
    @Override
    public void segmentCreated(WireCommands.SegmentCreated streamsSegmentCreated) {
        throw new IllegalStateException("Unexpected operation: " + streamsSegmentCreated);
    }

    @Override
    public void transactionCreated(WireCommands.TransactionCreated transactionCreated) {
        throw new IllegalStateException("Unexpected operation: " + transactionCreated);
    }

    @Override
    public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
        throw new IllegalStateException("Unexpected operation: " + transactionCommitted);
    }

    @Override
    public void transactionAborted(WireCommands.TransactionAborted transactionAborted) {
        throw new IllegalStateException("Unexpected operation: " + transactionAborted);
    }
    
    @Override
    public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
        throw new IllegalStateException("Unexpected operation: " + segmentSealed);
    }

    @Override
    public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
        throw new IllegalStateException("Unexpected operation: " + segmentDeleted);
    }

    @Override
    public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segment) {
        throw new IllegalStateException("Unexpected operation: " + segment);
    }

    @Override
    public void keepAlive(WireCommands.KeepAlive keepAlive) {
        throw new IllegalStateException("Unexpected operation: " + keepAlive);
    }

}
