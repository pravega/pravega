/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

import io.pravega.shared.protocol.netty.WireCommands.Hello;

/**
 * A class that handles each type of reply. (Visitor pattern)
 */
public interface ReplyProcessor {
    void hello(Hello hello);
    
    void wrongHost(WireCommands.WrongHost wrongHost);

    void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists);

    void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed);

    void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment);

    void noSuchBatch(WireCommands.NoSuchTransaction noSuchBatch);

    void appendSetup(WireCommands.AppendSetup appendSetup);

    void dataAppended(WireCommands.DataAppended dataAppended);
    
    void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended);

    void segmentRead(WireCommands.SegmentRead segmentRead);

    void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo);
    
    void transactionInfo(WireCommands.TransactionInfo transactionInfo);

    void segmentCreated(WireCommands.SegmentCreated segmentCreated);

    void transactionCreated(WireCommands.TransactionCreated transactionCreated);

    void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted);
    
    void transactionAborted(WireCommands.TransactionAborted transactionAborted);

    void segmentSealed(WireCommands.SegmentSealed segmentSealed);

    void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted);

    void keepAlive(WireCommands.KeepAlive keepAlive);
    
    void connectionDropped();

    void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated);
    
    void processingFailure(Exception error);
}
