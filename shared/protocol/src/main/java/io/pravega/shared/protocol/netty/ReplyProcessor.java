/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

    void noSuchTransaction(WireCommands.NoSuchTransaction noSuchTransaction);
    
    void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber);

    void appendSetup(WireCommands.AppendSetup appendSetup);

    void dataAppended(WireCommands.DataAppended dataAppended);
    
    void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended);

    void segmentRead(WireCommands.SegmentRead segmentRead);
    
    void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated);
    
    void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute);
    
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
