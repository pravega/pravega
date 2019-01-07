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
    
    default void process(Reply reply) {
        reply.process(this);
    }
    
    void hello(Hello hello);
    
    void wrongHost(WireCommands.WrongHost wrongHost);

    void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists);

    void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed);

    void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated);

    void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment);

    void notEmptyTableSegment(WireCommands.NotEmptyTableSegment notEmptyTableSegment);

    void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber);

    void appendSetup(WireCommands.AppendSetup appendSetup);

    void dataAppended(WireCommands.DataAppended dataAppended);
    
    void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended);

    void segmentRead(WireCommands.SegmentRead segmentRead);
    
    void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated);
    
    void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute);
    
    void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo);
    
    void segmentCreated(WireCommands.SegmentCreated segmentCreated);

    void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged);

    void segmentSealed(WireCommands.SegmentSealed segmentSealed);

    void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated);

    void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted);

    void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported);

    void keepAlive(WireCommands.KeepAlive keepAlive);
    
    void connectionDropped();

    void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated);
    
    void processingFailure(Exception error);

    void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed);

    void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated);

    void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved);

    void tableRead(WireCommands.TableRead tableRead);

    void tableKeyTooLong(WireCommands.TableKeyTooLong tableKeyTooLong);

    void tableValueTooLong(WireCommands.TableValueTooLong tableValueTooLong);

    void conditionalTableUpdateFailed(WireCommands.ConditionalTableUpdateFailed conditionalTableUpdateFailed);
}
