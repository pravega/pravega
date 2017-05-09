/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

/**
 * A ReplyProcessor that hands off all implementation to another ReplyProcessor.
 * This is useful for creating subclasses that only handle a subset of Commands.
 */
public abstract class DelegatingReplyProcessor implements ReplyProcessor {

    public abstract ReplyProcessor getNextReplyProcessor();

    @Override
    public void wrongHost(WireCommands.WrongHost wrongHost) {
        getNextReplyProcessor().wrongHost(wrongHost);
    }

    @Override
    public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {
        getNextReplyProcessor().segmentIsSealed(segmentIsSealed);
    }

    @Override
    public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
        getNextReplyProcessor().segmentAlreadyExists(segmentAlreadyExists);
    }

    @Override
    public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
        getNextReplyProcessor().noSuchSegment(noSuchSegment);
    }

    @Override
    public void noSuchBatch(WireCommands.NoSuchTransaction noSuchBatch) {
        getNextReplyProcessor().noSuchBatch(noSuchBatch);
    }

    @Override
    public void appendSetup(WireCommands.AppendSetup appendSetup) {
        getNextReplyProcessor().appendSetup(appendSetup);
    }

    @Override
    public void dataAppended(WireCommands.DataAppended dataAppended) {
        getNextReplyProcessor().dataAppended(dataAppended);
    }
    
    @Override
    public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {
        getNextReplyProcessor().conditionalCheckFailed(dataNotAppended);
    }

    @Override
    public void segmentRead(WireCommands.SegmentRead data) {
        getNextReplyProcessor().segmentRead(data);
    }

    @Override
    public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {
        getNextReplyProcessor().streamSegmentInfo(streamInfo);
    }
    
    @Override
    public void transactionInfo(WireCommands.TransactionInfo transactionInfo) {
        getNextReplyProcessor().transactionInfo(transactionInfo);
    }

    @Override
    public void segmentCreated(WireCommands.SegmentCreated streamsSegmentCreated) {
        getNextReplyProcessor().segmentCreated(streamsSegmentCreated);
    }

    @Override
    public void transactionCreated(WireCommands.TransactionCreated transactionCreated) {
        getNextReplyProcessor().transactionCreated(transactionCreated);
    }

    @Override
    public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {
        getNextReplyProcessor().transactionCommitted(transactionCommitted);
    }
    
    @Override
    public void transactionAborted(WireCommands.TransactionAborted transactionAborted) {
        getNextReplyProcessor().transactionAborted(transactionAborted);
    }

    @Override
    public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {
        getNextReplyProcessor().segmentSealed(segmentSealed);
    }

    @Override
    public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
        getNextReplyProcessor().segmentDeleted(segmentDeleted);
    }

    @Override
    public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segment) {
        getNextReplyProcessor().segmentPolicyUpdated(segment);
    }

    @Override
    public void keepAlive(WireCommands.KeepAlive keepAlive) {
        getNextReplyProcessor().keepAlive(keepAlive);
    }

}
