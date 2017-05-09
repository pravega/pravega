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

import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchTransaction;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentDeleted;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentPolicyUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.shared.protocol.netty.WireCommands.SegmentSealed;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.TransactionAborted;
import io.pravega.shared.protocol.netty.WireCommands.TransactionCommitted;
import io.pravega.shared.protocol.netty.WireCommands.TransactionCreated;
import io.pravega.shared.protocol.netty.WireCommands.TransactionInfo;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import lombok.extern.slf4j.Slf4j;


/**
 * A ReplyProcessor that throws on every method. (Useful to subclass)
 */
@Slf4j
public abstract class FailingReplyProcessor implements ReplyProcessor {

    @Override
    public void hello(Hello hello) {
        log.info("Received hello: {}", hello);
    }
    
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
