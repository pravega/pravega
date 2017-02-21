/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.netty;

import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.ConditionalCheckFailed;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.NoSuchTransaction;
import com.emc.pravega.common.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.SegmentDeleted;
import com.emc.pravega.common.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.netty.WireCommands.SegmentSealed;
import com.emc.pravega.common.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.common.netty.WireCommands.TransactionCommitted;
import com.emc.pravega.common.netty.WireCommands.TransactionCreated;
import com.emc.pravega.common.netty.WireCommands.TransactionAborted;
import com.emc.pravega.common.netty.WireCommands.TransactionInfo;
import com.emc.pravega.common.netty.WireCommands.WrongHost;

/**
 * A ReplyProcessor that hands off all implementation to another ReplyProcessor.
 * This is useful for creating subclasses that only handle a subset of Commands.
 */
public abstract class DelegatingReplyProcessor implements ReplyProcessor {

    public abstract ReplyProcessor getNextReplyProcessor();

    @Override
    public void wrongHost(WrongHost wrongHost) {
        getNextReplyProcessor().wrongHost(wrongHost);
    }

    @Override
    public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
        getNextReplyProcessor().segmentIsSealed(segmentIsSealed);
    }

    @Override
    public void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists) {
        getNextReplyProcessor().segmentAlreadyExists(segmentAlreadyExists);
    }

    @Override
    public void noSuchSegment(NoSuchSegment noSuchSegment) {
        getNextReplyProcessor().noSuchSegment(noSuchSegment);
    }

    @Override
    public void noSuchBatch(NoSuchTransaction noSuchBatch) {
        getNextReplyProcessor().noSuchBatch(noSuchBatch);
    }

    @Override
    public void appendSetup(AppendSetup appendSetup) {
        getNextReplyProcessor().appendSetup(appendSetup);
    }

    @Override
    public void dataAppended(DataAppended dataAppended) {
        getNextReplyProcessor().dataAppended(dataAppended);
    }
    
    @Override
    public void conditionalCheckFailed(ConditionalCheckFailed dataNotAppended) {
        getNextReplyProcessor().conditionalCheckFailed(dataNotAppended);
    }

    @Override
    public void segmentRead(SegmentRead data) {
        getNextReplyProcessor().segmentRead(data);
    }

    @Override
    public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
        getNextReplyProcessor().streamSegmentInfo(streamInfo);
    }
    
    @Override
    public void transactionInfo(TransactionInfo transactionInfo) {
        getNextReplyProcessor().transactionInfo(transactionInfo);
    }

    @Override
    public void segmentCreated(SegmentCreated streamsSegmentCreated) {
        getNextReplyProcessor().segmentCreated(streamsSegmentCreated);
    }

    @Override
    public void transactionCreated(TransactionCreated transactionCreated) {
        getNextReplyProcessor().transactionCreated(transactionCreated);
    }

    @Override
    public void transactionCommitted(TransactionCommitted transactionCommitted) {
        getNextReplyProcessor().transactionCommitted(transactionCommitted);
    }
    
    @Override
    public void transactionAborted(TransactionAborted transactionAborted) {
        getNextReplyProcessor().transactionAborted(transactionAborted);
    }

    @Override
    public void segmentSealed(SegmentSealed segmentSealed) {
        getNextReplyProcessor().segmentSealed(segmentSealed);
    }

    @Override
    public void segmentDeleted(SegmentDeleted segmentDeleted) {
        getNextReplyProcessor().segmentDeleted(segmentDeleted);
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        getNextReplyProcessor().keepAlive(keepAlive);
    }

}
