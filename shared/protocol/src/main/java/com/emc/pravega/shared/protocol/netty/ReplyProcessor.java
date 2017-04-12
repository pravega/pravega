/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.shared.protocol.netty;

import com.emc.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import com.emc.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import com.emc.pravega.shared.protocol.netty.WireCommands.DataAppended;
import com.emc.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import com.emc.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.shared.protocol.netty.WireCommands.NoSuchTransaction;
import com.emc.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import com.emc.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import com.emc.pravega.shared.protocol.netty.WireCommands.SegmentDeleted;
import com.emc.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import com.emc.pravega.shared.protocol.netty.WireCommands.SegmentPolicyUpdated;
import com.emc.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import com.emc.pravega.shared.protocol.netty.WireCommands.SegmentSealed;
import com.emc.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import com.emc.pravega.shared.protocol.netty.WireCommands.TransactionCommitted;
import com.emc.pravega.shared.protocol.netty.WireCommands.TransactionCreated;
import com.emc.pravega.shared.protocol.netty.WireCommands.TransactionAborted;
import com.emc.pravega.shared.protocol.netty.WireCommands.TransactionInfo;
import com.emc.pravega.shared.protocol.netty.WireCommands.WrongHost;

/**
 * A class that handles each type of reply. (Visitor pattern)
 */
public interface ReplyProcessor {
    void wrongHost(WrongHost wrongHost);

    void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists);

    void segmentIsSealed(SegmentIsSealed segmentIsSealed);

    void noSuchSegment(NoSuchSegment noSuchSegment);

    void noSuchBatch(NoSuchTransaction noSuchBatch);

    void appendSetup(AppendSetup appendSetup);

    void dataAppended(DataAppended dataAppended);
    
    void conditionalCheckFailed(ConditionalCheckFailed dataNotAppended);

    void segmentRead(SegmentRead segmentRead);

    void streamSegmentInfo(StreamSegmentInfo streamInfo);
    
    void transactionInfo(TransactionInfo transactionInfo);

    void segmentCreated(SegmentCreated segmentCreated);

    void transactionCreated(TransactionCreated transactionCreated);

    void transactionCommitted(TransactionCommitted transactionCommitted);
    
    void transactionAborted(TransactionAborted transactionAborted);

    void segmentSealed(SegmentSealed segmentSealed);

    void segmentDeleted(SegmentDeleted segmentDeleted);

    void keepAlive(KeepAlive keepAlive);
    
    void connectionDropped();

    void segmentPolicyUpdated(SegmentPolicyUpdated segmentPolicyUpdated);
    
    void processingFailure(Exception error);
}
