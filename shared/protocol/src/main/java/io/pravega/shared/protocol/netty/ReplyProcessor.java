/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.protocol.netty;

/**
 * A class that handles each type of reply. (Visitor pattern)
 */
public interface ReplyProcessor {
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
