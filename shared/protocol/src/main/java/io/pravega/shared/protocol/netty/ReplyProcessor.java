/**
 * Copyright Pravega Authors.
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

    void tableSegmentNotEmpty(WireCommands.TableSegmentNotEmpty tableSegmentNotEmpty);

    void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber);

    void appendSetup(WireCommands.AppendSetup appendSetup);

    void dataAppended(WireCommands.DataAppended dataAppended);
    
    void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended);

    void storageFlushed(WireCommands.StorageFlushed storageFlushed);

    void storageChunksListed(WireCommands.StorageChunksListed storageChunksListed);

    void segmentRead(WireCommands.SegmentRead segmentRead);
    
    void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated);
    
    void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute);
    
    void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo);
    
    void segmentCreated(WireCommands.SegmentCreated segmentCreated);

    void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged);
    
    void segmentsBatchMerged(WireCommands.SegmentsBatchMerged segmentsMerged);

    void segmentSealed(WireCommands.SegmentSealed segmentSealed);

    void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated);

    void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted);

    void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported);

    void keepAlive(WireCommands.KeepAlive keepAlive);
    
    void connectionDropped();

    void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated);
    
    void processingFailure(Exception error);

    void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed);

    void tableSegmentInfo(WireCommands.TableSegmentInfo info);

    void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated);

    void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved);

    void tableRead(WireCommands.TableRead tableRead);

    void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist);

    void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion);

    void tableKeysRead(WireCommands.TableKeysRead tableKeysRead);

    void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead);

    void tableEntriesDeltaRead(WireCommands.TableEntriesDeltaRead tableEntriesDeltaRead);
    
    void offsetLocated(WireCommands.OffsetLocated offsetLocated);

    void errorMessage(WireCommands.ErrorMessage errorMessage);
}
