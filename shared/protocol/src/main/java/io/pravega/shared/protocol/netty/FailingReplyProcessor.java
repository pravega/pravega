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

import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Hello;
import io.pravega.shared.protocol.netty.WireCommands.InvalidEventNumber;
import io.pravega.shared.protocol.netty.WireCommands.KeepAlive;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.TableSegmentNotEmpty;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentDeleted;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentPolicyUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.shared.protocol.netty.WireCommands.SegmentSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentTruncated;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.SegmentsMerged;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import lombok.extern.slf4j.Slf4j;


/**
 * A ReplyProcessor that throws on every method. (Useful to subclass)
 */
@Slf4j
public abstract class FailingReplyProcessor implements ReplyProcessor {

    @Override
    public void hello(Hello hello) {
        if (hello.getLowVersion() > WireCommands.WIRE_VERSION || hello.getHighVersion() < WireCommands.OLDEST_COMPATIBLE_VERSION) {
            log.error("Incompatible wire protocol versions {}", hello);
        } else {
            log.info("Received hello: {}", hello);
        }
    }

    @Override
    public void operationUnsupported(OperationUnsupported operationUnsupported) {
        throw new UnsupportedOperationException("Operation '" + operationUnsupported.getOperationName() +
                "' is not supported on the target SegmentStore.");
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
    public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {
        throw new IllegalStateException("Segment is truncated: " + segmentIsTruncated.segment
                + " at offset " + segmentIsTruncated.startOffset);
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
    public void invalidEventNumber(InvalidEventNumber invalidEventNumber) {
        throw new IllegalStateException("Invalid event number: " + invalidEventNumber);
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
    public void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated) {
        throw new IllegalStateException("Unexpected operation: " + segmentAttributeUpdated);
    }

    @Override
    public void storageFlushed(WireCommands.StorageFlushed storageFlushed) {
        throw new IllegalStateException("Unexpected operation: " + storageFlushed);
    }

    @Override
    public void storageChunksListed(WireCommands.StorageChunksListed storageChunksListed) {
        throw new IllegalStateException("Unexpected operation: " + storageChunksListed);
    }

    @Override
    public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {
        throw new IllegalStateException("Unexpected operation: " + segmentAttribute);
    }

    @Override
    public void streamSegmentInfo(StreamSegmentInfo streamInfo) {
        throw new IllegalStateException("Unexpected operation: " + streamInfo);
    }

    @Override
    public void segmentCreated(SegmentCreated streamsSegmentCreated) {
        throw new IllegalStateException("Unexpected operation: " + streamsSegmentCreated);
    }

    @Override
    public void segmentsMerged(SegmentsMerged segmentsMerged) {
        throw new IllegalStateException("Unexpected operation: " + segmentsMerged);
    }

    @Override
    public void segmentsBatchMerged(WireCommands.SegmentsBatchMerged segmentsMerged) {
        throw new IllegalStateException("Unexpected operation: " + segmentsMerged);
    }

    @Override
    public void segmentSealed(SegmentSealed segmentSealed) {
        throw new IllegalStateException("Unexpected operation: " + segmentSealed);
    }

    @Override
    public void segmentTruncated(SegmentTruncated segmentTruncated) {
        throw new IllegalStateException("Unexpected operation: " + segmentTruncated);
    }

    @Override
    public void segmentDeleted(SegmentDeleted segmentDeleted) {
        throw new IllegalStateException("Unexpected operation: " + segmentDeleted);
    }

    @Override
    public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authFailed) {
        throw new IllegalStateException("Unexpected operation: " + authFailed);
    }

    @Override
    public void tableSegmentInfo(WireCommands.TableSegmentInfo info) {
        throw new IllegalStateException("Unexpected operation: " + info);
    }

    @Override
    public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {
        throw new IllegalStateException("Unexpected operation: " + tableEntriesUpdated);
    }

    @Override
    public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {
        throw new IllegalStateException("Unexpected operation: " + tableKeysRemoved);
    }

    @Override
    public void tableRead(WireCommands.TableRead tableRead) {
        throw new IllegalStateException("Unexpected operation: " + tableRead);
    }

    @Override
    public void tableSegmentNotEmpty(TableSegmentNotEmpty tableSegmentNotEmpty) {
        throw new IllegalStateException("Unexpected operation: " + tableSegmentNotEmpty);
    }

    @Override
    public void segmentPolicyUpdated(SegmentPolicyUpdated segment) {
        throw new IllegalStateException("Unexpected operation: " + segment);
    }

    @Override
    public void keepAlive(KeepAlive keepAlive) {
        log.trace("KeepAlive received");
    }

    @Override
    public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {
        throw new IllegalStateException("Unexpected operation: " + tableKeyDoesNotExist);
    }

    @Override
    public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {
        throw new IllegalStateException("Unexpected operation: " + tableKeyBadVersion);
    }

    @Override
    public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {
        throw new IllegalStateException("Unexpected operation: " + tableKeysRead);
    }

    @Override
    public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {
        throw new IllegalStateException("Unexpected operation: " + tableEntriesRead);
    }

    @Override
    public void tableEntriesDeltaRead(WireCommands.TableEntriesDeltaRead tableEntriesDeltaRead) {
        throw new IllegalStateException("Unexpected operation: " + tableEntriesDeltaRead);
    }
    
    @Override
    public void offsetLocated(WireCommands.OffsetLocated offsetLocated) {
        throw new IllegalStateException("Unexpected operation: " + offsetLocated);
    }

    @Override
    public void errorMessage(WireCommands.ErrorMessage errorMessage) {
        throw new IllegalStateException("Unexpected operation: " + errorMessage);
    }
}
