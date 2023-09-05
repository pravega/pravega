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
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.ErrorMessage;
import io.pravega.shared.protocol.netty.WireCommands.InvalidEventNumber;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAlreadyExists;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttribute;
import io.pravega.shared.protocol.netty.WireCommands.StorageFlushed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentAttributeUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentDeleted;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentIsTruncated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentPolicyUpdated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.shared.protocol.netty.WireCommands.SegmentSealed;
import io.pravega.shared.protocol.netty.WireCommands.SegmentTruncated;
import io.pravega.shared.protocol.netty.WireCommands.SegmentsMerged;
import io.pravega.shared.protocol.netty.WireCommands.StreamSegmentInfo;
import io.pravega.shared.protocol.netty.WireCommands.TableEntriesDeltaRead;
import io.pravega.shared.protocol.netty.WireCommands.TableEntriesRead;
import io.pravega.shared.protocol.netty.WireCommands.TableEntriesUpdated;
import io.pravega.shared.protocol.netty.WireCommands.TableKeyBadVersion;
import io.pravega.shared.protocol.netty.WireCommands.TableKeyDoesNotExist;
import io.pravega.shared.protocol.netty.WireCommands.TableKeysRead;
import io.pravega.shared.protocol.netty.WireCommands.TableKeysRemoved;
import io.pravega.shared.protocol.netty.WireCommands.TableRead;
import io.pravega.shared.protocol.netty.WireCommands.TableSegmentNotEmpty;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import org.junit.Test;

import java.util.ArrayList;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public class FailingReplyProcessorTest {

    private ReplyProcessor rp = new FailingReplyProcessor() {
        @Override
        public void connectionDropped() {
        }

        @Override
        public void processingFailure(Exception error) {
        }
    };

    @Test
    public void testEverythingThrows() {
        assertThrows(IllegalStateException.class, () -> rp.appendSetup(new AppendSetup(0, "", null, 1)));
        assertThrows(IllegalStateException.class, () -> rp.authTokenCheckFailed(new AuthTokenCheckFailed(0, "")));
        assertThrows(IllegalStateException.class, () -> rp.conditionalCheckFailed(new ConditionalCheckFailed(null, 1, 2)));
        assertThrows(IllegalStateException.class, () -> rp.dataAppended(new DataAppended(1, null, 0, -1, 2)));
        assertThrows(IllegalStateException.class, () -> rp.invalidEventNumber(new InvalidEventNumber(null, 0, "", 0)));
        assertThrows(IllegalStateException.class, () -> rp.noSuchSegment(new NoSuchSegment(0, "", "", 2)));
        assertThrows(UnsupportedOperationException.class, () -> rp.operationUnsupported(new OperationUnsupported(0, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.segmentAlreadyExists(new SegmentAlreadyExists(1, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.segmentAttribute(new SegmentAttribute(1, 3)));
        assertThrows(IllegalStateException.class, () -> rp.segmentAttributeUpdated(new SegmentAttributeUpdated(1, false)));
        assertThrows(IllegalStateException.class, () -> rp.segmentCreated(new SegmentCreated(1, "")));
        assertThrows(IllegalStateException.class, () -> rp.segmentDeleted(new SegmentDeleted(0, "")));
        assertThrows(IllegalStateException.class, () -> rp.segmentIsSealed(new SegmentIsSealed(1, "", "", 1)));
        assertThrows(IllegalStateException.class, () -> rp.segmentIsTruncated(new SegmentIsTruncated(0, ":", 1, "", 2)));
        assertThrows(IllegalStateException.class, () -> rp.segmentPolicyUpdated(new SegmentPolicyUpdated(0, "")));
        assertThrows(IllegalStateException.class, () -> rp.segmentRead(new SegmentRead("", 1, true, false, null, 0)));
        assertThrows(IllegalStateException.class, () -> rp.segmentSealed(new SegmentSealed(0, "")));
        assertThrows(IllegalStateException.class, () -> rp.segmentsMerged(new SegmentsMerged(0, "", "", 2)));
        assertThrows(IllegalStateException.class, () -> rp.segmentTruncated(new SegmentTruncated(0, "")));
        assertThrows(IllegalStateException.class, () -> rp.streamSegmentInfo(new StreamSegmentInfo(0, "", false, false, false, 0, 0, 0)));
        assertThrows(IllegalStateException.class, () -> rp.tableSegmentInfo(new WireCommands.TableSegmentInfo(0, "", 0, 0, 0, 0)));
        assertThrows(IllegalStateException.class, () -> rp.tableEntriesDeltaRead(new TableEntriesDeltaRead(0, "", null, false, true, 0)));
        assertThrows(IllegalStateException.class, () -> rp.tableEntriesRead(new TableEntriesRead(0, "", null, null)));
        assertThrows(IllegalStateException.class, () -> rp.tableEntriesUpdated(new TableEntriesUpdated(0, null)));
        assertThrows(IllegalStateException.class, () -> rp.tableKeyBadVersion(new TableKeyBadVersion(0, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.tableKeyDoesNotExist(new TableKeyDoesNotExist(0, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.tableKeysRead(new TableKeysRead(0, "", null, null)));
        assertThrows(IllegalStateException.class, () -> rp.tableKeysRemoved(new TableKeysRemoved(0, "")));
        assertThrows(IllegalStateException.class, () -> rp.tableRead(new TableRead(0, "", null)));
        assertThrows(IllegalStateException.class, () -> rp.tableSegmentNotEmpty(new TableSegmentNotEmpty(0, "", "")));
        assertThrows(IllegalStateException.class, () -> rp.wrongHost(new WrongHost(0, "", "", "")));
        assertThrows(IllegalStateException.class, () -> rp.errorMessage(new ErrorMessage(0, "", "", ErrorMessage.ErrorCode.UNSPECIFIED)));
        assertThrows(IllegalStateException.class, () -> rp.storageFlushed(new StorageFlushed(0)));
        assertThrows(IllegalStateException.class, () -> rp.storageChunksListed(new WireCommands.StorageChunksListed(0, new ArrayList<>())));
        assertThrows(IllegalStateException.class, () -> rp.offsetLocated(new WireCommands.OffsetLocated(0, "", 0)));
    }

}
