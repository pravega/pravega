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

import com.google.common.base.Preconditions;
import java.io.IOException;

/**
 * The various types of commands that can be sent over the wire.
 * Each has two fields the first is a code that identifies it in the wire protocol. (This is the first thing written)
 * The second is a constructor method, that is used to decode commands of that type.
 * 
 * (Types below that are grouped into pairs where there is a corresponding request and reply.)
 */
public enum WireCommandType {
    HELLO(-127, WireCommands.Hello::readFrom),
    
    PADDING(-1, WireCommands.Padding::readFrom),

    PARTIAL_EVENT(-2, WireCommands.PartialEvent::readFrom),

    FLUSH_TO_STORAGE(-3, WireCommands.FlushToStorage::readFrom),
    FLUSHED_TO_STORAGE(-4, WireCommands.StorageFlushed::readFrom),

    LIST_STORAGE_CHUNKS(-5, WireCommands.ListStorageChunks::readFrom),
    STORAGE_CHUNKS_LISTED(-6, WireCommands.StorageChunksListed::readFrom),

    EVENT(0, null), // Is read manually.

    SETUP_APPEND(1, WireCommands.SetupAppend::readFrom),
    APPEND_SETUP(2, WireCommands.AppendSetup::readFrom),

    APPEND_BLOCK(3, WireCommands.AppendBlock::readFrom),
    APPEND_BLOCK_END(4, WireCommands.AppendBlockEnd::readFrom),
    CONDITIONAL_APPEND(5, WireCommands.ConditionalAppend::readFrom),

    DATA_APPENDED(7, WireCommands.DataAppended::readFrom),
    CONDITIONAL_CHECK_FAILED(8, WireCommands.ConditionalCheckFailed::readFrom),

    READ_SEGMENT(9, WireCommands.ReadSegment::readFrom),
    SEGMENT_READ(10, WireCommands.SegmentRead::readFrom),

    GET_STREAM_SEGMENT_INFO(11, WireCommands.GetStreamSegmentInfo::readFrom),
    STREAM_SEGMENT_INFO(12, WireCommands.StreamSegmentInfo::readFrom),
    
    CREATE_SEGMENT(20, WireCommands.CreateSegment::readFrom),
    SEGMENT_CREATED(21, WireCommands.SegmentCreated::readFrom),

    SEAL_SEGMENT(28, WireCommands.SealSegment::readFrom),
    SEGMENT_SEALED(29, WireCommands.SegmentSealed::readFrom),

    DELETE_SEGMENT(30, WireCommands.DeleteSegment::readFrom),
    SEGMENT_DELETED(31, WireCommands.SegmentDeleted::readFrom),

    UPDATE_SEGMENT_POLICY(32, WireCommands.UpdateSegmentPolicy::readFrom),
    SEGMENT_POLICY_UPDATED(33, WireCommands.SegmentPolicyUpdated::readFrom),
    
    GET_SEGMENT_ATTRIBUTE(34, WireCommands.GetSegmentAttribute::readFrom),
    SEGMENT_ATTRIBUTE(35, WireCommands.SegmentAttribute::readFrom),
    
    UPDATE_SEGMENT_ATTRIBUTE(36, WireCommands.UpdateSegmentAttribute::readFrom),
    SEGMENT_ATTRIBUTE_UPDATED(37, WireCommands.SegmentAttributeUpdated::readFrom),

    TRUNCATE_SEGMENT(38, WireCommands.TruncateSegment::readFrom),
    SEGMENT_TRUNCATED(39, WireCommands.SegmentTruncated::readFrom),

    CREATE_TRANSIENT_SEGMENT(40, WireCommands.CreateTransientSegment::readFrom),
    
    LOCATE_OFFSET(41, WireCommands.LocateOffset::readFrom),
    OFFSET_LOCATED(42, WireCommands.OffsetLocated::readFrom),

    WRONG_HOST(50, WireCommands.WrongHost::readFrom),
    SEGMENT_IS_SEALED(51, WireCommands.SegmentIsSealed::readFrom),
    SEGMENT_ALREADY_EXISTS(52, WireCommands.SegmentAlreadyExists::readFrom),
    NO_SUCH_SEGMENT(53, WireCommands.NoSuchSegment::readFrom),
    INVALID_EVENT_NUMBER(55, WireCommands.InvalidEventNumber::readFrom),
    SEGMENT_IS_TRUNCATED(56, WireCommands.SegmentIsTruncated::readFrom),
    OPERATION_UNSUPPORTED(57, WireCommands.OperationUnsupported::readFrom),

    MERGE_SEGMENTS(58, WireCommands.MergeSegments::readFrom),
    SEGMENTS_MERGED(59, WireCommands.SegmentsMerged::readFrom),

    AUTH_TOKEN_CHECK_FAILED(60, WireCommands.AuthTokenCheckFailed::readFrom),
    ERROR_MESSAGE(61, WireCommands.ErrorMessage::readFrom),

    GET_TABLE_SEGMENT_INFO(68, WireCommands.GetTableSegmentInfo::readFrom),
    TABLE_SEGMENT_INFO(69, WireCommands.TableSegmentInfo::readFrom),
    CREATE_TABLE_SEGMENT(70, WireCommands.CreateTableSegment::readFrom),
    DELETE_TABLE_SEGMENT(71, WireCommands.DeleteTableSegment::readFrom),
    UPDATE_TABLE_ENTRIES(74, WireCommands.UpdateTableEntries::readFrom),
    TABLE_ENTRIES_UPDATED(75, WireCommands.TableEntriesUpdated::readFrom),

    REMOVE_TABLE_KEYS(76, WireCommands.RemoveTableKeys::readFrom),
    TABLE_KEYS_REMOVED(77, WireCommands.TableKeysRemoved::readFrom),

    READ_TABLE(78, WireCommands.ReadTable::readFrom),
    TABLE_READ(79, WireCommands.TableRead::readFrom),

    TABLE_SEGMENT_NOT_EMPTY(80, WireCommands.TableSegmentNotEmpty::readFrom),
    TABLE_KEY_DOES_NOT_EXIST(81, WireCommands.TableKeyDoesNotExist::readFrom),
    TABLE_KEY_BAD_VERSION(82, WireCommands.TableKeyBadVersion::readFrom),

    READ_TABLE_KEYS(83, WireCommands.ReadTableKeys::readFrom ),
    TABLE_KEYS_READ(84, WireCommands.TableKeysRead::readFrom),

    READ_TABLE_ENTRIES(85, WireCommands.ReadTableEntries::readFrom),
    TABLE_ENTRIES_READ(86, WireCommands.TableEntriesRead::readFrom),

    TABLE_ENTRIES_DELTA_READ(87, WireCommands.TableEntriesDeltaRead::readFrom),
    READ_TABLE_ENTRIES_DELTA(88, WireCommands.ReadTableEntriesDelta::readFrom),

    CONDITIONAL_BLOCK_END(89, WireCommands.ConditionalBlockEnd::readFrom),
    MERGE_SEGMENTS_BATCH(90, WireCommands.MergeSegmentsBatch::readFrom),
    SEGMENTS_BATCH_MERGED(91, WireCommands.SegmentsBatchMerged::readFrom),

    KEEP_ALIVE(100, WireCommands.KeepAlive::readFrom);

    private final int code;
    private final WireCommands.Constructor factory;

    WireCommandType(int code, WireCommands.Constructor factory) {
        Preconditions.checkArgument(code <= 127 && code >= -127, "All codes should fit in a byte.");
        this.code = code;
        this.factory = factory;
    }

    public int getCode() {
        return code;
    }

    public WireCommand readFrom(EnhancedByteBufInputStream in, int length) throws IOException {
        return factory.readFrom(in, length);
    }
}