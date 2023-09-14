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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class SystemJournalRecordsTests {

    @Test
    public void testChunkAddedRecordSerialization() throws Exception {
        testSystemJournalRecordSerialization(SystemJournal.ChunkAddedRecord.builder()
                .segmentName("segmentName")
                .newChunkName("newChunkName")
                .oldChunkName("oldChunkName")
                .offset(1)
                .build());

        // With nullable values
        testSystemJournalRecordSerialization(SystemJournal.ChunkAddedRecord.builder()
                .segmentName("segmentName")
                .newChunkName("newChunkName")
                .oldChunkName(null)
                .offset(1)
                .build());
    }

    @Test
    public void testTruncationRecordSerialization() throws Exception {
        testSystemJournalRecordSerialization(SystemJournal.TruncationRecord.builder()
                .segmentName("segmentName")
                .offset(1)
                .firstChunkName("firstChunkName")
                .startOffset(2)
                .build());
    }

    private void testSystemJournalRecordSerialization(SystemJournal.SystemJournalRecord original) throws Exception {
        val serializer = new SystemJournal.SystemJournalRecord.SystemJournalRecordSerializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    @Test
    public void testSystemJournalRecordBatchSerialization() throws Exception {
        ArrayList<SystemJournal.SystemJournalRecord> lst = new ArrayList<SystemJournal.SystemJournalRecord>();
        testSystemJournalRecordBatchSerialization(
                SystemJournal.SystemJournalRecordBatch.builder()
                        .systemJournalRecords(lst)
                        .build());

        ArrayList<SystemJournal.SystemJournalRecord> lst2 = new ArrayList<SystemJournal.SystemJournalRecord>();
        lst2.add(SystemJournal.ChunkAddedRecord.builder()
                .segmentName("segmentName")
                .newChunkName("newChunkName")
                .oldChunkName("oldChunkName")
                .offset(1)
                .build());
        lst2.add(SystemJournal.ChunkAddedRecord.builder()
                .segmentName("segmentName")
                .newChunkName("newChunkName")
                .oldChunkName(null)
                .offset(1)
                .build());
        lst2.add(SystemJournal.TruncationRecord.builder()
                .segmentName("segmentName")
                .offset(1)
                .firstChunkName("firstChunkName")
                .startOffset(2)
                .build());
        testSystemJournalRecordBatchSerialization(
                SystemJournal.SystemJournalRecordBatch.builder()
                        .systemJournalRecords(lst)
                        .build());
    }

    private void testSystemJournalRecordBatchSerialization(SystemJournal.SystemJournalRecordBatch original) throws Exception {
        val serializer = new SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    @Test
    public void testSnapshotRecordSerialization() throws Exception {

        ArrayList<ChunkMetadata> list = new ArrayList<>();
        list.add(ChunkMetadata.builder()
                .name("name")
                .nextChunk("nextChunk")
                .length(1)
                .status(2)
                .build());
        list.add(ChunkMetadata.builder()
                .name("name")
                .length(1)
                .status(2)
                .build());

        testSegmentSnapshotRecordSerialization(
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(SegmentMetadata.builder()
                                .name("name")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk("firstChunk")
                                .lastChunk("lastChunk")
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .chunkMetadataCollection(list)
                        .build());

        testSegmentSnapshotRecordSerialization(
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(SegmentMetadata.builder()
                                .name("name")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk(null)
                                .lastChunk(null)
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .chunkMetadataCollection(list)
                        .build());
    }

    private void testSegmentSnapshotRecordSerialization(SystemJournal.SegmentSnapshotRecord original) throws Exception {
        val serializer = new SystemJournal.SegmentSnapshotRecord.Serializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    @Test
    public void testSystemSnapshotRecordSerialization() throws Exception {

        ArrayList<ChunkMetadata> list1 = new ArrayList<>();
        list1.add(ChunkMetadata.builder()
                .name("name1")
                .nextChunk("nextChunk1")
                .length(1)
                .status(2)
                .build());
        list1.add(ChunkMetadata.builder()
                .name("name12")
                .length(1)
                .status(2)
                .build());

        ArrayList<ChunkMetadata> list2 = new ArrayList<>();
        list2.add(ChunkMetadata.builder()
                .name("name2")
                .nextChunk("nextChunk2")
                .length(1)
                .status(3)
                .build());
        list2.add(ChunkMetadata.builder()
                .name("name22")
                .length(1)
                .status(3)
                .build());

        ArrayList<SystemJournal.SegmentSnapshotRecord> segmentlist = new ArrayList<>();

        segmentlist.add(
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(SegmentMetadata.builder()
                                .name("name1")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk("firstChunk111")
                                .lastChunk("lastChun111k")
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .chunkMetadataCollection(list1)
                        .build());

        segmentlist.add(
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(SegmentMetadata.builder()
                                .name("name2")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk(null)
                                .lastChunk(null)
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .chunkMetadataCollection(list2)
                        .build());
        val systemSnapshot = SystemJournal.SystemSnapshotRecord.builder()
                .epoch(42)
                .fileIndex(7)
                .segmentSnapshotRecords(segmentlist)
                .build();
        testSystemSnapshotRecordSerialization(systemSnapshot);
    }

    private void testSystemSnapshotRecordSerialization(SystemJournal.SystemSnapshotRecord original) throws Exception {
        val serializer = new SystemJournal.SystemSnapshotRecord.Serializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    @Test
    public void testValid() {
        val valid = SystemJournal.SegmentSnapshotRecord.builder()
                .segmentMetadata(
                        SegmentMetadata.builder()
                                .name("test")
                                .chunkCount(4)
                                .firstChunk("A")
                                .firstChunkStartOffset(1)
                                .startOffset(2)
                                .lastChunk("D")
                                .lastChunkStartOffset(10)
                                .length(15)
                                .build()
                                .setActive(true)
                                .setStorageSystemSegment(true)
                )
                .chunkMetadataCollection(Arrays.asList(
                        ChunkMetadata.builder()
                                .name("A")
                                .length(2)
                                .nextChunk("B")
                                .build(),
                        ChunkMetadata.builder()
                                .name("B")
                                .length(3)
                                .nextChunk("C")
                                .build(),
                        ChunkMetadata.builder()
                                .name("C")
                                .length(4)
                                .nextChunk("D")
                                .build(),
                        ChunkMetadata.builder()
                                .name("D")
                                .length(5)
                                .nextChunk(null)
                                .build()
                ))
                .build();
        valid.checkInvariants();
    }

    @Test
    public void testInvalidRecords() {
        // Create mal formed data
        SystemJournal.SegmentSnapshotRecord[] invalidDataList = new SystemJournal.SegmentSnapshotRecord[] {
                // Not system segment
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(
                                SegmentMetadata.builder()
                                        .name("test")
                                        .build()
                                        .setActive(true)
                        )
                        .chunkMetadataCollection(Arrays.asList())
                        .build(),
                // Incorrect chunk count
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(
                                SegmentMetadata.builder()
                                        .name("test")
                                        .firstChunk("A")
                                        .lastChunk("A")
                                        .chunkCount(1)
                                        .build()
                                        .setActive(true)
                                        .setStorageSystemSegment(true)
                        )
                        .chunkMetadataCollection(Arrays.asList(
                                ChunkMetadata.builder()
                                        .name("A")
                                        .length(2)
                                        .nextChunk("B")
                                        .build(),
                                ChunkMetadata.builder()
                                        .name("B")
                                        .length(3)
                                        .nextChunk(null)
                                        .build()
                        ))
                        .build(),
                // Incorrect chunk count.
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(
                                SegmentMetadata.builder()
                                        .name("test")
                                        .firstChunk("A")
                                        .lastChunk("A")
                                        .chunkCount(1)
                                        .build()
                                        .setActive(true)
                                        .setStorageSystemSegment(true)
                        )
                        .chunkMetadataCollection(Arrays.asList(
                                ChunkMetadata.builder()
                                        .name("A")
                                        .length(2)
                                        .nextChunk(null)
                                        .build()
                        ))
                        .build(),
                // Incorrect chunks
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(
                                SegmentMetadata.builder()
                                        .name("test")
                                        .firstChunk("A")
                                        .lastChunk("A")
                                        .chunkCount(1)
                                        .length(2)
                                        .build()
                                        .setActive(true)
                                        .setStorageSystemSegment(true)
                        )
                        .chunkMetadataCollection(Arrays.asList(
                                ChunkMetadata.builder()
                                        .name("Z")
                                        .length(2)
                                        .nextChunk(null)
                                        .build()
                        ))
                        .build(),
                // Wrong last chunk pointer
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(
                                SegmentMetadata.builder()
                                        .name("test")
                                        .firstChunk("A")
                                        .lastChunk("A")
                                        .chunkCount(1)
                                        .length(2)
                                        .build()
                                        .setActive(true)
                                        .setStorageSystemSegment(true)
                        )
                        .chunkMetadataCollection(Arrays.asList(
                                ChunkMetadata.builder()
                                        .name("A")
                                        .length(2)
                                        .nextChunk("Z")
                                        .build()
                        ))
                        .build(),
                // Incorrect
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(
                                SegmentMetadata.builder()
                                        .name("test")
                                        .firstChunk("A")
                                        .lastChunk("B")
                                        .chunkCount(2)
                                        .length(10)
                                        .lastChunkStartOffset(6)
                                        .build()
                                        .setActive(true)
                                        .setStorageSystemSegment(true)
                        )
                        .chunkMetadataCollection(Arrays.asList(
                                ChunkMetadata.builder()
                                        .name("A")
                                        .length(5)
                                        .nextChunk("B")
                                        .build(),
                                ChunkMetadata.builder()
                                        .name("B")
                                        .length(5)
                                        .nextChunk(null)
                                        .build()
                        ))
                        .build()
        };

        for (val invalidData: invalidDataList) {
            AssertExtensions.assertThrows(invalidData.toString(),
                    () -> invalidData.checkInvariants(),
                    ex -> ex instanceof IllegalStateException);
        }
    }

    @Test
    public void testSystemSnapshotInfoSerialization() throws Exception {
        testSystemSnapshotInfoSerialization(SnapshotInfo.builder()
                .build());
        testSystemSnapshotInfoSerialization(SnapshotInfo.builder()
                .snapshotId(1)
                .build());
        testSystemSnapshotInfoSerialization(SnapshotInfo.builder()
                .epoch(2)
                .build());
        testSystemSnapshotInfoSerialization(SnapshotInfo.builder()
                .snapshotId(3)
                .epoch(4)
                .build());
    }

    private void testSystemSnapshotInfoSerialization(SnapshotInfo original) throws Exception {
        val serializer = new SnapshotInfo.Serializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }
}
