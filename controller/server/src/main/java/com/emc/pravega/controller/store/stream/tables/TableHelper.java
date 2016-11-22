/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.SegmentNotFoundException;

import java.io.ByteArrayOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper class for operations pertaining to segment store tables (segment, history, index).
 * All the processing is done locally and this class does not make any network calls.
 * All methods are synchronous and blocking.
 */
public class TableHelper {
    /**
     * Segment Table records are of fixed size.
     * So O(constant) operation to get segment given segmentTable Chunk.
     * <p>
     * Note: this method assumes you have supplied the correct chunk
     *
     * @param number       segment number
     * @param segmentTable segment table
     * @return
     */
    public static Segment getSegment(final int number, final byte[] segmentTable) {

        Optional<SegmentRecord> recordOpt = SegmentRecord.readRecord(segmentTable, number);
        if (recordOpt.isPresent()) {
            SegmentRecord record = recordOpt.get();
            return new Segment(record.getSegmentNumber(),
                    record.getStartTime(),
                    record.getRoutingKeyStart(),
                    record.getRoutingKeyEnd());
        } else {
            throw new SegmentNotFoundException(number);
        }
    }

    /**
     * Helper method to determine segmentChunk information from segment number.
     *
     * @param segmentNumber segment number
     * @return
     */
    public static int getSegmentChunkNumber(final int segmentNumber) {
        return segmentNumber / SegmentRecord.SEGMENT_CHUNK_SIZE;
    }

    /**
     * Helper method to get next higher number than highest segment number.
     *
     * @param chunkNumber chunk number
     * @param chunkData   chunk data
     * @return
     */
    public static int getNextSegmentNumber(final int chunkNumber, final byte[] chunkData) {
        return chunkNumber * SegmentRecord.SEGMENT_CHUNK_SIZE +
                (chunkData.length / SegmentRecord.SEGMENT_RECORD_SIZE);
    }

    /**
     * Current active segments correspond to last entry in the history table.
     * Until segment number is written to the history table it is not exposed to outside world
     * (e.g. callers - producers and consumers)
     *
     * @param historyTable history table
     * @return
     */
    public static List<Integer> getActiveSegments(final byte[] historyTable) {
        final Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyTable);

        return record.isPresent() ? record.get().getSegments() : new ArrayList<>();
    }

    /**
     * Get active segments at given timestamp.
     * Perform binary search on index table to find the record corresponding to timestamp.
     * Note: index table may be stale or not reflect lastest state of history table.
     * So we may need to fall through in the history table from the record being pointed to by index
     * until we find the correct record.
     *
     * @param timestamp    timestamp
     * @param indexTable   indextable
     * @param historyTable history table
     * @return
     */
    public static List<Integer> getActiveSegments(final long timestamp, final byte[] indexTable, final byte[] historyTable) {
        final Optional<IndexRecord> recordOpt = IndexRecord.search(timestamp, indexTable).getValue();
        final int startingOffset = recordOpt.isPresent() ? recordOpt.get().getHistoryOffset() : 0;

        final Optional<HistoryRecord> record = findRecordInHistoryTable(startingOffset, timestamp, historyTable);
        return record.isPresent() ? record.get().getSegments() : new ArrayList<>();
    }

    /**
     * Find segments from the candidate set that have overlapping key ranges with current segment.
     *
     * @param current    current segment number
     * @param candidates candidates
     * @return
     */
    public static List<Integer> getOverlaps(
            final Segment current,
            final List<Segment> candidates) {
        return candidates.stream().filter(x -> x.overlaps(current)).map(x -> x.getNumber()).collect(Collectors.toList());
    }

    /**
     * Find history record from the event when the given segment was sealed.
     * If segment is never sealed this method returns an empty list.
     * If segment is yet to be created, this method still returns empty list.
     * <p>
     * Find index that corresponds to segment start event.
     * Perform binary search on index+history records to find segment seal event.
     * <p>
     * If index table is not up to date we may have two cases:
     * 1. Segment create time > highest event time in index
     * 2. Segment seal time > highest event time in index
     * <p>
     * For 1 we cant have any searches in index and will need to fall through
     * History table starting from last indexed record.
     * <p>
     * For 2, fall through History Table starting from last indexed record
     * to find segment sealed event in history table.
     *
     * @param segment      segment
     * @param indexTable   index table
     * @param historyTable history table
     * @return
     */
    public static List<Integer> findSegmentSuccessorCandidates(
            final Segment segment,
            final byte[] indexTable,
            final byte[] historyTable) {
        // fetch segment start time from segment Is
        // fetch last index Ic
        // fetch record corresponding to Ic. If segment present in that history record, fall through history table
        // else perform binary searchIndex
        // Note: if segment is present at Ic, we will fall through in the history table one record at a time
        final Optional<IndexRecord> recordOpt = IndexRecord.search(segment.getStart(), indexTable)
                .getValue();
        final int startingOffset = recordOpt.isPresent() ? recordOpt.get().getHistoryOffset() : 0;

        final Optional<HistoryRecord> historyRecordOpt = findRecordInHistoryTable(startingOffset,
                segment.getStart(),
                historyTable);

        // segment information not in history table
        if (!historyRecordOpt.isPresent()) {
            return new ArrayList<>();
        }

        final int lower = IndexRecord.search(segment.getStart(), indexTable).getKey() / IndexRecord.INDEX_RECORD_SIZE;

        final int upper = (indexTable.length - IndexRecord.INDEX_RECORD_SIZE) / IndexRecord.INDEX_RECORD_SIZE;

        // index table may be stale, whereby we may not find segment.start to match an entry in the index table
        final Optional<IndexRecord> indexRecord = IndexRecord.readLatestRecord(indexTable);
        // if nothing is indexed read the first record in history table, hence offset = 0
        final int lastIndexedRecordOffset = indexRecord.isPresent() ? indexRecord.get().getHistoryOffset() : 0;

        final Optional<HistoryRecord> lastIndexedRecord = HistoryRecord.readRecord(historyTable, lastIndexedRecordOffset);

        // if segment is present in history table but its offset is greater than last indexed record,
        // we cant do anything on index table, fall through. OR
        // if segment exists at the last indexed record in history table, fall through,
        // no binary search possible on index
        if (lastIndexedRecord.get().getEventTime() < historyRecordOpt.get().getEventTime() ||
                lastIndexedRecord.get().getSegments().contains(segment.getNumber())) {
            // segment was sealed after the last index entry
            HistoryRecord startPoint = lastIndexedRecord.get().getEventTime() < historyRecordOpt.get().getEventTime() ?
                    historyRecordOpt.get() : lastIndexedRecord.get();
            Optional<HistoryRecord> next = HistoryRecord.fetchNext(startPoint, historyTable);

            while (next.isPresent() && next.get().getSegments().contains(segment.getNumber())) {
                startPoint = next.get();
                next = HistoryRecord.fetchNext(startPoint, historyTable);
            }

            if (next.isPresent()) {
                return next.get().getSegments();
            } else { // we have reached end of history table which means segment was never sealed
                return new ArrayList<>();
            }
        } else {
            // segment is definitely sealed and segment sealed event is also present in index table
            // we should be able to find it by doing binary search on Index table
            final Optional<HistoryRecord> record = findSegmentSealedEvent(
                    lower,
                    upper,
                    segment.getNumber(),
                    indexTable,
                    historyTable);

            return record.isPresent() ? record.get().getSegments() : new ArrayList<>();
        }
    }

    /**
     * Method to find candidates for predecessors.
     * If segment was created at the time of creation of stream (= no predecessors)
     * it returns an empty list.
     * <p>
     * First find the segment start time entry in the history table by using a binary
     * search on index followed by fall through History table if index is not up to date.
     * <p>
     * Fetch the record in history table that immediately preceeds segment created entry.
     *
     * @param segment      segment
     * @param indexTable   index table
     * @param historyTable history table
     * @return
     */
    public static List<Integer> findSegmentPredecessorCandidates(
            final Segment segment,
            final byte[] indexTable,
            final byte[] historyTable) {
        final Optional<IndexRecord> recordOpt = IndexRecord.search(segment.getStart(), indexTable)
                .getValue();
        final int startingOffset = recordOpt.isPresent() ? recordOpt.get().getHistoryOffset() : 0;

        final Optional<HistoryRecord> historyRecordOpt = findRecordInHistoryTable(startingOffset,
                segment.getStart(),
                historyTable);

        if (!historyRecordOpt.isPresent()) {
            return new ArrayList<>();
        }

        final HistoryRecord record = historyRecordOpt.get();

        final Optional<HistoryRecord> previous = HistoryRecord.fetchPrevious(record, historyTable);

        if (!previous.isPresent()) {
            return new ArrayList<>();
        } else {
            assert !previous.get().getSegments().contains(segment.getNumber());
            return previous.get().getSegments();
        }
    }

    /**
     * Add new segments to the segment table.
     * This method is designed to work with chunked creation. So it takes a
     * toCreate count and newRanges and it picks toCreate entries from the end of newranges.
     *
     * @param startingSegmentNumber starting segment number
     * @param segmentTable          segment table
     * @param toCreate              remaining number of segments to create
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return
     */
    public static byte[] updateSegmentTable(final int startingSegmentNumber,
                                            final byte[] segmentTable,
                                            final int toCreate,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final long timeStamp) {

        final int created = newRanges.size() - toCreate;

        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        try {
            segmentStream.write(segmentTable);

            IntStream.range(0, toCreate)
                    .forEach(
                            x -> {
                                try {
                                    segmentStream.write(new SegmentRecord(startingSegmentNumber + x,
                                            timeStamp,
                                            newRanges.get(created + x).getKey(),
                                            newRanges.get(created + x).getValue()).toByteArray());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return segmentStream.toByteArray();
    }

    /**
     * Add a new row to the history table.
     *
     * @param historyTable      history table
     * @param timestamp         timestamp
     * @param newActiveSegments new active segments
     * @return
     */
    public static byte[] updateHistoryTable(final byte[] historyTable,
                                            final long timestamp,
                                            final List<Integer> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        try {
            historyStream.write(historyTable);
            historyStream.write(new HistoryRecord(
                    timestamp,
                    newActiveSegments,
                    historyTable.length)
                    .toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param indexTable    index table
     * @param timestamp     timestamp
     * @param historyOffset history offset
     * @return
     */
    public static byte[] updateIndexTable(final byte[] indexTable,
                                          final long timestamp,
                                          final int historyOffset) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();

        try {
            indexStream.write(indexTable);
            indexStream.write(new IndexRecord(
                    timestamp,
                    historyOffset)
                    .toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return indexStream.toByteArray();
    }

    private static Optional<HistoryRecord> findRecordInHistoryTable(final int startingOffset,
                                                                    final long timeStamp,
                                                                    final byte[] historyTable) {
        final Optional<HistoryRecord> recordOpt = HistoryRecord.readRecord(historyTable, startingOffset);

        if (!recordOpt.isPresent() || recordOpt.get().getEventTime() > timeStamp) {
            return Optional.empty();
        }

        HistoryRecord record = recordOpt.get();
        Optional<HistoryRecord> next = HistoryRecord.fetchNext(record, historyTable);

        // check if current record is correct else we need to fall through
        // if timestamp is > record.timestamp and less than next.timestamp
        assert timeStamp >= record.getEventTime();
        while (next.isPresent() && timeStamp >= next.get().getEventTime()) {
            record = next.get();
            next = HistoryRecord.fetchNext(record, historyTable);
        }

        return Optional.of(record);
    }


    private static Optional<HistoryRecord> findSegmentSealedEvent(final int lower,
                                                                  final int upper,
                                                                  final int segmentNumber,
                                                                  final byte[] indexTable,
                                                                  final byte[] historyTable) {

        if (lower > upper || historyTable.length == 0) {
            return Optional.empty();
        }

        final int offset = ((lower + upper) / 2) * IndexRecord.INDEX_RECORD_SIZE;

        final Optional<IndexRecord> indexRecord = IndexRecord.readRecord(indexTable, offset);

        final Optional<IndexRecord> previousIndex = indexRecord.isPresent() ?
                IndexRecord.fetchPrevious(indexTable, offset) :
                Optional.empty();

        final int historyTableOffset = indexRecord.isPresent() ? indexRecord.get().getHistoryOffset() : 0;
        final Optional<HistoryRecord> record = HistoryRecord.readRecord(historyTable, historyTableOffset);

        // if segment is not present in history record, check if it is present in previous
        // if yes, we have found the segment sealed event
        // else repeat binary searchIndex
        if (!record.get().getSegments().contains(segmentNumber)) {
            assert previousIndex.isPresent();

            final Optional<HistoryRecord> previousRecord = HistoryRecord.readRecord(historyTable,
                    previousIndex.get().getHistoryOffset());
            if (previousRecord.get().getSegments().contains(segmentNumber)) {
                return record; // search complete
            } else { // binary search lower
                return findSegmentSealedEvent(lower,
                        (lower + upper) / 2 - 1,
                        segmentNumber,
                        indexTable,
                        historyTable);
            }
        } else { // binary search upper
            // not sealed in the current location: look in second half
            return findSegmentSealedEvent((lower + upper) / 2 + 1,
                    upper,
                    segmentNumber,
                    indexTable,
                    historyTable);
        }
    }
}
