/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables;

import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                    "Segment number: " + String.valueOf(number));
        }
    }

    /**
     * Helper method to get next higher number than highest segment number.
     * @param segmentTable segment table.
     * @return
     */
    public static int getLastSegmentNumber(final byte[] segmentTable) {
        return (segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE) - 1;
    }

    /**
     * This method reads segment table and returns total number of segments in the table.
     *
     * @param segmentTable history table.
     * @return total number of segments in the stream.
     */
    public static int getSegmentCount(final byte[] segmentTable) {
        return segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE;
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
        final Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyTable, true);

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
        Optional<HistoryRecord> record = HistoryRecord.readRecord(historyTable, 0, true);
        if (record.isPresent() && timestamp > record.get().getScaleTime()) {
            final Optional<IndexRecord> recordOpt = IndexRecord.search(timestamp, indexTable).getValue();
            final int startingOffset = recordOpt.isPresent() ? recordOpt.get().getHistoryOffset() : 0;

            record = findRecordInHistoryTable(startingOffset, timestamp, historyTable, true);
        }
        return record.map(HistoryRecord::getSegments).orElse(new ArrayList<>());
    }

    public static List<Pair<Long, List<Integer>>> getScaleMetadata(byte[] historyTable) {
        return HistoryRecord.readAllRecords(historyTable);
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
        Pair<Integer, Optional<IndexRecord>> search = IndexRecord.search(segment.getStart(), indexTable);
        final Optional<IndexRecord> recordOpt = search.getValue();
        final int startingOffset = recordOpt.isPresent() ? recordOpt.get().getHistoryOffset() : 0;

        final Optional<HistoryRecord> segmentCreatedHistoryRecordOpt = findSegmentCreatedEvent(startingOffset,
                segment, historyTable);

        // segment information not in history table
        if (!segmentCreatedHistoryRecordOpt.isPresent()) {
            return new ArrayList<>();
        }

        // take index of segment created event instead of searched index based on segment.startTime.
        final int lower = IndexRecord.search(segmentCreatedHistoryRecordOpt.get().getScaleTime(), indexTable).getKey() / IndexRecord.INDEX_RECORD_SIZE;

        final int upper = (indexTable.length - IndexRecord.INDEX_RECORD_SIZE) / IndexRecord.INDEX_RECORD_SIZE;

        // index table may be stale, whereby we may not find segment.start to match an entry in the index table
        final Optional<IndexRecord> indexRecord = IndexRecord.readLatestRecord(indexTable);
        // if nothing is indexed read the first record in history table, hence offset = 0
        final int lastIndexedRecordOffset = indexRecord.isPresent() ? indexRecord.get().getHistoryOffset() : 0;

        final Optional<HistoryRecord> lastIndexedRecord = HistoryRecord.readRecord(historyTable, lastIndexedRecordOffset, false);

        // if segment is present in history table but its offset is greater than last indexed record,
        // we cant do anything on index table, fall through. OR
        // if segment exists at the last indexed record in history table, fall through,
        // no binary search possible on index
        // Note: lower will always be lessThanEq upper. So if upper.getScaleTime < segmentCreatedRecord.ScaleTime then lower < segmentCreatedRecord.ScaleTime.
        if (lastIndexedRecord.get().getScaleTime() < segmentCreatedHistoryRecordOpt.get().getScaleTime() ||
                lastIndexedRecord.get().getSegments().contains(segment.getNumber())) {
            // segment was sealed after the last index entry
            HistoryRecord startPoint = lastIndexedRecord.get().getScaleTime() < segmentCreatedHistoryRecordOpt.get().getScaleTime() ?
                    segmentCreatedHistoryRecordOpt.get() : lastIndexedRecord.get();
            Optional<HistoryRecord> next = HistoryRecord.fetchNext(startPoint, historyTable, false);

            while (next.isPresent() && next.get().getSegments().contains(segment.getNumber())) {
                startPoint = next.get();
                next = HistoryRecord.fetchNext(startPoint, historyTable, false);
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

        Optional<HistoryRecord> historyRecordOpt = findSegmentCreatedEvent(startingOffset, segment, historyTable);
        if (!historyRecordOpt.isPresent()) {
            // cant compute predecessors because the creation event is not present in history table yet.
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
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return
     */
    public static byte[] updateSegmentTable(final int startingSegmentNumber,
                                            final byte[] segmentTable,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final long timeStamp) {
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        try {
            segmentStream.write(segmentTable);

            IntStream.range(0, newRanges.size())
                    .forEach(
                            x -> {
                                try {
                                    segmentStream.write(new SegmentRecord(startingSegmentNumber + x,
                                            timeStamp,
                                            newRanges.get(x).getKey(),
                                            newRanges.get(x).getValue()).toByteArray());
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
     * Add a new row to the history table. This row is only partial as it only contains list of segments.
     * Timestamp is added using completeHistoryRecord method.
     *
     * @param historyTable      history table
     * @param newActiveSegments new active segments
     * @return
     */
    public static byte[] addPartialRecordToHistoryTable(final byte[] historyTable,
                                                        final List<Integer> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();
        Optional<HistoryRecord> last = HistoryRecord.readLatestRecord(historyTable, false);
        assert last.isPresent() && !(last.get().isPartial());

        try {
            historyStream.write(historyTable);
            historyStream.write(new HistoryRecord(last.get().getEpoch() + 1, newActiveSegments, historyTable.length).toBytePartial());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Adds timestamp to the last record in the history table.
     *
     * @param historyTable         history table
     * @param partialHistoryRecord partial history record
     * @param timestamp            scale timestamp
     * @return
     */
    public static byte[] completePartialRecordInHistoryTable(final byte[] historyTable,
                                                             final HistoryRecord partialHistoryRecord,
                                                             final long timestamp) {
        Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyTable, false);
        assert record.isPresent() && record.get().isPartial() && record.get().getEpoch() == partialHistoryRecord.getEpoch();
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        try {
            historyStream.write(historyTable);

            historyStream.write(new HistoryRecord(partialHistoryRecord.getSegments(),
                    partialHistoryRecord.getEpoch(), timestamp, partialHistoryRecord.getOffset()).remainingByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to the history table.
     *
     * @param timestamp         timestamp
     * @param newActiveSegments new active segments
     * @return
     */
    public static byte[] createHistoryTable(final long timestamp,
                                            final List<Integer> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        try {
            historyStream.write(new HistoryRecord(
                    newActiveSegments,
                    0,
                    timestamp,
                    0)
                    .toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param timestamp     timestamp
     * @param historyOffset history offset
     * @return
     */
    public static byte[] createIndexTable(final long timestamp,
                                          final int historyOffset) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();

        try {
            indexStream.write(new IndexRecord(
                    timestamp,
                    historyOffset)
                    .toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return indexStream.toByteArray();
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

    /**
     * Method to check if a scale operation is currently ongoing.
     * @param historyTable history table
     * @param segmentTable segment table
     * @return true if a scale operation is ongoing, false otherwise
     */
    public static boolean isScaleOngoing(final byte[] historyTable, final byte[] segmentTable) {

        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, false).get();
        return latestHistoryRecord.isPartial() || !latestHistoryRecord.getSegments().contains(getLastSegmentNumber(segmentTable));
    }

    /**
     * Method to check if no scale operation is currently ongoing and scale operation can be performed with given input.
     * @param segmentsToSeal segments to seal
     * @param historyTable history table
     * @return true if a scale operation can be performed, false otherwise
     */
    public static boolean canScaleFor(final List<Integer> segmentsToSeal, final byte[] historyTable) {
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, false).get();
        return latestHistoryRecord.getSegments().containsAll(segmentsToSeal);
    }

    /**
     * Method that looks at the supplied input and compares it with partial state in metadata store to determine
     * if the partial state corresponds to supplied input.
     * @param segmentsToSeal segments to seal
     * @param newRanges new ranges to create
     * @param historyTable history table
     * @param segmentTable segment table
     * @return true if input matches partial state, false otherwise
     */
    public static boolean isRerunOf(final List<Integer> segmentsToSeal,
                    final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                    final byte[] historyTable,
                    final byte[] segmentTable) {
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, false).get();

        int n = newRanges.size();
        List<SegmentRecord> lastN = SegmentRecord.readLastN(segmentTable, n);

        boolean newSegmentsPredicate = newRanges.stream()
                .allMatch(x -> lastN.stream().anyMatch(y -> y.getRoutingKeyStart() == x.getKey() && y.getRoutingKeyEnd() == x.getValue()));
        boolean segmentToSealPredicate;
        boolean exactMatchPredicate;

        // CASE 1: only segment table is updated.. history table isnt...
        if (!latestHistoryRecord.isPartial()) {
            // it is implicit: history.latest.containsNone(lastN)
            segmentToSealPredicate = latestHistoryRecord.getSegments().containsAll(segmentsToSeal);
            assert !latestHistoryRecord.getSegments().isEmpty();
            exactMatchPredicate = latestHistoryRecord.getSegments().stream()
                    .max(Comparator.naturalOrder()).get() + n == getLastSegmentNumber(segmentTable);
        } else { // CASE 2: segment table updated.. history table updated (partial record)..
            // since latest is partial so previous has to exist
            HistoryRecord previousHistoryRecord = HistoryRecord.fetchPrevious(latestHistoryRecord, historyTable).get();

            segmentToSealPredicate = latestHistoryRecord.getSegments().containsAll(lastN.stream()
                    .map(SegmentRecord::getSegmentNumber).collect(Collectors.toList())) &&
                    previousHistoryRecord.getSegments().containsAll(segmentsToSeal);
            exactMatchPredicate = previousHistoryRecord.getSegments().stream()
                    .max(Comparator.naturalOrder()).get() + n == getLastSegmentNumber(segmentTable);
        }

        return newSegmentsPredicate && segmentToSealPredicate && exactMatchPredicate;
    }

    /**
     * Return the active epoch.
     * @param historyTableData history table
     * @return active epoch
     */
    public static Pair<Integer, List<Integer>> getActiveEpoch(byte[] historyTableData) {
        HistoryRecord historyRecord = HistoryRecord.readLatestRecord(historyTableData, true).get();
        return new ImmutablePair<>(historyRecord.getEpoch(), historyRecord.getSegments());
    }

    /**
     * Return segments in the epoch.
     * @param historyTableData history table
     * @param epoch            epoch
     *
     * @return segments in the epoch
     */
    public static List<Integer> getSegmentsInEpoch(byte[] historyTableData, int epoch) {
        Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyTableData, false);

        while (record.isPresent() && record.get().getEpoch() > epoch) {
            record = HistoryRecord.fetchPrevious(record.get(), historyTableData);
        }

        return record.orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Epoch: " + epoch + " not found in history table")).getSegments();
    }

    /**
     * Return the active epoch.
     * @param historyTableData history table
     * @return active epoch
     */
    public static Pair<Integer, List<Integer>> getLatestEpoch(byte[] historyTableData) {
        HistoryRecord historyRecord = HistoryRecord.readLatestRecord(historyTableData, false).get();
        return new ImmutablePair<>(historyRecord.getEpoch(), historyRecord.getSegments());
    }

    /**
     * Method to compute segments created and deleted in latest scale event.
     *
     * @param historyTable history table
     * @return pair of segments sealed and segments created in last scale event.
     */
    public static Pair<List<Integer>, List<Integer>> getLatestScaleData(final byte[] historyTable) {
        final Optional<HistoryRecord> current = HistoryRecord.readLatestRecord(historyTable, false);
        ImmutablePair<List<Integer>, List<Integer>> result;
        if (current.isPresent()) {
            final Optional<HistoryRecord> previous = HistoryRecord.fetchPrevious(current.get(), historyTable);
            result = previous.map(historyRecord ->
                    new ImmutablePair<>(diff(historyRecord.getSegments(), current.get().getSegments()),
                        diff(current.get().getSegments(), historyRecord.getSegments())))
                    .orElseGet(() -> new ImmutablePair<>(Collections.emptyList(), current.get().getSegments()));
        } else {
            result = new ImmutablePair<>(Collections.emptyList(), Collections.emptyList());
        }
        return result;
    }

    private static List<Integer> diff(List<Integer> list1, List<Integer> list2) {
        return list1.stream().filter(z -> !list2.contains(z)).collect(Collectors.toList());
    }

    private static Optional<HistoryRecord> findRecordInHistoryTable(final int startingOffset,
                                                                    final long timeStamp,
                                                                    final byte[] historyTable,
                                                                    final boolean ignorePartial) {
        final Optional<HistoryRecord> recordOpt = HistoryRecord.readRecord(historyTable, startingOffset, ignorePartial);

        if (!recordOpt.isPresent() || recordOpt.get().getScaleTime() > timeStamp) {
            return Optional.empty();
        }

        HistoryRecord record = recordOpt.get();
        Optional<HistoryRecord> next = HistoryRecord.fetchNext(record, historyTable, ignorePartial);

        // check if current record is correct else we need to fall through
        // if timestamp is > record.timestamp and less than next.timestamp
        assert timeStamp >= record.getScaleTime();
        while (next.isPresent() && !next.get().isPartial() && timeStamp >= next.get().getScaleTime()) {
            record = next.get();
            next = HistoryRecord.fetchNext(record, historyTable, ignorePartial);
        }

        return Optional.of(record);
    }

    /**
     * It finds the segment sealed event between lower and upper where 'lower' offset is guaranteed to be greater than or equal to segmentCreatedEvent
     * @param lower starting record number in index table from where to search
     * @param upper last record number in index table till where to search
     * @param segmentNumber segment number to find sealed event
     * @param indexTable index table
     * @param historyTable history table
     * @return returns history record where segment was sealed
     */
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
        final Optional<HistoryRecord> record = HistoryRecord.readRecord(historyTable, historyTableOffset, false);

        // if segment is not present in history record, check if it is present in previous
        // if yes, we have found the segment sealed event
        // else repeat binary searchIndex
        if (!record.get().getSegments().contains(segmentNumber)) {
            assert previousIndex.isPresent();

            final Optional<HistoryRecord> previousRecord = HistoryRecord.readRecord(historyTable,
                    previousIndex.get().getHistoryOffset(), false);
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

    private static Optional<HistoryRecord> findSegmentCreatedEvent(final int startingOffset,
                                                                   final Segment segment,
                                                                   final byte[] historyTable) {

        Optional<HistoryRecord> historyRecordOpt = findRecordInHistoryTable(startingOffset,
                segment.getStart(), historyTable, false);

        if (!historyRecordOpt.isPresent()) {
            // segment not present in history record.
            return Optional.empty();
        }

        // By doing the indexed search using segment's start time we have found the record in history table that was active
        // at the time segment was created in segment table.
        // Since segment has eventTime from before scale and history record is assigned time after scale,
        // So history record's time identifying when segment was created will typically be after the segment table record.
        // This is not true for initial sets of segments though where segment.createTime == historyrecord.eventTime.
        // So we will need to check at both records. We are guaranteed that it cannot be before this though.
        // Question is should we fall thru more than one entry because of clock mismatch between controller instances.
        while (historyRecordOpt.isPresent() && !historyRecordOpt.get().getSegments().contains(segment.getNumber())) {
            historyRecordOpt = HistoryRecord.fetchNext(historyRecordOpt.get(), historyTable, false);
        }

        return historyRecordOpt;
    }

    public static boolean isScaleInputValid(final List<Integer> segmentsToSeal,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final byte[] segmentTable) {
        boolean newRangesPredicate = newRanges.stream().noneMatch(x -> x.getKey() >= x.getValue() &&
                x.getKey() >= 0 && x.getValue() > 0);

        List<AbstractMap.SimpleEntry<Double, Double>> oldRanges = segmentsToSeal.stream()
                .map(segment -> SegmentRecord.readRecord(segmentTable, segment).map(x ->
                        new AbstractMap.SimpleEntry<>(x.getRoutingKeyStart(), x.getRoutingKeyEnd())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        return newRangesPredicate && reduce(oldRanges).equals(reduce(newRanges));
    }

    /**
     * Helper method to compute list of continuous ranges. For example, two neighbouring key ranges where,
     * range1.high == range2.low then they are considered neighbours.
     * This method reduces input range into distinct continuous blocks.
     * @param input list of key ranges.
     * @return reduced list of key ranges.
     */
    private static List<AbstractMap.SimpleEntry<Double, Double>> reduce(List<AbstractMap.SimpleEntry<Double, Double>> input) {
        List<AbstractMap.SimpleEntry<Double, Double>> ranges = new ArrayList<>(input);
        ranges.sort(Comparator.comparingDouble(AbstractMap.SimpleEntry::getKey));
        List<AbstractMap.SimpleEntry<Double, Double>> result = new ArrayList<>();
        double low = -1.0;
        double high = -1.0;
        for (AbstractMap.SimpleEntry<Double, Double> range : ranges) {
            if (high < range.getKey()) {
                // add previous result and start a new result if prev.high is less than next.low
                if (low != -1.0 && high != -1.0) {
                    result.add(new AbstractMap.SimpleEntry<>(low, high));
                }
                low = range.getKey();
                high = range.getValue();
            } else if (high == range.getKey()) {
                // if adjacent (prev.high == next.low) then update only high
                high = range.getValue();
            } else {
                // if prev.high > next.low.
                // [Note: next.low cannot be less than 0] which means prev.high > 0
                assert low >= 0;
                assert high > 0;
                result.add(new AbstractMap.SimpleEntry<>(low, high));
                low = range.getKey();
                high = range.getValue();
            }
        }
        // add the last range
        if (low != -1.0 && high != -1.0) {
            result.add(new AbstractMap.SimpleEntry<>(low, high));
        }
        return result;
    }
}
