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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import lombok.Lombok;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper class for operations pertaining to segment store tables (segment, history, index).
 * All the processing is done locally and this class does not make any network calls.
 * All methods are synchronous and blocking.
 */
public class TableHelper {
    /**
     * Segment Table records are of fixed size. So its O(constant) operation to read the segment record given the segment
     * number.
     *
     * @param number       segment number
     * @param segmentTable segment table
     * @return segment
     */
    public static Segment getSegment(final int number, final byte[] segmentTable) {

        Optional<SegmentRecord> recordOpt = SegmentRecord.readRecord(segmentTable, number);
        if (recordOpt.isPresent()) {
            SegmentRecord record = recordOpt.get();
            return new Segment(record.getSegmentNumber(),
                    record.getEpoch(),
                    record.getStartTime(),
                    record.getRoutingKeyStart(),
                    record.getRoutingKeyEnd());
        } else {
            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                    "Segment number: " + String.valueOf(number));
        }
    }

    /**
     * This method reads segment table and returns total number of segments in the table.
     *
     * @param segmentTable segment table.
     * @return total number of segments in the stream.
     */
    public static int getSegmentCount(final byte[] segmentTable) {
        return segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE;
    }

    /**
     * Current active segments correspond to last "complete" entry in the history table.
     * Until segment number is written to the history table it is not exposed to outside world
     * (e.g. callers - producers and consumers)
     *
     * @param historyTable history table
     * @return segments in the active epoch
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
     * Once we find the segments, compare them to truncationRecord and take the more recent of the two.
     * @param timestamp        timestamp
     * @param indexTable       index table
     * @param historyTable     history table
     * @param segmentTable     segment table
     * @param truncationRecord truncation record
     * @return list of active segments at given time or head of stream if timestamp is before head of stream.
     */
    public static List<Integer> getActiveSegments(final long timestamp, final byte[] indexTable, final byte[] historyTable,
                                                  final byte[] segmentTable, final StreamTruncationRecord truncationRecord) {
        // read epoch 0
        Optional<HistoryRecord> recordOpt = HistoryRecord.readRecord(historyTable, 0, true);
        if (recordOpt.isPresent() && timestamp > recordOpt.get().getScaleTime()) {
            final Optional<IndexRecord> indexOpt = IndexRecord.search(timestamp, indexTable);
            final int startingOffset = indexOpt.map(IndexRecord::getHistoryOffset).orElse(0);

            recordOpt = findRecordInHistoryTable(startingOffset, timestamp, historyTable, true);
        }

        return recordOpt.map(record -> {
            List<Integer> segments;
            if (truncationRecord == null) {
                segments = record.getSegments();
            } else {
                // case 1: if record.epoch is before truncation, simply pick the truncation stream cut
                if (record.getEpoch() < truncationRecord.getTruncationEpochLow()) {
                    segments = Lists.newArrayList(truncationRecord.getStreamCut().keySet());
                } else if (record.getEpoch() > truncationRecord.getTruncationEpochHigh()) {
                    // case 2: if record.epoch is after truncation, simply use the record epoch
                    segments = record.getSegments();
                } else {
                    // case 3: overlap between requested epoch and stream cut.
                    // take segments from stream cut that are from or aftergit re this epoch.
                    // take remaining segments from this epoch.
                    segments = new ArrayList<>();
                    // all segments from stream cut that have epoch >= this epoch
                    List<Integer> fromStreamCut = truncationRecord.getCutEpochMap().entrySet().stream()
                            .filter(x -> x.getValue() >= record.getEpoch())
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    segments.addAll(fromStreamCut);
                    // put remaining segments as those that dont overlap with ones taken from streamCut.
                    segments.addAll(record.getSegments().stream().filter(x -> fromStreamCut.stream().noneMatch(y ->
                            getSegment(x, segmentTable).overlaps(getSegment(y, segmentTable))))
                            .collect(Collectors.toList()));
                }
            }
            return segments;
        }).orElse(Collections.emptyList());
    }

    public static void validateStreamCut(List<AbstractMap.SimpleEntry<Double, Double>> list) {
        // verify that stream cut covers the entire range of 0.0 to 1.0 keyspace without overlaps.
        List<AbstractMap.SimpleEntry<Double, Double>> reduced = reduce(list);
        Exceptions.checkArgument(reduced.size() == 1 && reduced.get(0).getKey().equals(0.0) &&
                        reduced.get(0).getValue().equals(1.0), "streamCut",
                " Invalid input, Stream Cut does not cover full key range.");
    }

    public static StreamTruncationRecord computeTruncationRecord(final byte[] indexTable, final byte[] historyTable,
                                                                 final byte[] segmentTable, final Map<Integer, Long> streamCut,
                                                                 final StreamTruncationRecord previousTruncationRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(indexTable);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());

        Map<Integer, Integer> epochCutMap = computeEpochCutMap(historyTable, indexTable, segmentTable, streamCut);
        Map<Segment, Integer> cutMapSegments = transform(segmentTable, epochCutMap);

        Map<Segment, Integer> previousCutMapSegment = transform(segmentTable, previousTruncationRecord.getCutEpochMap());

        Exceptions.checkArgument(greaterThan(cutMapSegments, previousCutMapSegment, streamCut, previousTruncationRecord.getStreamCut()),
                "streamCut", "stream cut has to be strictly ahead of previous stream cut");

        Set<Integer> toDelete = computeToDelete(cutMapSegments, historyTable, segmentTable, previousTruncationRecord.getDeletedSegments());
        return new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), ImmutableMap.copyOf(epochCutMap),
                previousTruncationRecord.getDeletedSegments(), ImmutableSet.copyOf(toDelete));
    }

    /**
     * A method to compute size of stream in bytes from start till given stream cut.
     * Note: this computed size is absolute size and even if the stream has been truncated, this size is computed for the
     * entire amount of data that was written into the stream.
     *
     * @param indexTable index table for the stream
     * @param historyTable history table for the stream
     * @param segmentTable segment table for the stream
     * @param streamCut stream cut to compute size till
     * @param sealedSegmentsRecord record for all the sealed segments for the given stream.
     * @return size (in bytes) of stream till the given stream cut.
     */
    public static long getSizeTillStreamCut(final byte[] indexTable, final byte[] historyTable, final byte[] segmentTable,
                                            final Map<Integer, Long> streamCut, final SealedSegmentsRecord sealedSegmentsRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(indexTable);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(sealedSegmentsRecord);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());
        Map<Integer, Integer> epochCutMap = computeEpochCutMap(historyTable, indexTable, segmentTable, streamCut);
        Map<Segment, Integer> cutMapSegments = transform(segmentTable, epochCutMap);
        AtomicLong size = new AtomicLong();
        Map<Integer, Long> sealedSegmentSizeMap = sealedSegmentsRecord.getSealedSegmentsSizeMap();

        // add sizes for segments in stream cut
        streamCut.forEach((key, value) -> size.addAndGet(value));

        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(historyTable, 0, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            size.addAndGet(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentTable);
                return cutMapSegments.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().getNumber() == epochSegment.getNumber() ||
                        (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).map(sealedSegmentSizeMap::get).reduce((x, y) -> x + y).orElse(0L));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyTable, true);
        }

        return size.get();
    }

    public static List<Pair<Long, List<Integer>>> getScaleMetadata(byte[] historyTable) {
        return HistoryRecord.readAllRecords(historyTable);
    }

    /**
     * Find segments from the candidate set that have overlapping key ranges with current segment.
     *
     * @param current    current segment number
     * @param candidates candidates
     * @return overlapping segments from candidate list
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
     *
     * Find index that corresponds to segment start event.
     * Perform binary search on index+history records to find segment seal event.
     *
     * If index table is not up to date we may have two cases:
     * 1. Segment create time > highest event time in index
     * 2. Segment seal time > highest event time in index
     *
     * For 1 we cant have any searches in index and will need to fall through
     * History table starting from last indexed record.
     *
     * For 2, fall through History Table starting from last indexed record
     * to find segment sealed event in history table.
     *
     * @param segment      segment
     * @param indexTable   index table
     * @param historyTable history table
     * @return list of segments that belong to epoch where given segment was sealed
     */
    public static List<Integer> findSegmentSuccessorCandidates(
            final Segment segment,
            final byte[] indexTable,
            final byte[] historyTable) {
        // find segment creation epoch.
        // find latest epoch from history table.
        final int creationEpoch = segment.getEpoch();
        final Optional<HistoryRecord> creationRecordOpt = readEpochHistoryRecord(creationEpoch, indexTable, historyTable);

        // segment information not in history table
        if (!creationRecordOpt.isPresent()) {
            return new ArrayList<>();
        }

        final HistoryRecord latest = HistoryRecord.readLatestRecord(historyTable, false).get();

        if (latest.getSegments().contains(segment.getNumber())) {
            // Segment is not sealed yet so there cannot be a successor.
            return new ArrayList<>();
        } else {
            // segment is definitely sealed, we should be able to find it by doing binary search to find the respective epoch.
            return findSegmentSealedEvent(
                    creationEpoch,
                    latest.getEpoch(),
                    segment.getNumber(),
                    indexTable,
                    historyTable).map(HistoryRecord::getSegments).get();
        }
    }

    /**
     * Method to find candidates for predecessors.
     * If segment was created at the time of creation of stream (= no predecessors)
     * it returns an empty list.
     *
     * First find the segment start time entry in the history table by using a binary
     * search on index followed by fall through History table if index is not up to date.
     *
     * Fetch the record in history table that immediately preceeds segment created entry.
     *
     * @param segment      segment
     * @param indexTable   index table
     * @param historyTable history table
     * @return list of segments from epoch before the segment was created
     */
    public static List<Integer> findSegmentPredecessorCandidates(
            final Segment segment,
            final byte[] indexTable,
            final byte[] historyTable) {
        Optional<HistoryRecord> historyRecordOpt = readEpochHistoryRecord(segment.getEpoch(), indexTable, historyTable);
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
     * It takes the newRanges and creates corresponding entries in segment table.
     *
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return serialized segment table
     */
    @SneakyThrows
    public static byte[] createSegmentTable(final List<AbstractMap.SimpleEntry<Double, Double>> newRanges, final long timeStamp) {
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        writeSegmentsToSegmentTable(0, 0, newRanges, timeStamp, segmentStream);

        return segmentStream.toByteArray();
    }

    /**
     * Add new segments to the segment table.
     * It takes starting segment number and newRanges and it computes toCreate entries from the end of newranges.
     *
     * @param startingSegmentNumber starting segment number
     * @param creationEpoch         epoch in which segment is created
     * @param segmentTable          segment table
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return serialized segment table
     */
    @SneakyThrows
    public static byte[] updateSegmentTable(final int startingSegmentNumber,
                                            final int creationEpoch,
                                            final byte[] segmentTable,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final long timeStamp) {
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        segmentStream.write(segmentTable);

        writeSegmentsToSegmentTable(startingSegmentNumber, creationEpoch, newRanges, timeStamp, segmentStream);

        return segmentStream.toByteArray();
    }

    private static void writeSegmentsToSegmentTable(int startingSegmentNumber, int creationEpoch, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long timeStamp, ByteArrayOutputStream segmentStream) {
        IntStream.range(0, newRanges.size())
                .forEach(
                        newRange -> {
                            try {
                                segmentStream.write(new SegmentRecord(startingSegmentNumber + newRange,
                                        creationEpoch, timeStamp, newRanges.get(newRange).getKey(), newRanges.get(newRange).getValue())
                                        .toByteArray());
                            } catch (IOException e) {
                                throw Lombok.sneakyThrow(e);
                            }
                        }
                );
    }

    /**
     * Add a new row to the history table. This row is only partial as it only contains list of segments.
     * Timestamp is added using completeHistoryRecord method.
     *
     * @param historyTable      history table
     * @param newActiveSegments new active segments
     * @return serialized updated history table
     */
    @SneakyThrows
    public static byte[] addPartialRecordToHistoryTable(final byte[] historyTable,
                                                        final List<Integer> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();
        Optional<HistoryRecord> last = HistoryRecord.readLatestRecord(historyTable, false);
        assert last.isPresent() && !(last.get().isPartial());

        historyStream.write(historyTable);
        historyStream.write(new HistoryRecord(last.get().getEpoch() + 1, newActiveSegments, historyTable.length).toBytePartial());
        return historyStream.toByteArray();
    }

    /**
     * Adds timestamp to the last record in the history table.
     *
     * @param historyTable         history table
     * @param partialHistoryRecord partial history record
     * @param timestamp            scale timestamp
     * @return serialized updated history table
     */
    @SneakyThrows
    public static byte[] completePartialRecordInHistoryTable(final byte[] historyTable,
                                                             final HistoryRecord partialHistoryRecord,
                                                             final long timestamp) {
        Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyTable, false);
        assert record.isPresent() && record.get().isPartial() && record.get().getEpoch() == partialHistoryRecord.getEpoch();
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        historyStream.write(historyTable);

        historyStream.write(new HistoryRecord(partialHistoryRecord.getSegments(),
                partialHistoryRecord.getEpoch(), timestamp, partialHistoryRecord.getOffset()).remainingByteArray());
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to the history table.
     *
     * @param timestamp         timestamp
     * @param newActiveSegments new active segments
     * @return serialized history table
     */
    @SneakyThrows
    public static byte[] createHistoryTable(final long timestamp,
                                            final List<Integer> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        historyStream.write(new HistoryRecord(
                newActiveSegments,
                0,
                timestamp,
                0)
                .toByteArray());
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param timestamp     timestamp
     * @return serialized index table
     */
    @SneakyThrows
    public static byte[] createIndexTable(final long timestamp) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();

        indexStream.write(new IndexRecord(timestamp, 0, 0).toByteArray());
        return indexStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param indexTable    index table
     * @param timestamp     timestamp
     * @param historyOffset history table offset
     * @return serialized index table
     */
    @SneakyThrows
    public static byte[] updateIndexTable(final byte[] indexTable,
                                          final long timestamp,
                                          final int historyOffset) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();

        indexStream.write(indexTable);
        indexStream.write(new IndexRecord(
                timestamp,
                indexTable.length / IndexRecord.INDEX_RECORD_SIZE,
                historyOffset)
                .toByteArray());
        return indexStream.toByteArray();
    }

    /**
     * Method to check if a scale operation is currently ongoing and has created a new epoch (presence of partial record).
     * @param historyTable history table
     * @return true if a scale operation is ongoing, false otherwise
     */
    public static boolean isNewEpochCreated(final byte[] historyTable) {
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, false).get();
        return latestHistoryRecord.isPartial();
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
     * Method that looks at the supplied epoch transition record and compares it with partial state in metadata store to determine
     * if the partial state corresponds to supplied input.
     * 
     * @param epochTransitionRecord epoch transition record
     * @param historyTable history table
     * @param segmentTable segment table
     * @return true if input matches partial state, false otherwise
     */
    public static boolean isEpochTransitionConsistent(final EpochTransitionRecord epochTransitionRecord,
                    final byte[] historyTable,
                    final byte[] segmentTable) {
        AtomicBoolean isConsistent = new AtomicBoolean(true);
        SegmentRecord latest = SegmentRecord.readRecord(segmentTable, getSegmentCount(segmentTable) - 1).get();
        // verify that epoch transition record is consistent with segment table
        if (latest.getEpoch() == epochTransitionRecord.newEpoch) { // if segment table is updated
            epochTransitionRecord.newSegmentsWithRange.entrySet().forEach(segmentWithRange -> {
                Optional<SegmentRecord> segmentOpt = SegmentRecord.readRecord(segmentTable, segmentWithRange.getKey());
                isConsistent.compareAndSet(true, segmentOpt.isPresent() &&
                        segmentOpt.get().getEpoch() == epochTransitionRecord.getNewEpoch() &&
                        segmentOpt.get().getRoutingKeyStart() == segmentWithRange.getValue().getKey() &&
                        segmentOpt.get().getRoutingKeyEnd() == segmentWithRange.getValue().getValue());
            });
        } else { // if segment table is not updated
            isConsistent.compareAndSet(true, latest.getEpoch() == epochTransitionRecord.getActiveEpoch());
        }

        // verify that epoch transition record is consistent with history table
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyTable, false).get();
        // if history table is not updated
        if (latestHistoryRecord.getEpoch() == epochTransitionRecord.activeEpoch) {
            isConsistent.compareAndSet(true,
                    !latestHistoryRecord.isPartial() &&
                    latestHistoryRecord.getSegments().containsAll(epochTransitionRecord.segmentsToSeal));
        } else if (latestHistoryRecord.getEpoch() == epochTransitionRecord.newEpoch) {
            // if history table is updated
            boolean check = latestHistoryRecord.getSegments().containsAll(epochTransitionRecord.newSegmentsWithRange.keySet()) &&
            epochTransitionRecord.segmentsToSeal.stream().noneMatch(x -> latestHistoryRecord.getSegments().contains(x));

            isConsistent.compareAndSet(true, check);
        } else {
            isConsistent.set(false);
        }

        return isConsistent.get();
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
     * It finds the segment sealed event between lower and upper where 'lower' offset is guaranteed to be greater than or
     * equal to segment creation epoch
     * @param lowerEpoch starting record number in index table from where to search
     * @param upperEpoch last record number in index table till where to search
     * @param segmentNumber segment number to find sealed event
     * @param indexTable index table
     * @param historyTable history table
     * @return returns history record where segment was sealed
     */
    private static Optional<HistoryRecord> findSegmentSealedEvent(final int lowerEpoch,
                                                                  final int upperEpoch,
                                                                  final int segmentNumber,
                                                                  final byte[] indexTable,
                                                                  final byte[] historyTable) {
        if (lowerEpoch > upperEpoch || historyTable.length == 0) {
            return Optional.empty();
        }

        final int middle = (lowerEpoch + upperEpoch) / 2;

        final Optional<HistoryRecord> record = readEpochHistoryRecord(middle, indexTable, historyTable);
        assert record.isPresent();
        // if segment is not present in middle record, check if it is present in previous
        // if yes, we have found the segment sealed event
        // else repeat binary searchIndex
        if (!record.get().getSegments().contains(segmentNumber)) {
            final Optional<HistoryRecord> previousRecord = HistoryRecord.fetchPrevious(record.get(), historyTable);
            assert previousRecord.isPresent();
            if (previousRecord.get().getSegments().contains(segmentNumber)) {
                return record; // search complete
            } else { // binary search lower
                return findSegmentSealedEvent(lowerEpoch,
                        (lowerEpoch + upperEpoch) / 2 - 1,
                        segmentNumber,
                        indexTable,
                        historyTable);
            }
        } else { // binary search upper
            // not sealed in the current location: look in second half
            return findSegmentSealedEvent((lowerEpoch + upperEpoch) / 2 + 1,
                    upperEpoch,
                    segmentNumber,
                    indexTable,
                    historyTable);
        }
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

    private static Map<Segment, Integer> transform(byte[] segmentTable, Map<Integer, Integer> epochStreamCutMap) {
        return epochStreamCutMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> getSegment(entry.getKey(), segmentTable),
                        Map.Entry::getValue));
    }

    /**
     * Method to compute epoch transition record. It takes segments to seal and new ranges and all the tables and
     * computes the next epoch transition record.
     * @param historyTable history table.
     * @param segmentTable segment table
     * @param segmentsToSeal segments to seal
     * @param newRanges new ranges
     * @param scaleTimestamp scale time
     * @return new epoch transition record based on supplied input
     */
    public static EpochTransitionRecord computeEpochTransition(byte[] historyTable, byte[] segmentTable,
                                                               List<Integer> segmentsToSeal,
                                                               List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                               long scaleTimestamp) {
        Pair<Integer, List<Integer>> activeEpoch = getActiveEpoch(historyTable);
        Preconditions.checkState(activeEpoch.getValue().containsAll(segmentsToSeal), "Invalid epoch transition request");

        int newEpoch = activeEpoch.getKey() + 1;
        int segmentCount = getSegmentCount(segmentTable);
        Map<Integer, AbstractMap.SimpleEntry<Double, Double>> newSegments = new HashMap<>();
        IntStream.range(0, newRanges.size()).forEach(x -> {
            newSegments.put(segmentCount + x, newRanges.get(x));
        });
        return new EpochTransitionRecord(activeEpoch.getKey(), newEpoch, scaleTimestamp, ImmutableSet.copyOf(segmentsToSeal),
                ImmutableMap.copyOf(newSegments));
    }

    private static Map<Integer, Integer> computeEpochCutMap(byte[] historyTable, byte[] indexTable,
                                                                  byte[] segmentTable, Map<Integer, Long> streamCut) {
        Map<Integer, Integer> epochStreamCutMap = new HashMap<>();

        int mostRecent = streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
        Segment mostRecentSegment = getSegment(mostRecent, segmentTable);

        final Optional<HistoryRecord> highEpochRecord = readEpochHistoryRecord(mostRecentSegment.getEpoch(),
                indexTable, historyTable);

        List<Integer> toFind = new ArrayList<>(streamCut.keySet());
        Optional<HistoryRecord> epochRecord = highEpochRecord;

        while (epochRecord.isPresent() && !toFind.isEmpty()) {
            List<Integer> epochSegments = epochRecord.get().getSegments();
            Map<Boolean, List<Integer>> group = toFind.stream().collect(Collectors.groupingBy(epochSegments::contains));
            toFind = Optional.ofNullable(group.get(false)).orElse(Collections.emptyList());
            int epoch = epochRecord.get().getEpoch();
            List<Integer> found = Optional.ofNullable(group.get(true)).orElse(Collections.emptyList());
            found.forEach(x -> epochStreamCutMap.put(x, epoch));
            epochRecord = HistoryRecord.fetchPrevious(epochRecord.get(), historyTable);
        }

        return epochStreamCutMap;
    }

    public static Optional<HistoryRecord> readEpochHistoryRecord(int epoch, byte[] indexTable, byte[] historyTable) {
        Optional<IndexRecord> index = IndexRecord.readRecord(indexTable, epoch);
        return index.flatMap(i -> HistoryRecord.readRecord(historyTable, i.getHistoryOffset(), false));
    }

    private static Set<Integer> computeToDelete(Map<Segment, Integer> epochCutMap, byte[] historyTable,
                                                byte[] segmentTable, Set<Integer> deletedSegments) {
        Set<Integer> toDelete = new HashSet<>();
        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);

        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(historyTable, 0, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            toDelete.addAll(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentTable);
                // ignore already deleted segments from todelete
                // toDelete.add(epoch.segment overlaps cut.segment && epoch < cut.segment.epoch)
                return !deletedSegments.contains(epochSegmentNumber) &&
                        epochCutMap.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().getNumber() == epochSegment.getNumber() ||
                        (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).collect(Collectors.toSet()));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyTable, true);
        }
        return toDelete;
    }

    private static boolean greaterThan(Map<Segment, Integer> map1, Map<Segment, Integer> map2, Map<Integer, Long> cut1, Map<Integer, Long> cut2) {
        // find overlapping segments in map2 for all segments in map1
        // compare epochs. map1 should have epochs gt or eq its overlapping segments in map2
        return map1.entrySet().stream().allMatch(e1 ->
                map2.entrySet().stream().noneMatch(e2 ->
                        (e2.getKey().getNumber() == e1.getKey().getNumber() && cut1.get(e1.getKey().getNumber()) < cut2.get(e2.getKey().getNumber()))
                        || (e2.getKey().overlaps(e1.getKey()) && e1.getValue() < e2.getValue())));
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
