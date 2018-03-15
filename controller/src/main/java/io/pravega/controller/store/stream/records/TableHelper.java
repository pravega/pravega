/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import lombok.Lombok;
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
     * Segment Table records are indexed and and it is O(constant) operation to get segment offset given segmentIndex.
     *
     * @param number       segment number
     * @param segmentIndex segment table index
     * @param segmentTable segment table
     * @return Segment
     */
    public static Segment getSegment(final int number, final byte[] segmentIndex, final byte[] segmentTable) {

        Optional<SegmentRecord> recordOpt = SegmentRecord.readRecord(segmentIndex, segmentTable, number);
        if (recordOpt.isPresent()) {
            SegmentRecord record = recordOpt.get();
            return new Segment(record.getSegmentNumber(),
                    record.getStartTime(),
                    record.getCreationEpoch(),
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
     * @param segmentIndex segment table index
     * @param segmentTable segment table
     * @return total number of segments in the stream.
     */
    public static int getSegmentCount(final byte[] segmentIndex, final byte[] segmentTable) {
        Optional<SegmentRecord> segmentRecord = SegmentRecord.readLatest(segmentIndex, segmentTable);
        assert segmentRecord.isPresent();
        return segmentRecord.get().getSegmentNumber() + 1;
    }

    /**
     * Current active segments correspond to last entry in the history table.
     * Until segment number is written to the history table it is not exposed to outside world
     * (e.g. callers - producers and consumers)
     *
     * @param historyIndex history index
     * @param historyTable history table
     * @return list of active segment numbers in current active epoch. This ignores partial epochs if scale operation
     * is ongoing and returns the latest completed epoch.
     */
    public static List<Integer> getActiveSegments(final byte[] historyIndex, final byte[] historyTable) {
        final Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyIndex, historyTable, true);

        return record.isPresent() ? record.get().getSegments() : new ArrayList<>();
    }

    /**
     * Get active segments at given timestamp.
     * Perform binary search on index table to find the record corresponding to timestamp.
     * Once we find the segments, compare them to truncationRecord and take the more recent of the two.
     * @param timestamp        timestamp
     * @param historyIndex     history index
     * @param historyTable     history table
     * @param segmentIndex     segment index
     * @param segmentTable     segment table
     * @param truncationRecord truncation record
     * @return list of active segments.
     */
    public static List<Integer> getActiveSegments(final long timestamp, final byte[] historyIndex, final byte[] historyTable,
                                                  final byte[] segmentIndex, final byte[] segmentTable,
                                                  final StreamTruncationRecord truncationRecord) {
        Optional<HistoryRecord> recordOpt = HistoryRecord.readRecord(historyIndex, historyTable, 0, true);
        if (recordOpt.isPresent() && timestamp > recordOpt.get().getScaleTime()) {
            final Optional<HistoryIndexRecord> indexOpt = HistoryIndexRecord.search(timestamp, historyIndex);
            final int startingOffset = indexOpt.map(HistoryIndexRecord::getHistoryOffset).orElse(0);

            // read the record corresponding to indexed record
            recordOpt = HistoryRecord.readRecord(historyIndex, historyTable, indexOpt.get().getEpoch(), true);
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
                            getSegment(x, segmentIndex, segmentTable).overlaps(getSegment(y, segmentIndex, segmentTable))))
                            .collect(Collectors.toList()));
                }
            }
            return segments;
        }).orElse(Collections.emptyList());
    }

    /**
     * Method to validate a given stream Cut.
     * A stream cut is valid if it covers the entire key space without any overlaps in ranges for segments that form the
     * streamcut. It throws Invalid argument exception if the supplied stream cut does not satisfy the invariants.
     *
     * @param streamCut supplied stream cut.
     */
    public static void validateStreamCut(List<AbstractMap.SimpleEntry<Double, Double>> streamCut) {
        // verify that stream cut covers the entire range of 0.0 to 1.0 keyspace without overlaps.
        List<AbstractMap.SimpleEntry<Double, Double>> reduced = reduce(streamCut);
        Exceptions.checkArgument(reduced.size() == 1 && reduced.get(0).getKey().equals(0.0) &&
                        reduced.get(0).getValue().equals(1.0), "streamCut",
                " Invalid input, Stream Cut does not cover full key range.");
    }

    /**
     * Method to compute new truncation record by applying supplied streamCut on previous truncation record.
     * @param historyIndex             history index
     * @param historyTable             history table
     * @param segmentIndex             segment index
     * @param segmentTable             segment table
     * @param streamCut                stream cut to truncate at.
     * @param previousTruncationRecord the current truncation record that identifies truncation point.
     * @return Returns new truncation record with updating flag set to true.
     */
    public static StreamTruncationRecord computeTruncationRecord(final byte[] historyIndex, final byte[] historyTable,
                                                                 final byte[] segmentIndex, final byte[] segmentTable,
                                                                 final Map<Integer, Long> streamCut,
                                                                 final StreamTruncationRecord previousTruncationRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(historyIndex);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(segmentIndex);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());

        Map<Integer, Integer> epochCutMap = computeEpochCutMap(historyIndex, historyTable, segmentIndex, segmentTable, streamCut);
        Map<Segment, Integer> cutMapSegments = transform(segmentIndex, segmentTable, epochCutMap);

        Map<Segment, Integer> previousCutMapSegment = transform(segmentIndex, segmentTable, previousTruncationRecord.getCutEpochMap());

        Exceptions.checkArgument(greaterThan(cutMapSegments, previousCutMapSegment, streamCut, previousTruncationRecord.getStreamCut()),
                "streamCut", "stream cut has to be strictly ahead of previous stream cut");

        Set<Integer> toDelete = computeToDelete(cutMapSegments, historyIndex, historyTable, segmentIndex, segmentTable,
                previousTruncationRecord.getDeletedSegments());
        return new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), ImmutableMap.copyOf(epochCutMap),
                previousTruncationRecord.getDeletedSegments(), ImmutableSet.copyOf(toDelete), true);
    }

    /**
     * A method to compute size of stream in bytes from start till given stream cut.
     * Note: this computed size is absolute size and even if the stream has been truncated, this size is computed for the
     * entire amount of data that was written into the stream.
     *
     * @param historyIndex history index for the stream
     * @param historyTable history table for the stream
     * @param segmentIndex segment index for the stream
     * @param segmentTable segment table for the stream
     * @param streamCut stream cut to compute size till
     * @param sealedSegmentsRecord record for all the sealed segments for the given stream.
     * @return size (in bytes) of stream till the given stream cut.
     */
    public static long getSizeTillStreamCut(final byte[] historyIndex, final byte[] historyTable, final byte[] segmentIndex,
                                            final byte[] segmentTable, final Map<Integer, Long> streamCut,
                                            final SealedSegmentsRecord sealedSegmentsRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(historyIndex);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(sealedSegmentsRecord);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());
        Map<Integer, Integer> epochCutMap = computeEpochCutMap(historyIndex, historyTable, segmentIndex, segmentTable, streamCut);
        Map<Segment, Integer> cutMapSegments = transform(segmentIndex, segmentTable, epochCutMap);
        AtomicLong size = new AtomicLong();
        Map<Integer, Long> sealedSegmentSizeMap = sealedSegmentsRecord.getSealedSegmentsSizeMap();

        // add sizes for segments in stream cut
        streamCut.forEach((key, value) -> size.addAndGet(value));

        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(historyIndex, historyTable, 0, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            size.addAndGet(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentIndex, segmentTable);
                return cutMapSegments.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().getNumber() == epochSegment.getNumber() ||
                        (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).map(sealedSegmentSizeMap::get).reduce((x, y) -> x + y).orElse(0L));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyIndex, historyTable, true);
        }

        return size.get();
    }

    /**
     * Method to return all the epochs metadata.
     * @param historyIndex history index
     * @param historyTable history table
     * @return List of pairs of epoch and segments in the epoch.
     */
    public static List<Pair<Long, List<Integer>>> getScaleMetadata(final byte[] historyIndex, final byte[] historyTable) {
        return HistoryRecord.readAllRecords(historyIndex, historyTable);
    }

    /**
     * Find segments from the candidate set that have overlapping key ranges with current segment.
     *
     * @param current    current segment number
     * @param candidates candidates for overlap
     * @return overlapping segments with current segment
     */
    public static List<Integer> getOverlaps(
            final Segment current,
            final List<Segment> candidates) {
        return candidates.stream().filter(x -> x.overlaps(current)).map(Segment::getNumber).collect(Collectors.toList());
    }

    /**
     * Find history record from the time when the given segment was sealed.
     * If segment is never sealed this method returns an empty list.
     * If segment is yet to be created, this method still returns empty list.
     *
     * Find index that corresponds to segment start event.
     * Perform binary search on index+history records to find segment seal event.
     *
     * If index table is not up to date we may have two cases:
     * 1. Segment create time &gt; highest event time in index
     * 2. Segment seal time &gt; highest event time in index
     *
     * For 1 we cant have any searches in index and will need to fall through
     * History table starting from last indexed record.
     *
     * For 2, fall through History Table starting from last indexed record
     * to find segment sealed event in history table.
     *
     * @param segment      segment
     * @param historyIndex   index table
     * @param historyTable history table
     * @return List of successors for given segment
     */
    public static List<Integer> findSegmentSuccessorCandidates(
            final Segment segment,
            final byte[] historyIndex,
            final byte[] historyTable) {

        // find segment creation epoch.
        // find latest epoch from history table.
        final int creationEpoch = segment.getEpoch();
        final Optional<HistoryRecord> creationRecordOpt = HistoryRecord.readRecord(historyIndex, historyTable, creationEpoch, true);

        // segment information not in history table
        if (!creationRecordOpt.isPresent()) {
            return new ArrayList<>();
        }

        // take index of segment created event instead of searched index based on segment.startTime.
        final int lower = creationEpoch;

        final int upper = HistoryIndexRecord.readLatestRecord(historyIndex).map(HistoryIndexRecord::getEpoch).orElse(0);
        final HistoryRecord latest = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();

        if (latest.getSegments().contains(segment.getNumber())) {
            // Segment is not sealed yet so there cannot be a successor.
            return new ArrayList<>();
        } else {
            // segment is definitely sealed, we should be able to find it by doing binary search to find the respective epoch.
            return findSegmentSealedEvent(
                    lower,
                    upper,
                    segment.getNumber(),
                    historyIndex,
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
     * @param historyIndex   index table
     * @param historyTable history table
     * @return List of predecessor candidates.
     */
    public static List<Integer> findSegmentPredecessorCandidates(
            final Segment segment,
            final byte[] historyIndex,
            final byte[] historyTable) {
        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(historyIndex, historyTable, segment.getEpoch(), false);
        if (!historyRecordOpt.isPresent()) {
            // cant compute predecessors because the segment creation entry is not present in history table yet.
            return new ArrayList<>();
        }

        final HistoryRecord record = historyRecordOpt.get();

        final Optional<HistoryRecord> previous = HistoryRecord.fetchPrevious(record, historyIndex, historyTable);

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
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return pair of serialized segment index and segment table.
     */
    public static Pair<byte[], byte[]> createSegmentTableAndIndex(
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final long timeStamp) {
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        final ByteArrayOutputStream segmentIndex = new ByteArrayOutputStream();
        writeToSegmentTableAndIndex(0, 0, newRanges, timeStamp, segmentStream, segmentIndex);

        return new ImmutablePair<>(segmentStream.toByteArray(), segmentIndex.toByteArray());
    }

    /**
     * Add new segments to the segment table.
     * This method is designed to work with chunked creation. So it takes a
     * toCreate count and newRanges and it picks toCreate entries from the end of newranges.
     *
     * @param startingSegmentNumber starting segment number
     * @param newEpoch                 epoch in which segment is created
     * @param segmentTable          segment table
     * @param segmentIndex          segment index
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return pair of serialized segment index and segment table.
     */
    public static Pair<byte[], byte[]> addNewSegmentsToSegmentTableAndIndex(final int startingSegmentNumber,
                                                                            final int newEpoch,
                                                                            final byte[] segmentIndex,
                                                                            final byte[] segmentTable,
                                                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                                            final long timeStamp) {

        // if startingSegmentNumber was already previously indexed, overwrite the index
        int segmentIndexOffset = SegmentIndexRecord.readRecord(segmentIndex, startingSegmentNumber)
                .map(index -> index.getIndexOffset()).orElse(segmentIndex.length);
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();
        try {
            indexStream.write(segmentIndex, 0, segmentIndexOffset);
            segmentStream.write(segmentTable);
        } catch (IOException e) {
            throw Lombok.sneakyThrow(e);
        }
        writeToSegmentTableAndIndex(startingSegmentNumber, newEpoch, newRanges, timeStamp, segmentStream, indexStream);
        return new ImmutablePair<>(segmentStream.toByteArray(), indexStream.toByteArray());
    }

    private static void writeToSegmentTableAndIndex(int startingSegmentNumber, int newEpoch,
                                                    List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                    long timeStamp,
                                                    ByteArrayOutputStream segmentStream,
                                                    ByteArrayOutputStream indexStream) {
        try {
            IntStream.range(0, newRanges.size())
                    .forEach(
                            x -> {
                                try {
                                    int offset = segmentStream.size();
                                    segmentStream.write(new SegmentRecord(startingSegmentNumber + x,
                                            timeStamp, newEpoch, newRanges.get(x).getKey(), newRanges.get(x).getValue())
                                            .toByteArray());
                                    indexStream.write(new SegmentIndexRecord(startingSegmentNumber + x,
                                            offset).toByteArray());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                    );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add a new row to the history table. This row is only partial as it only contains list of segments.
     * Timestamp is added using completeHistoryRecord method.
     *
     * @param historyTable      history table
     * @param historyIndex      history index
     * @param newActiveSegments new active segments
     * @return serialized history table as byte array
     */
    public static byte[] addPartialRecordToHistoryTable(final byte[] historyIndex, final byte[] historyTable,
                                                        final List<Integer> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();
        Optional<HistoryRecord> last = HistoryRecord.readLatestRecord(historyIndex, historyTable, false);
        assert last.isPresent() && !(last.get().isPartial());

        try {
            historyStream.write(historyTable);
            HistoryRecord record = new HistoryRecord(last.get().getEpoch() + 1, newActiveSegments);
            historyStream.write(record.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Adds timestamp to the last record in the history table.
     *
     * @param historyTable         history table
     * @param historyIndex         history index
     * @param partialHistoryRecord partial history record
     * @param timestamp            scale timestamp
     * @return
     */
    public static byte[] completePartialRecordInHistoryTable(final byte[] historyIndex, final byte[] historyTable,
                                                             final HistoryRecord partialHistoryRecord,
                                                             final long timestamp) {
        Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyIndex, historyTable, false);
        assert record.isPresent() && record.get().isPartial() && record.get().getEpoch() == partialHistoryRecord.getEpoch();

        HistoryRecord previous = HistoryRecord.fetchPrevious(record.get(), historyIndex, historyTable).get();
        HistoryIndexRecord indexRecord = HistoryIndexRecord.readLatestRecord(historyIndex).get();
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        try {
            historyStream.write(historyTable, 0, indexRecord.getHistoryOffset());
            historyStream.write(new HistoryRecord(partialHistoryRecord.getEpoch(), partialHistoryRecord.getSegments(),
                    timestamp).toByteArray());
        } catch (Exception e) {
            throw Lombok.sneakyThrow(e);
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
            historyStream.write(new HistoryRecord(0, newActiveSegments, timestamp).toByteArray());
        } catch (Exception e) {
            throw Lombok.sneakyThrow(e);
        }
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param timestamp     timestamp
     * @param historyOffset history Offset
     * @return
     */
    public static byte[] createHistoryIndex(final long timestamp, final int historyOffset) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();

        try {
            indexStream.write(new HistoryIndexRecord(0, timestamp, historyOffset).toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return indexStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param historyIndex    index table
     * @param timestamp     timestamp
     * @param historyOffset history table offset
     * @return
     */
    public static byte[] updateHistoryIndex(final byte[] historyIndex,
                                            final long timestamp,
                                            final int historyOffset) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();
        int epoch = HistoryIndexRecord.readLatestRecord(historyIndex).get().getEpoch();
        try {
            indexStream.write(historyIndex);
            indexStream.write(new HistoryIndexRecord(epoch, timestamp, historyOffset).toByteArray());
        } catch (Exception e) {
            throw Lombok.sneakyThrow(e);
        }
        return indexStream.toByteArray();
    }

    /**
     * Method to check if a scale operation is currently ongoing and has created a new epoch (presence of partial record).
     * @param historyTable history table
     * @param historyIndex history index
     * @return true if a scale operation is ongoing, false otherwise
     */
    public static boolean isNewEpochCreated(final byte[] historyIndex, final byte[] historyTable) {
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        return latestHistoryRecord.isPartial();
    }

    /**
     * Method to check scale operation can be performed with given input.
     * @param segmentsToSeal segments to seal
     * @param historyTable history table
     * @param historyIndex history index
     * @return true if a scale operation can be performed, false otherwise
     */
    public static boolean canScaleFor(final List<Integer> segmentsToSeal, final byte[] historyIndex, final byte[] historyTable) {
        return getActiveEpoch(historyIndex, historyTable).getValue().containsAll(segmentsToSeal);
    }

    /**
     * Return the active epoch.
     * @param historyTable history table
     * @param historyIndex history index
     * @return active epoch
     */
    public static Pair<Integer, List<Integer>> getActiveEpoch(final byte[] historyIndex, final byte[] historyTable) {
        HistoryRecord historyRecord = HistoryRecord.readLatestRecord(historyIndex, historyTable, true).get();
        return new ImmutablePair<>(historyRecord.getEpoch(), historyRecord.getSegments());
    }

    /**
     * Return segments in the epoch.
     * @param historyTable history table
     * @param historyIndex history index
     * @param epoch            epoch
     *
     * @return segments in the epoch
     */
    public static List<Integer> getSegmentsInEpoch(final byte[] historyIndex, final byte[] historyTable, final int epoch) {
        Optional<HistoryRecord> record = HistoryRecord.readRecord(historyIndex, historyTable, epoch, false);

        return record.orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Epoch: " + epoch + " not found in history table")).getSegments();
    }

    /**
     * Return the active epoch.
     * @param historyTable history table
     * @param historyIndex history index
     * @return active epoch
     */
    public static HistoryRecord getLatestEpoch(byte[] historyIndex, byte[] historyTable) {
        return HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
    }

    /**
     * Method to compute segments created and deleted in latest scale event.
     *
     * @param historyTable history table
     * @param historyIndex history index
     * @return pair of segments sealed and segments created in last scale event.
     */
    public static Pair<List<Integer>, List<Integer>> getLatestScaleData(final byte[] historyIndex, final byte[] historyTable) {
        final Optional<HistoryRecord> current = HistoryRecord.readLatestRecord(historyIndex, historyTable, false);
        ImmutablePair<List<Integer>, List<Integer>> result;
        if (current.isPresent()) {
            final Optional<HistoryRecord> previous = HistoryRecord.fetchPrevious(current.get(), historyIndex, historyTable);
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

    /**
     * It finds the segment sealed event between lower and upper where 'lower' offset is guaranteed to be greater than or
     * equal to segment creation epoch
     * @param lowerEpoch starting record number in index table from where to search
     * @param upperEpoch last record number in index table till where to search
     * @param segmentNumber segment number to find sealed event
     * @param historyIndex index table
     * @param historyTable history table
     * @return returns history record where segment was sealed
     */
    private static Optional<HistoryRecord> findSegmentSealedEvent(final int lowerEpoch,
                                                                  final int upperEpoch,
                                                                  final int segmentNumber,
                                                                  final byte[] historyIndex,
                                                                  final byte[] historyTable) {
        if (lowerEpoch > upperEpoch || historyTable.length == 0) {
            return Optional.empty();
        }

        final int middle = (lowerEpoch + upperEpoch) / 2;

        final Optional<HistoryRecord> record = HistoryRecord.readRecord(historyIndex, historyTable, middle, false);
        assert record.isPresent();
        // if segment is not present in history record, check if it is present in previous
        // if yes, we have found the segment sealed event
        // else repeat binary searchIndex
        if (!record.get().getSegments().contains(segmentNumber)) {
            final Optional<HistoryRecord> previousRecord = HistoryRecord.readRecord(historyIndex, historyTable,
                    middle - 1, false);
            assert previousRecord.isPresent();
            if (previousRecord.get().getSegments().contains(segmentNumber)) {
                return record; // search complete
            } else { // binary search lower
                return findSegmentSealedEvent(lowerEpoch,
                        (lowerEpoch + upperEpoch) / 2 - 1,
                        segmentNumber,
                        historyIndex,
                        historyTable);
            }
        } else { // binary search upper
            // not sealed in the current location: look in second half
            return findSegmentSealedEvent((lowerEpoch + upperEpoch) / 2 + 1,
                    upperEpoch,
                    segmentNumber,
                    historyIndex,
                    historyTable);
        }
    }

    /**
     * Method to validate supplied scale input. It performs two checks
     * 1. if segments to seal are active in the current epoch.
     * 2. new ranges are identical to sealed ranges.
     * @param segmentsToSeal segments to seal
     * @param newRanges      new ranges to create
     * @param segmentTable   segment table
     * @param segmentIndex   segment index
     * @return true if scale input is valid, false otherwise.
     */
    public static boolean isScaleInputValid(final List<Integer> segmentsToSeal,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final byte[] segmentIndex,
                                            final byte[] segmentTable) {
        boolean newRangesPredicate = newRanges.stream().noneMatch(x -> x.getKey() >= x.getValue() &&
                x.getKey() >= 0 && x.getValue() > 0);

        List<AbstractMap.SimpleEntry<Double, Double>> oldRanges = segmentsToSeal.stream()
                .map(segment -> SegmentRecord.readRecord(segmentIndex, segmentTable, segment).map(x ->
                        new AbstractMap.SimpleEntry<>(x.getRoutingKeyStart(), x.getRoutingKeyEnd())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        return newRangesPredicate && reduce(oldRanges).equals(reduce(newRanges));
    }

    /**
     * Method to compute epoch transition record. It takes segments to seal and new ranges and all the tables and
     * computes the next epoch transition record.
     * @param historyIndex history index.
     * @param historyTable history table.
     * @param segmentIndex segment index
     * @param segmentTable segment table
     * @param segmentsToSeal segments to seal
     * @param newRanges new ranges
     * @param scaleTimestamp scale time
     * @return new epoch transition record based on supplied input
     */
    public static EpochTransitionRecord computeEpochTransition(byte[] historyIndex, byte[] historyTable, byte[] segmentIndex,
                                                               byte[] segmentTable, List<Integer> segmentsToSeal,
                                                               List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        Pair<Integer, List<Integer>> activeEpoch = getActiveEpoch(historyIndex, historyTable);
        Preconditions.checkState(activeEpoch.getValue().containsAll(segmentsToSeal), "Invalid epoch transition request");

        int newEpoch = activeEpoch.getKey() + 1;
        int segmentCount = getSegmentCount(segmentIndex, segmentTable);
        Map<Integer, AbstractMap.SimpleEntry<Double, Double>> newSegments = new HashMap<>();
        IntStream.range(0, newRanges.size()).forEach(x -> {
            newSegments.put(segmentCount + x, newRanges.get(x));
        });
        return new EpochTransitionRecord(activeEpoch.getKey(), newEpoch, scaleTimestamp, ImmutableSet.copyOf(segmentsToSeal),
                ImmutableMap.copyOf(newSegments));
    }

    private static Map<Segment, Integer> transform(byte[] segmentIndex, byte[] segmentTable, Map<Integer, Integer> epochStreamCutMap) {
        return epochStreamCutMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> getSegment(entry.getKey(), segmentIndex, segmentTable),
                        Map.Entry::getValue));
    }

    private static Map<Integer, Integer> computeEpochCutMap(byte[] historyIndex, byte[] historyTable, byte[] segmentIndex,
                                                            byte[] segmentTable, Map<Integer, Long> streamCut) {
        Map<Integer, Integer> epochStreamCutMap = new HashMap<>();

        int mostRecent = streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
        Segment mostRecentSegment = getSegment(mostRecent, segmentIndex, segmentTable);

        final Optional<HistoryRecord> highEpochRecord = HistoryRecord.readRecord(historyIndex, historyTable,
                mostRecentSegment.getEpoch(), false);

        List<Integer> toFind = new ArrayList<>(streamCut.keySet());
        Optional<HistoryRecord> epochRecord = highEpochRecord;

        while (epochRecord.isPresent() && !toFind.isEmpty()) {
            List<Integer> epochSegments = epochRecord.get().getSegments();
            Map<Boolean, List<Integer>> group = toFind.stream().collect(Collectors.groupingBy(epochSegments::contains));
            toFind = Optional.ofNullable(group.get(false)).orElse(Collections.emptyList());
            int epoch = epochRecord.get().getEpoch();
            List<Integer> found = Optional.ofNullable(group.get(true)).orElse(Collections.emptyList());
            found.forEach(x -> epochStreamCutMap.put(x, epoch));
            epochRecord = HistoryRecord.fetchPrevious(epochRecord.get(), historyIndex, historyTable);
        }

        return epochStreamCutMap;
    }

    private static Set<Integer> computeToDelete(Map<Segment, Integer> epochCutMap, byte[] historyIndex, byte[] historyTable,
                                                byte[] segmentIndex, byte[] segmentTable, Set<Integer> deletedSegments) {
        Set<Integer> toDelete = new HashSet<>();
        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);

        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(historyIndex, historyTable, 0, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            toDelete.addAll(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentIndex, segmentTable);
                // ignore already deleted segments from todelete
                // toDelete.add(epoch.segment overlaps cut.segment && epoch < cut.segment.epoch)
                return !deletedSegments.contains(epochSegmentNumber) &&
                        epochCutMap.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().getNumber() == epochSegment.getNumber() ||
                        (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).collect(Collectors.toSet()));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyIndex, historyTable, true);
        }
        return toDelete;
    }

    private static boolean greaterThan(Map<Segment, Integer> map1, Map<Segment, Integer> map2, Map<Integer, Long> cut1,
                                       Map<Integer, Long> cut2) {
        // find overlapping segments in map2 for all segments in map1
        // compare epochs. map1 should have epochs gt or eq its overlapping segments in map2
        return map1.entrySet().stream().allMatch(e1 ->
                map2.entrySet().stream().noneMatch(e2 ->
                        (e2.getKey().getNumber() == e1.getKey().getNumber() &&
                                cut1.get(e1.getKey().getNumber()) < cut2.get(e2.getKey().getNumber()))
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
