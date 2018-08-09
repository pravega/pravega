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
import io.pravega.common.Exceptions;
import io.pravega.common.util.ArrayView;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.Lombok;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getSegmentNumber;

/**
 * Helper class for operations pertaining to segment store tables (segment, history, index).
 * All the processing is done locally and this class does not make any network calls.
 * All methods are synchronous and blocking.
 */
public class TableHelper {
    /**
     * Segment Table records are indexed and and it is O(constant) operation to get segment offset given segmentIndex.
     *
     * @param segmentId    segment id
     * @param segmentIndex segment table index
     * @param segmentTable segment table
     * @param historyIndex history index
     * @param historyTable history table
     * @return Segment object
     */
    public static Segment getSegment(final long segmentId, final byte[] segmentIndex, final byte[] segmentTable,
                                     final byte[] historyIndex, final byte[] historyTable) {
        int segmentNumber = getSegmentNumber(segmentId);
        Optional<SegmentRecord> recordOpt = SegmentRecord.readRecord(segmentIndex, segmentTable, segmentNumber);
        if (recordOpt.isPresent()) {
            SegmentRecord record = recordOpt.get();
            long creationTime;
            int epoch = StreamSegmentNameUtils.getEpoch(segmentId);

            if (epoch == record.getCreationEpoch()) {
                creationTime = record.getStartTime();
            } else {
                Optional<HistoryRecord> historyRecord = HistoryRecord.readRecord(epoch, historyIndex, historyTable, false);
                if (!historyRecord.isPresent() || !historyRecord.get().getSegments().contains(segmentId)) {
                    // We only give out segments to clients once they have been written to history table. So any query for
                    // segment id that is not present in history table is invalid
                    throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "Segment : " + segmentId);
                }
                creationTime = historyRecord.get().getScaleTime();
            }

            return new Segment(segmentId,
                    creationTime,
                    record.getRoutingKeyStart(),
                    record.getRoutingKeyEnd());
        } else {
            throw StoreException.create(StoreException.Type.DATA_NOT_FOUND, "Segment : " + segmentId);
        }
    }

    public static SegmentRecord getLatestSegmentRecord(final byte[] segmentIndex, final byte[] segmentTable) {
        return SegmentRecord.readLatest(segmentIndex, segmentTable).get();
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
        final int startingSegmentNumber = getStartingSegmentNumber(segmentIndex);
        // The count should take into account both the segment number and the starting segment number for the stream.
        return segmentRecord.get().getSegmentNumber() - startingSegmentNumber + 1;
    }

    /**
     * Current active segments correspond to last "complete" entry in the history table.
     * Until segment number is written to the history table it is not exposed to outside world
     * (e.g. callers - producers and consumers)
     *
     * @param historyIndex history index
     * @param historyTable history table
     * @return list of active segment numbers in current active epoch. This ignores partial epochs if scale operation
     * is ongoing and returns the latest completed epoch.
     */
    public static List<Long> getActiveSegments(final byte[] historyIndex, final byte[] historyTable) {
        HistoryRecord record = getActiveEpoch(historyIndex, historyTable);
        return record.getSegments();
    }

    /**
     * Get active segments at given timestamp.
     * Perform binary search on index table to find the record corresponding to timestamp.
     * Once we find the segments, compare them to truncationRecord and take the more recent of the two.
     *
     * @param timestamp        timestamp
     * @param historyIndex     history index
     * @param historyTable     history table
     * @param segmentIndex     segment index
     * @param segmentTable     segment table
     * @param truncationRecord truncation record
     * @return list of active segments.
     */
    public static Map<Long, Long> getActiveSegments(final long timestamp, final byte[] historyIndex, final byte[] historyTable,
                                               final byte[] segmentIndex, final byte[] segmentTable,
                                               final StreamTruncationRecord truncationRecord) {
        final HistoryRecord record = findRecordInHistoryTable(timestamp, historyIndex, historyTable);

        Map<Long, Long> segments;
        if (truncationRecord == null) {
            segments = record.getSegments().stream().collect(Collectors.toMap(x -> x, x -> 0L));
        } else {
            // case 1: if record.epoch is before truncation, simply pick the truncation stream cut
            if (record.getEpoch() < truncationRecord.getTruncationEpochLow()) {
                segments = truncationRecord.getStreamCut();
            } else if (record.getEpoch() > truncationRecord.getTruncationEpochHigh()) {
                // case 2: if record.epoch is after truncation, simply use the record epoch
                segments = record.getSegments().stream().collect(Collectors.toMap(x -> x, 
                        x -> truncationRecord.getStreamCut().getOrDefault(x, 0L)));
            } else {
                // case 3: overlap between requested epoch and stream cut.
                // take segments from stream cut that are from or after this epoch.
                // take remaining segments from this epoch.
                segments = new HashMap<>();
                // all segments from stream cut that have epoch >= this epoch
                List<Long> fromStreamCut = truncationRecord.getCutEpochMap().entrySet().stream()
                        .filter(x -> x.getValue() >= record.getEpoch())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                // add segments from the truncation record with corresponding offsets 
                fromStreamCut.forEach(x -> segments.put(x, truncationRecord.getStreamCut().get(x)));
                
                // put remaining segments as those that dont overlap with ones taken from streamCut.
                // Note: we will use the head of these segments, basically offset = 0
                record.getSegments().stream().filter(x -> fromStreamCut.stream().noneMatch(y ->
                        getSegment(x, segmentIndex, segmentTable, historyIndex, historyTable)
                                .overlaps(getSegment(y, segmentIndex, segmentTable, historyIndex, historyTable))))
                        .forEach(x -> segments.put(x, 0L));
            }
        }
        return segments;
    }

    /**
     * Method to validate a given stream Cut.
     * A stream cut is valid if it covers the entire key space without any overlaps in ranges for segments that form the
     * streamcut. It throws {@link IllegalArgumentException} if the supplied stream cut does not satisfy the invariants.
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
     *
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
                                                                 final Map<Long, Long> streamCut,
                                                                 final StreamTruncationRecord previousTruncationRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(historyIndex);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(segmentIndex);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());

        Map<Long, Integer> epochCutMap = computeEpochCutMap(historyIndex, historyTable, segmentIndex, segmentTable, streamCut);
        Map<Segment, Integer> cutMapSegments = transform(segmentIndex, segmentTable, historyIndex, historyTable, epochCutMap);

        Map<Segment, Integer> previousCutMapSegment = transform(segmentIndex, segmentTable, historyIndex, historyTable, previousTruncationRecord.getCutEpochMap());

        Exceptions.checkArgument(greaterThan(cutMapSegments, previousCutMapSegment, streamCut, previousTruncationRecord.getStreamCut()),
                "streamCut", "stream cut has to be strictly ahead of previous stream cut");

        Set<Long> toDelete = computeToDelete(cutMapSegments, historyIndex, historyTable, segmentIndex, segmentTable,
                previousTruncationRecord.getDeletedSegments());
        return new StreamTruncationRecord(ImmutableMap.copyOf(streamCut), ImmutableMap.copyOf(epochCutMap),
                previousTruncationRecord.getDeletedSegments(), ImmutableSet.copyOf(toDelete), true);
    }

    /**
     * Method to find all segments between given from and to stream cuts.
     * @param historyIndex             history index
     * @param historyTable             history table
     * @param segmentIndex             segment index
     * @param segmentTable             segment table
     * @param from                     stream cut to truncate at.
     * @param to                       stream cut to truncate at.
     * @return returns segments that fall between given stream cuts
     */
    public static List<Segment> findSegmentsBetweenStreamCuts(final byte[] historyIndex, final byte[] historyTable,
                                                                 final byte[] segmentIndex, final byte[] segmentTable,
                                                                 final Map<Long, Long> from,
                                                                 final Map<Long, Long> to) {
        Preconditions.checkArgument(!(from.isEmpty() && to.isEmpty()));
        // 1. compute epoch cut map for from and to
        Map<Segment, Integer> fromEpochCutMap = from.isEmpty() ? Collections.emptyMap() :
                computeEpochCutMapWithSegment(historyIndex, historyTable, segmentIndex, segmentTable, from);
        Map<Segment, Integer> toEpochCutMap = to.isEmpty() ? Collections.emptyMap() :
                computeEpochCutMapWithSegment(historyIndex, historyTable, segmentIndex, segmentTable, to);
        Preconditions.checkArgument(greaterThan(toEpochCutMap, fromEpochCutMap, to, from));

        Set<Long> segments = new HashSet<>();

        // if from is empty, lower bound becomes lowest possible epoch = 0
        final int fromLowEpoch = fromEpochCutMap.values().stream().min(Comparator.naturalOrder()).orElse(0);
        final int fromHighEpoch = fromEpochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(0);

        // if to is empty, upper bound becomes highest epoch
        final int highestEpoch = HistoryRecord.readLatestRecord(historyIndex, historyTable, true).get().getEpoch();
        final int toLowEpoch = toEpochCutMap.values().stream().min(Comparator.naturalOrder()).orElse(highestEpoch);
        final int toHighEpoch = toEpochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(highestEpoch);

        // 2. loop from.lowestEpoch till to.highestEpoch
        for (int epoch = fromLowEpoch; epoch <= toHighEpoch; epoch++) {
            HistoryRecord epochRecord = HistoryRecord.readRecord(epoch, historyIndex, historyTable, false).get();

            // for epochs that cleanly lie between from.high and to.low epochs we can include all segments present in them
            // because they are guaranteed to be greater than `from` and less than `to` stream cuts.
            if (epoch >= fromHighEpoch && epoch <= toLowEpoch) {
                segments.addAll(epochRecord.getSegments());
            } else {
                // 3. for each segment in epoch.segments, find overlaps in from and to
                epochRecord.getSegments().stream().filter(x -> !segments.contains(x)).forEach(segmentId -> {
                    // 4. if segment.number >= from.segmentNumber && segment.number <= to.segmentNumber include segment.number
                    Segment epochSegment = getSegment(segmentId, segmentIndex, segmentTable, historyIndex, historyTable);
                    boolean greatThanFrom = fromEpochCutMap.keySet().stream().filter(x -> x.overlaps(epochSegment))
                            .allMatch(x -> x.segmentId() <= epochSegment.segmentId());
                    boolean lessThanTo = toEpochCutMap.keySet().stream().filter(x -> x.overlaps(epochSegment))
                            .allMatch(x -> epochSegment.segmentId() <= x.segmentId());
                    if (greatThanFrom && lessThanTo) {
                        segments.add(epochSegment.segmentId());
                    }
                });
            }
        }

        return segments.stream().map(segmentId -> getSegment(segmentId, segmentIndex, segmentTable, historyIndex, historyTable))
                .collect(Collectors.toList());
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
                                            final byte[] segmentTable, final Map<Long, Long> streamCut,
                                            final SealedSegmentsRecord sealedSegmentsRecord) {
        Preconditions.checkNotNull(streamCut);
        Preconditions.checkNotNull(historyIndex);
        Preconditions.checkNotNull(historyTable);
        Preconditions.checkNotNull(sealedSegmentsRecord);
        Preconditions.checkNotNull(segmentTable);
        Preconditions.checkArgument(!streamCut.isEmpty());
        Map<Segment, Integer> epochCutMap = computeEpochCutMapWithSegment(historyIndex, historyTable, segmentIndex, segmentTable, streamCut);
        AtomicLong size = new AtomicLong();
        Map<Long, Long> sealedSegmentSizeMap = sealedSegmentsRecord.getSealedSegmentsSizeMap();

        // add sizes for segments in stream cut
        streamCut.forEach((key, value) -> size.addAndGet(value));

        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(0, historyIndex, historyTable, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            size.addAndGet(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentIndex, segmentTable, historyIndex, historyTable);
                return epochCutMap.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().segmentId() == epochSegment.segmentId() ||
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
    public static List<Pair<Long, List<Long>>> getScaleMetadata(final byte[] historyIndex, final byte[] historyTable) {
        return HistoryRecord.readAllRecords(historyIndex, historyTable);
    }

    /**
     * Find segments from the candidate set that have overlapping key ranges with current segment.
     *
     * @param current    current segment number
     * @param candidates candidates for overlap
     * @return overlapping segments with current segment
     */
    public static List<Long> getOverlaps(
            final Segment current,
            final List<Segment> candidates) {
        return candidates.stream().filter(x -> x.overlaps(current)).map(Segment::segmentId).collect(Collectors.toList());
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
     * @param historyIndex   index table
     * @param historyTable history table
     * @return list of segments that belong to epoch where given segment was sealed
     */
    public static List<Long> findSegmentSuccessorCandidates(
            final Segment segment,
            final byte[] historyIndex,
            final byte[] historyTable) {
        // find segment creation epoch.
        // find latest epoch from history table.
        final int creationEpoch = segment.getEpoch();
        final Optional<HistoryRecord> creationRecordOpt = HistoryRecord.readRecord(creationEpoch, historyIndex,
                historyTable, true);

        // segment information not in history table
        if (!creationRecordOpt.isPresent()) {
            return Collections.emptyList();
        }

        final HistoryRecord latest = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();

        if (latest.getSegments().contains(segment.segmentId())) {
            // Segment is not sealed yet so there cannot be a successor.
            return Collections.emptyList();
        } else {
            // segment is definitely sealed, we should be able to find it by doing binary search to find the respective epoch.
            return findSegmentSealedEvent(
                    creationEpoch,
                    latest.getEpoch(),
                    segment.segmentId(),
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
    public static List<Long> findSegmentPredecessorCandidates(
            final Segment segment,
            final byte[] historyIndex,
            final byte[] historyTable) {
        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(segment.getEpoch(), historyIndex, historyTable, false);
        if (!historyRecordOpt.isPresent()) {
            // cant compute predecessors because the segment creation entry is not present in history table yet.
            return Collections.emptyList();
        }

        final HistoryRecord record = historyRecordOpt.get();

        final Optional<HistoryRecord> previous = HistoryRecord.fetchPrevious(record, historyIndex, historyTable);

        if (!previous.isPresent()) {
            return Collections.emptyList();
        } else {
            assert !previous.get().getSegments().contains(segment.segmentId());
            return previous.get().getSegments();
        }
    }

    /**
     * Add new segments to the segment table.
     * It takes the newRanges and creates corresponding entries in segment table.
     *
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @param startingSegmentNumber starting segment number for the stream.
     * @return pair of serialized segment index and segment table.
     */
    public static Pair<byte[], byte[]> createSegmentTableAndIndex(final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                                  final long timeStamp, final int startingSegmentNumber) {
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        final ByteArrayOutputStream segmentIndex = new ByteArrayOutputStream();
        writeToSegmentTableAndIndex(startingSegmentNumber, 0, newRanges, timeStamp, segmentStream, segmentIndex);

        return new ImmutablePair<>(segmentIndex.toByteArray(), segmentStream.toByteArray());
    }

    /**
     * Add new segments to the segment table.
     * It takes starting segment number and newRanges and it computes toCreate entries from the end of newranges.
     *
     * @param startingSegmentNumber starting segment number
     * @param newEpoch                 epoch in which segment is created
     * @param segmentTable          segment table
     * @param segmentIndex          segment index
     * @param newRanges             ranges
     * @param timeStamp             timestamp
     * @return pair of serialized segment index and segment table.
     */
    @SneakyThrows
    public static Pair<byte[], byte[]> addNewSegmentsToSegmentTableAndIndex(final int startingSegmentNumber,
                                                                            final int newEpoch,
                                                                            final byte[] segmentIndex,
                                                                            final byte[] segmentTable,
                                                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                                            final long timeStamp) {
        // if startingSegmentNumber was already previously indexed, overwrite the index
        int segmentIndexOffset = SegmentIndexRecord.readRecord(segmentIndex, startingSegmentNumber)
                .map(SegmentIndexRecord::getIndexOffset).orElse(segmentIndex.length);
        final ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();
        indexStream.write(segmentIndex, 0, segmentIndexOffset);
        segmentStream.write(segmentTable);
        writeToSegmentTableAndIndex(startingSegmentNumber, newEpoch, newRanges, timeStamp, segmentStream, indexStream);
        return new ImmutablePair<>(indexStream.toByteArray(), segmentStream.toByteArray());
    }

    @SneakyThrows
    private static void writeToSegmentTableAndIndex(int startingSegmentNumber, int newEpoch,
                                                    List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                    long timeStamp,
                                                    ByteArrayOutputStream segmentStream,
                                                    ByteArrayOutputStream indexStream) {
        IntStream.range(0, newRanges.size())
                .forEach(
                        x -> {
                            try {
                                int offset = segmentStream.size();
                                ArrayView arrayView = new SegmentRecord(startingSegmentNumber + x,
                                        timeStamp, newEpoch, newRanges.get(x).getKey(), newRanges.get(x).getValue())
                                        .toArrayView();
                                segmentStream.write(arrayView.array(), arrayView.arrayOffset(), arrayView.getLength());

                                // If we create a new segment index, its first element is the startingSegmentNumber for
                                // this stream. This avoids storing this value in a separate metadata item.
                                if (indexStream.size() == 0) {
                                    indexStream.write(ByteBuffer.allocate(SegmentIndexRecord.INDEX_RECORD_SIZE).putInt(startingSegmentNumber).array());
                                }

                                indexStream.write(new SegmentIndexRecord(startingSegmentNumber + x, offset, startingSegmentNumber).toByteArray());
                            } catch (IOException e) {
                                throw Lombok.sneakyThrow(e);
                            }
                        }
                );
    }

    /**
     * This method is used in rolling transaction workflow. It is used to create duplicate epochs in the History Table.
     * It takes a reference epoch, seals the active epoch, creates a duplicate entry for referenced epoch.
     * It then follows it up with creating a partial duplicate entry for active epoch.
     *
     * @param historyIndex      history index
     * @param historyTable      history table
     * @param referenceEpoch    reference epoch to duplicate
     * @param timestamp         timestamp to add to duplicate of the referenced epoch
     * @return serialized history index and table as byte array
     */
    @SneakyThrows
    public static Pair<byte[], byte[]> insertDuplicateRecordsInHistoryTable(final byte[] historyIndex, final byte[] historyTable,
                                                           int referenceEpoch, long timestamp) {
        HistoryRecord activeRecord = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        int nextEpoch = activeRecord.getEpoch() + 1;
        // if nextepoch was already previously indexed, overwrite the index
        int startingOffset = HistoryIndexRecord.readRecord(historyIndex, nextEpoch)
                .map(index -> index.getIndexOffset()).orElse(historyIndex.length);

        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();
        indexStream.write(historyIndex, 0, startingOffset);

        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();
        historyStream.write(historyTable);
        int historyOffset = historyTable.length;

        // duplicate the reference epoch
        HistoryRecord referenceRecord = HistoryRecord.readRecord(referenceEpoch, historyIndex, historyTable, true).get();
        List<Long> newSegments = referenceRecord.getSegments().stream().map(segmentId -> computeSegmentId(getSegmentNumber(segmentId),
                nextEpoch)).collect(Collectors.toList());
        HistoryRecord record = new HistoryRecord(nextEpoch, referenceRecord.getReferenceEpoch(), newSegments, timestamp);
        ArrayView arrayView = record.toArrayView();
        // add to history table and index
        indexStream.write(new HistoryIndexRecord(nextEpoch, historyOffset).toByteArray());
        historyStream.write(arrayView.array(), arrayView.arrayOffset(), arrayView.getLength());

        historyOffset = historyStream.size();

        // duplicate active epoch as a partial record. no time added here
        int newEpoch = nextEpoch + 1;
        newSegments = activeRecord.getSegments().stream().map(segmentId -> computeSegmentId(getSegmentNumber(segmentId), newEpoch)).collect(Collectors.toList());
        record = new HistoryRecord(newEpoch, activeRecord.getReferenceEpoch(), newSegments);
        arrayView = record.toArrayView();
        historyStream.write(arrayView.array(), arrayView.arrayOffset(), arrayView.getLength());
        indexStream.write(new HistoryIndexRecord(newEpoch, historyOffset).toByteArray());

        byte[] updatedHistoryTable = historyStream.toByteArray();
        byte[] updatedIndex = indexStream.toByteArray();
        return new ImmutablePair<>(updatedIndex, updatedHistoryTable);
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
    @SneakyThrows
    public static byte[] addPartialRecordToHistoryTable(final byte[] historyIndex, final byte[] historyTable,
                                                        final List<Long> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();
        Optional<HistoryRecord> last = HistoryRecord.readLatestRecord(historyIndex, historyTable, false);
        assert last.isPresent() && !(last.get().isPartial());

        historyStream.write(historyTable);
        HistoryRecord record = new HistoryRecord(last.get().getEpoch() + 1, newActiveSegments);
        ArrayView arrayView = record.toArrayView();
        historyStream.write(arrayView.array(), arrayView.arrayOffset(), arrayView.getLength());
        return historyStream.toByteArray();
    }

    /**
     * Adds timestamp to the last record in the history table.
     *
     * @param historyTable         history table
     * @param historyIndex         history index
     * @param partialHistoryRecord partial history record
     * @param timestamp            scale timestamp
     * @return serialized updated history table
     */
    @SneakyThrows
    public static byte[] completePartialRecordInHistoryTable(final byte[] historyIndex, final byte[] historyTable,
                                                             final HistoryRecord partialHistoryRecord,
                                                             final long timestamp) {
        Optional<HistoryRecord> record = HistoryRecord.readLatestRecord(historyIndex, historyTable, false);
        assert record.isPresent() && record.get().isPartial() && record.get().getEpoch() == partialHistoryRecord.getEpoch();

        HistoryIndexRecord indexRecord = HistoryIndexRecord.readLatestRecord(historyIndex).get();
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        historyStream.write(historyTable, 0, indexRecord.getHistoryOffset());
        HistoryRecord historyRecord = new HistoryRecord(partialHistoryRecord.getEpoch(), partialHistoryRecord.getReferenceEpoch(),
                partialHistoryRecord.getSegments(), timestamp);
        ArrayView arrayView = historyRecord.toArrayView();
        historyStream.write(arrayView.array(), arrayView.arrayOffset(), arrayView.getLength());
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
                                            final List<Long> newActiveSegments) {
        final ByteArrayOutputStream historyStream = new ByteArrayOutputStream();

        ArrayView arrayView = new HistoryRecord(0, newActiveSegments, timestamp).toArrayView();
        historyStream.write(arrayView.array(), arrayView.arrayOffset(), arrayView.getLength());
        return historyStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @return serialized index table
     */
    @SneakyThrows
    public static byte[] createHistoryIndex() {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();

        indexStream.write(new HistoryIndexRecord(0, 0).toByteArray());
        return indexStream.toByteArray();
    }

    /**
     * Add a new row to index table.
     *
     * @param historyIndex    index table
     * @param historyOffset history table offset
     * @return serialized index table
     */
    @SneakyThrows
    public static byte[] updateHistoryIndex(final byte[] historyIndex,
                                            final int historyOffset) {
        final ByteArrayOutputStream indexStream = new ByteArrayOutputStream();
        int epoch = HistoryIndexRecord.readLatestRecord(historyIndex).get().getEpoch();
        indexStream.write(historyIndex);
        indexStream.write(new HistoryIndexRecord(epoch, historyOffset).toByteArray());
        return indexStream.toByteArray();
    }

    /**
     * Method to check scale operation can be performed with given input.
     * @param segmentsToSeal segments to seal
     * @param historyTable history table
     * @param historyIndex history index
     * @return true if a scale operation can be performed, false otherwise
     */
    public static boolean canScaleFor(final List<Long> segmentsToSeal, final byte[] historyIndex, final byte[] historyTable) {
        return getActiveEpoch(historyIndex, historyTable).getSegments().containsAll(segmentsToSeal);
    }

    /**
     * Method that looks at the supplied epoch transition record and compares it with partial state in metadata store to determine
     * if the partial state corresponds to supplied input.
     *
     * @param epochTransitionRecord epoch transition record
     * @param historyIndex history index
     * @param historyTable history table
     * @param segmentIndex segment index
     * @param segmentTable segment table
     * @return true if input matches partial state, false otherwise
     */
    public static boolean isEpochTransitionConsistent(final EpochTransitionRecord epochTransitionRecord,
                                                      final byte[] historyIndex,
                                                      final byte[] historyTable,
                                                      final byte[] segmentIndex,
                                                      final byte[] segmentTable) {
        AtomicBoolean isConsistent = new AtomicBoolean(true);
        SegmentRecord latest = SegmentRecord.readLatest(segmentIndex, segmentTable).get();
        // verify that epoch transition record is consistent with segment table
        if (latest.getCreationEpoch() == epochTransitionRecord.getNewEpoch()) { // if segment table is updated
            epochTransitionRecord.newSegmentsWithRange.entrySet().forEach(segmentWithRange -> {
                Optional<SegmentRecord> segmentOpt = SegmentRecord.readRecord(segmentIndex, segmentTable, getSegmentNumber(segmentWithRange.getKey()));
                isConsistent.compareAndSet(true, segmentOpt.isPresent() &&
                        segmentOpt.get().getCreationEpoch() == epochTransitionRecord.getNewEpoch() &&
                        segmentOpt.get().getRoutingKeyStart() == segmentWithRange.getValue().getKey() &&
                        segmentOpt.get().getRoutingKeyEnd() == segmentWithRange.getValue().getValue());
            });
        } else { // if segment table is not updated
            isConsistent.compareAndSet(true, latest.getCreationEpoch() == epochTransitionRecord.getActiveEpoch());
        }

        // verify that epoch transition record is consistent with history table
        HistoryRecord latestHistoryRecord = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        // if history table is not updated
        if (latestHistoryRecord.getEpoch() == epochTransitionRecord.activeEpoch) {
            isConsistent.compareAndSet(true,
                    !latestHistoryRecord.isPartial() &&
                            latestHistoryRecord.getSegments().containsAll(epochTransitionRecord.segmentsToSeal));
        } else if (latestHistoryRecord.getEpoch() == epochTransitionRecord.getNewEpoch()) {
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
     * Return the active epoch. The active epoch is the one whose segments are not sealed yet.
     * It is typically the latest epoch. However, if the latest epoch is "partial" then it is one of the previous epochs.
     * During scale we only add one new epoch at a time, so active epoch becomes epoch that is previous to partial epoch.
     * However, during commit, it will always be behind by two.
     *
     * @param historyTable history table
     * @param historyIndex history index
     * @return active epoch
     */
    public static HistoryRecord getActiveEpoch(final byte[] historyIndex, final byte[] historyTable) {
        HistoryRecord historyRecord = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        // if the record is created as a sealed epoch, the previous epoch may not have been sealed yet and hence that will be active.
        if (historyRecord.isPartial()) {
            // Scale creates new fresh epochs. So if latest epoch is partial and a duplicate, then its previous epoch
            // is created by rolling txn workflow.
            // So for partial epochs created by scale, go back one epoch from latest to get active epoch. For rolling txn
            // go back by two epochs.
            if (historyRecord.isDuplicate()) {
                historyRecord = HistoryRecord.readRecord(historyRecord.getEpoch() - 2, historyIndex, historyTable, true).get();
            } else {
                historyRecord = HistoryRecord.fetchPrevious(historyRecord, historyIndex, historyTable).get();
            }
        }

        return historyRecord;
    }

    /**
     * Return segments in the epoch.
     *
     * @param historyTable history table
     * @param historyIndex history index
     * @param epoch            epoch
     *
     * @return segments in the epoch
     */
    public static List<Long> getSegmentsInEpoch(final byte[] historyIndex, final byte[] historyTable, final int epoch) {
        Optional<HistoryRecord> record = HistoryRecord.readRecord(epoch, historyIndex, historyTable, false);

        return record.orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Epoch: " + epoch + " not found in history table")).getSegments();
    }

    /**
     * Method to fetch history record corresponding to the given epoch.
     *
     * @param historyTable history table
     * @param historyIndex history index
     * @param epoch            epoch
     *
     * @return History record corresponding to the epoch
     */
    public static HistoryRecord getEpochRecord(final byte[] historyIndex, final byte[] historyTable, final int epoch) {
        Optional<HistoryRecord> record = HistoryRecord.readRecord(epoch, historyIndex, historyTable, false);

        return record.orElseThrow(() -> StoreException.create(StoreException.Type.DATA_NOT_FOUND,
                "Epoch: " + epoch + " not found in history table"));
    }

    private static List<Long> diff(List<Long> list1, List<Long> list2) {
        return list1.stream().filter(z -> !list2.contains(z)).collect(Collectors.toList());
    }

    private static HistoryRecord findRecordInHistoryTable(long timestamp, byte[] historyIndex, byte[] historyTable) {
        int latestEpoch = HistoryIndexRecord.readLatestRecord(historyIndex).get().getEpoch();

        final Optional<HistoryRecord> historyRecord = binarySearchHistory(0, latestEpoch, timestamp, historyIndex,
                historyTable);
        return historyRecord.orElseGet(() -> HistoryRecord.readRecord(0, historyIndex, historyTable, true).get());
    }

    private static Optional<HistoryRecord> binarySearchHistory(final int lowerEpoch,
                                                   final int upperEpoch,
                                                   final long timestamp,
                                                   final byte[] historyIndex,
                                                   final byte[] historyTable) {
        if (upperEpoch < lowerEpoch) {
            return Optional.empty();
        }

        final int middle = (lowerEpoch + upperEpoch) / 2;

        final Optional<HistoryRecord> record = HistoryRecord.readRecord(middle, historyIndex, historyTable, false);

        if (record.get().getScaleTime() <= timestamp) {
            Optional<HistoryRecord> next = HistoryRecord.fetchNext(record.get(), historyIndex, historyTable, false);
            if (!next.isPresent() || (next.get().getScaleTime() > timestamp)) {
                return record;
            } else {
                return binarySearchHistory(middle + 1, upperEpoch, timestamp,
                        historyIndex, historyTable);
            }
        } else {
            return binarySearchHistory(lowerEpoch, middle - 1, timestamp, historyIndex, historyTable);
        }
    }

    /**
     * It finds the segment sealed event between lower and upper where 'lower' offset is guaranteed to be greater than or
     * equal to segment creation epoch
     * @param lowerEpoch starting record number in index table from where to search
     * @param upperEpoch last record number in index table till where to search
     * @param segmentId segment number to find sealed event
     * @param historyIndex index table
     * @param historyTable history table
     * @return returns history record where segment was sealed
     */
    private static Optional<HistoryRecord> findSegmentSealedEvent(final int lowerEpoch,
                                                                  final int upperEpoch,
                                                                  final long segmentId,
                                                                  final byte[] historyIndex,
                                                                  final byte[] historyTable) {
        if (lowerEpoch > upperEpoch || historyTable.length == 0) {
            return Optional.empty();
        }

        final int middle = (lowerEpoch + upperEpoch) / 2;

        final Optional<HistoryRecord> record = HistoryRecord.readRecord(middle, historyIndex, historyTable, false);
        assert record.isPresent();
        // if segment is not present in history record, check if it is present in previous
        // if yes, we have found the segment sealed event
        // else repeat binary searchIndex
        if (!record.get().getSegments().contains(segmentId)) {
            final Optional<HistoryRecord> previousRecord = HistoryRecord.readRecord(middle - 1, historyIndex, historyTable,
                    false);
            assert previousRecord.isPresent();
            if (previousRecord.get().getSegments().contains(segmentId)) {
                return record; // search complete
            } else { // binary search lower
                return findSegmentSealedEvent(lowerEpoch,
                        (lowerEpoch + upperEpoch) / 2 - 1,
                        segmentId,
                        historyIndex,
                        historyTable);
            }
        } else { // binary search upper
            // not sealed in the current location: look in second half
            return findSegmentSealedEvent((lowerEpoch + upperEpoch) / 2 + 1,
                    upperEpoch,
                    segmentId,
                    historyIndex,
                    historyTable);
        }
    }

    /**
     * Method to validate supplied scale input. It performs a check that new ranges are identical to sealed ranges.
     *
     * @param segmentsToSeal segments to seal
     * @param newRanges      new ranges to create
     * @param segmentTable   segment table
     * @param segmentIndex   segment index
     * @return true if scale input is valid, false otherwise.
     */
    public static boolean isScaleInputValid(final List<Long> segmentsToSeal,
                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                            final byte[] segmentIndex,
                                            final byte[] segmentTable) {
        boolean newRangesPredicate = newRanges.stream().noneMatch(x -> x.getKey() >= x.getValue() &&
                x.getKey() >= 0 && x.getValue() > 0);

        List<AbstractMap.SimpleEntry<Double, Double>> oldRanges = segmentsToSeal.stream()
                .map(segmentId -> SegmentRecord.readRecord(segmentIndex, segmentTable, getSegmentNumber(segmentId)).map(x ->
                        new AbstractMap.SimpleEntry<>(x.getRoutingKeyStart(), x.getRoutingKeyEnd())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        return newRangesPredicate && reduce(oldRanges).equals(reduce(newRanges));
    }

    /**
     * Method to transform epoch cut map with segment id to epoch cut map with Segment object.
     */
    private static Map<Segment, Integer> transform(byte[] segmentIndex, byte[] segmentTable, byte[] historyIndex, byte[] historyTable, Map<Long, Integer> epochStreamCutMap) {
        return epochStreamCutMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> getSegment(entry.getKey(), segmentIndex, segmentTable, historyIndex, historyTable),
                        Map.Entry::getValue));
    }

    /**
     * Method to compute epoch transition record. It takes segments to seal and new ranges and all the tables and
     * computes the next epoch transition record.
     * @param historyIndex history index
     * @param historyTable history table
     * @param segmentIndex segment index
     * @param segmentTable segment table
     * @param segmentsToSeal segments to seal
     * @param newRanges new ranges
     * @param scaleTimestamp scale time
     * @return new epoch transition record based on supplied input
     */
    public static EpochTransitionRecord computeEpochTransition(byte[] historyIndex, byte[] historyTable, byte[] segmentIndex,
                                                               byte[] segmentTable, List<Long> segmentsToSeal,
                                                               List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                               long scaleTimestamp) {
        HistoryRecord activeEpoch = getActiveEpoch(historyIndex, historyTable);
        Preconditions.checkState(activeEpoch.getSegments().containsAll(segmentsToSeal), "Invalid epoch transition request");

        int newEpoch = activeEpoch.getEpoch() + 1;
        int segmentCount = getSegmentCount(segmentIndex, segmentTable);
        Map<Long, AbstractMap.SimpleEntry<Double, Double>> newSegments = new HashMap<>();
        final int startingSegmentNumber = getStartingSegmentNumber(segmentIndex);
        IntStream.range(0, newRanges.size()).forEach(x ->
            newSegments.put(computeSegmentId(startingSegmentNumber + segmentCount + x, newEpoch), newRanges.get(x)));
        return new EpochTransitionRecord(activeEpoch.getEpoch(), scaleTimestamp, ImmutableSet.copyOf(segmentsToSeal),
                ImmutableMap.copyOf(newSegments));
    }

    private static Map<Segment, Integer> computeEpochCutMapWithSegment(byte[] historyIndex, byte[] historyTable, byte[] segmentIndex,
                                                                       byte[] segmentTable, Map<Long, Long> streamCut) {
        Map<Long, Integer> epochCutMap = computeEpochCutMap(historyIndex, historyTable, segmentIndex, segmentTable, streamCut);
        return transform(segmentIndex, segmentTable, historyIndex, historyTable, epochCutMap);
    }

    /**
     * If a stream cut spans across multiple epochs then this map captures mapping of segments from the stream cut to
     * epochs they were found in closest to truncation point.
     * This data structure is used to find active segments wrt a stream cut.
     * So for example:
     * epoch 0: 0, 1
     * epoch 1: 0, 2, 3
     * epoch 2: 0, 2, 4, 5
     * epoch 3: 0, 4, 5, 6, 7
     *
     * Following is a valid stream cut {0/offset, 3/offset, 6/offset, 7/offset}
     * This spans from epoch 1 till epoch 3. Any request for segments at epoch 1 or 2 or 3 will need to have this stream cut
     * applied on it to find segments that are available for consumption.
     *
     * This method takes a stream cut and maps it to highest epochs per segment in the stream cut.
     * So in the above example, the map produced for {0/offset, 3/offset, 6/offset, 7/offset} will be
     * {0/3, 3/1, 6/3, 7/3}
     */
    private static Map<Long, Integer> computeEpochCutMap(byte[] historyIndex, byte[] historyTable, byte[] segmentIndex,
                                                                              byte[] segmentTable, Map<Long, Long> streamCut) {
        Map<Long, Integer> epochStreamCutMap = new HashMap<>();

        Long mostRecent = streamCut.keySet().stream().max(Comparator.naturalOrder()).get();
        Segment mostRecentSegment = getSegment(mostRecent, segmentIndex, segmentTable, historyIndex, historyTable);

        final Optional<HistoryRecord> highEpochRecord = HistoryRecord.readRecord(mostRecentSegment.getEpoch(), historyIndex,
                historyTable, false);

        List<Long> toFind = new ArrayList<>(streamCut.keySet());
        Optional<HistoryRecord> epochRecord = highEpochRecord;

        while (epochRecord.isPresent() && !toFind.isEmpty()) {
            List<Long> epochSegments = epochRecord.get().getSegments();
            Map<Boolean, List<Long>> group = toFind.stream().collect(Collectors.groupingBy(epochSegments::contains));
            toFind = Optional.ofNullable(group.get(false)).orElse(Collections.emptyList());
            int epoch = epochRecord.get().getEpoch();
            List<Long> found = Optional.ofNullable(group.get(true)).orElse(Collections.emptyList());
            found.forEach(x -> epochStreamCutMap.put(x, epoch));
            epochRecord = HistoryRecord.fetchPrevious(epochRecord.get(), historyIndex, historyTable);
        }

        return epochStreamCutMap;
    }

    private static Set<Long> computeToDelete(Map<Segment, Integer> epochCutMap, byte[] historyIndex, byte[] historyTable,
                                                                  byte[] segmentIndex, byte[] segmentTable, Set<Long> deletedSegments) {
        Set<Long> toDelete = new HashSet<>();
        int highestEpoch = epochCutMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);

        Optional<HistoryRecord> historyRecordOpt = HistoryRecord.readRecord(0, historyIndex, historyTable, true);

        // start with epoch 0 and go all the way upto epochCutMap.highEpoch
        while (historyRecordOpt.isPresent() && historyRecordOpt.get().getEpoch() <= highestEpoch) {
            HistoryRecord historyRecord = historyRecordOpt.get();
            int epoch = historyRecord.getEpoch();

            toDelete.addAll(historyRecord.getSegments().stream().filter(epochSegmentNumber -> {
                Segment epochSegment = getSegment(epochSegmentNumber, segmentIndex, segmentTable, historyIndex, historyTable);
                // ignore already deleted segments from todelete
                // toDelete.add(epoch.segment overlaps cut.segment && epoch < cut.segment.epoch)
                return !deletedSegments.contains(epochSegmentNumber) &&
                        epochCutMap.entrySet().stream().noneMatch(cutSegment -> cutSegment.getKey().segmentId() == epochSegment.segmentId() ||
                                (cutSegment.getKey().overlaps(epochSegment) && cutSegment.getValue() <= epoch));
            }).collect(Collectors.toSet()));
            historyRecordOpt = HistoryRecord.fetchNext(historyRecord, historyIndex, historyTable, true);
        }
        return toDelete;
    }

    private static boolean greaterThan(Map<Segment, Integer> map1, Map<Segment, Integer> map2, Map<Long, Long> cut1,
                                       Map<Long, Long> cut2) {
        // find overlapping segments in map2 for all segments in map1
        // compare epochs. map1 should have epochs gt or eq its overlapping segments in map2
        return map1.entrySet().stream().allMatch(e1 ->
                map2.entrySet().stream().noneMatch(e2 ->
                        (e2.getKey().segmentId() == e1.getKey().segmentId() &&
                                cut1.get(e1.getKey().segmentId()) < cut2.get(e2.getKey().segmentId()))
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

    public static long getEpochScaleTime(int epoch, byte[] historyIndex, byte[] historyTable) {
        return HistoryRecord.readRecord(epoch, historyIndex, historyTable, true)
                .map(HistoryRecord::getScaleTime).orElse(0L);
    }

    public static int getTransactionEpoch(UUID txId) {
        // epoch == UUID.msb >> 32
        return (int) (txId.getMostSignificantBits() >> 32);
    }

    /**
     * This method takes a segment id and replaces its epoch with the epoch in the transaction.
     *
     * @param segmentId segment id
     * @param txId transaction id
     * @return new segment id which uses transaction's epoch.
     */
    public static long generalizedSegmentId(long segmentId, UUID txId) {
        return computeSegmentId(getSegmentNumber(segmentId), getTransactionEpoch(txId));
    }

    /**
     * This method provides the starting segment number for this stream that is stored in the first position of the
     * segment index.
     *
     * @param segmentIndex segment index
     * @return starting segment number for this stream.
     */
    public static int getStartingSegmentNumber(byte[] segmentIndex) {
        return SegmentIndexRecord.getStartingSegmentNumberFromIndex(segmentIndex);
    }
}
