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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.util.ArrayView;
import io.pravega.controller.store.stream.tables.serializers.HistoryRecordSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Class corresponding to one row in the HistoryTable.
 * HistoryRecords are of variable length, so we will use history index for
 * traversal.
 * Row : [epoch][List-of-active-segment-numbers], [scaleTime]
 */
@Data
public class HistoryRecord {
    public static final HistoryRecordSerializer SERIALIZER = new HistoryRecordSerializer();

    @Getter
    private final int epoch;
    /**
     * This refers to epoch whose duplicate this epoch HistoryRecord may be.
     * If referenceEpoch is same as epoch, then this is a clean creation of epoch rather than a duplicate.
     * If we are creating a duplicate of an epoch that was already a duplicate, we set the reference to the parent.
     * This ensures that instead of having a chain of duplicates we have a tree of duplicates with original epoch as
     * common parent and all duplicates referencing the parent epoch.
     */
    @Getter
    private final int referenceEpoch;

    /**
     * Segment ids have two parts, primary id and secondary id.
     * Primary Id is encoded in LSB of each long and secondary id is encoded in MSB.
     * Note: secondary id is optional and 0 value will signify its absence.
     */
    @Getter
    private final List<Long> segments;
    @Getter
    private final long scaleTime;
    @Getter
    private final boolean partial;

    @Builder
    HistoryRecord(int epoch, int referenceEpoch, List<Long> segments, long creationTime) {
        this.epoch = epoch;
        this.referenceEpoch = referenceEpoch;
        this.segments = segments;
        this.scaleTime = creationTime;
        partial = creationTime == Long.MIN_VALUE;
    }

    @Builder
    HistoryRecord(int epoch, List<Long> segments, long creationTime) {
        this(epoch, epoch, segments, creationTime);
    }

    @Builder
    HistoryRecord(int epoch, List<Long> segments) {
        this(epoch, segments, Long.MIN_VALUE);
    }

    @Builder
    HistoryRecord(int epoch, int referenceEpoch, List<Long> segments) {
        this(epoch, referenceEpoch, segments, Long.MIN_VALUE);
    }

    @SneakyThrows(IOException.class)
    public ArrayView toArrayView() {
        return SERIALIZER.serialize(this);
    }

    public boolean isDuplicate() {
        return epoch != referenceEpoch;
    }

    /**
     * Read record from the history table at the specified offset.
     *
     * @param epoch         epoch to read
     * @param historyTable  history table
     * @param historyIndex  history index
     * @param ignorePartial if set, ignore if the record is partial
     * @return Optional of history record for the epoch if it exists in the table predicated to ignore partial flag.
     */
    public static Optional<HistoryRecord> readRecord(final int epoch, final byte[] historyIndex, final byte[] historyTable,
                                                     boolean ignorePartial) {
        Optional<HistoryIndexRecord> historyIndexRecord = HistoryIndexRecord.readRecord(historyIndex, epoch);

        return historyIndexRecord.map(index -> {
            int offset = index.getHistoryOffset();

            if (offset >= historyTable.length) {
                // Note: index could be ahead of history table.
                return null;
            } else {
                HistoryRecord record = parse(historyTable, offset);

                if (record.isPartial() && ignorePartial) {
                    // this is a partial record and we have been asked to ignore it.
                    return null;
                } else {
                    return record;
                }
            }
        });
    }

    /**
     * Return latest record in the history table.
     *
     * @param historyTable  history table
     * @param historyIndex  history index
     * @param ignorePartial Ignore partial entry.
     * @return returns the latest history record. If latest entry is partial entry and ignorePartial flag is true
     * then read the previous entry.
     */
    public static Optional<HistoryRecord> readLatestRecord(final byte[] historyIndex, final byte[] historyTable,
                                                           boolean ignorePartial) {
        Optional<HistoryIndexRecord> latestIndex = HistoryIndexRecord.readLatestRecord(historyIndex);
        if (!latestIndex.isPresent()) {
            return Optional.empty();
        }

        Optional<HistoryRecord> record = readRecord(latestIndex.get().getEpoch(), historyIndex, historyTable, ignorePartial);
        if (!record.isPresent()) {
            // This can happen if we have the index updated but the history table isnt updated yet. Or the latest record was partial.
            // So fetch the previous indexed record.
            record = readRecord(latestIndex.get().getEpoch() - 1, historyIndex, historyTable, ignorePartial);
            assert record.isPresent();
        }

        if (ignorePartial && record.get().isPartial()) {
            return fetchPrevious(record.get(), historyIndex, historyTable);
        } else {
            return record;
        }
    }

    /**
     * Method to fetch record immediately following the given record.
     * @param record record whose next record is to be found.
     * @param historyTable history table
     * @param historyIndex history index
     * @param ignorePartial ignore partial record
     * @return returns history record immediately following given record. If the found record is partial entry and
     * ignorePartial flag is true then return empty.
     */
    public static Optional<HistoryRecord> fetchNext(final HistoryRecord record, final byte[] historyIndex, final byte[] historyTable,
                                                    boolean ignorePartial) {
        return readRecord(record.epoch + 1, historyIndex, historyTable, ignorePartial);
    }

    /**
     * Method to fetch record immediately preceeding the given record.
     * @param record record whose next record is to be found.
     * @param historyTable history table
     * @param historyIndex history index
     * @return returns history record immediately preceeding given record. If the given record is first record
     * in the table then return empty.
     */
    public static Optional<HistoryRecord> fetchPrevious(final HistoryRecord record, final byte[] historyIndex, final byte[] historyTable) {
        return readRecord(record.epoch - 1, historyIndex, historyTable, true);
    }

    @SneakyThrows(IOException.class)
    public static HistoryRecord parse(final byte[] table, final int offset) {
        InputStream inputStream = new ByteArrayInputStream(table, offset, table.length - offset);
        return SERIALIZER.deserialize(inputStream);
    }

    /**
     * Returns scale records in chronological order starting from epoch 0 and moving to the latest epoch.
     *
     * @param historyIndex history index
     * @param historyTable history table
     * @return list of pair of scale time and list of segments in the epoch.
     */
    public static List<Pair<Long, List<Long>>> readAllRecords(byte[] historyIndex, byte[] historyTable) {
        List<Pair<Long, List<Long>>> result = new LinkedList<>();
            Optional<HistoryRecord> record = HistoryRecord.readRecord(0, historyIndex, historyTable, true);
            while (record.isPresent()) {
                result.add(new ImmutablePair<>(record.get().getScaleTime(), record.get().getSegments()));
                record = fetchNext(record.get(), historyIndex, historyTable, true);
            }
        return result;
    }

    public static class HistoryRecordBuilder implements ObjectBuilder<HistoryRecord> {

    }
}
