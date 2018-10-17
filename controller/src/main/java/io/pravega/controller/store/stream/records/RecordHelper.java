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
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.shared.segment.StreamSegmentNameUtils;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;

public class RecordHelper {
    
    // region scale helper methods
    /**
     * Method to validate supplied scale input. It performs a check that new ranges are identical to sealed ranges.
     *
     * @param segmentsToSeal segments to seal
     * @param newRanges      new ranges to create
     * @param currentEpoch   current epoch record
     * @return true if scale input is valid, false otherwise.
     */
    public static boolean validateInputRange(final List<Long> segmentsToSeal,
                                             final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                             final EpochRecord currentEpoch) {
        boolean newRangesPredicate = newRanges.stream().noneMatch(x -> x.getKey() >= x.getValue() &&
                x.getKey() >= 0 && x.getValue() > 0);

        List<AbstractMap.SimpleEntry<Double, Double>> oldRanges = segmentsToSeal.stream()
                .map(segmentId -> currentEpoch.getSegments().stream().filter(x -> x.segmentId() == segmentId).findFirst().map(x ->
                        new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        return newRangesPredicate && reduce(oldRanges).equals(reduce(newRanges));
    }

    /**
     * Method to check that segments to seal are present in current epoch
     *
     * @param segmentsToSeal segments to seal
     * @param currentEpoch current epoch record
     * @return true if a scale operation can be performed, false otherwise
     */
    public static boolean canScaleFor(final List<Long> segmentsToSeal, final EpochRecord currentEpoch) {
        return currentEpoch.getSegmentIds().containsAll(segmentsToSeal);
    }

    /**
     * Method to verify if supplied epoch transition record matches the supplied input which includes segments to seal, 
     * new ranges to create. 
     * For manual scale, it will verify that segments to seal match and epoch transition record share the same segment 
     * number.
     * 
     * @param segmentsToSeal list of segments to seal
     * @param newRanges list of new ranges to create
     * @param isManualScale if it is manual scale
     * @param record epoch transition record
     * @return true if record matches supplied input, false otherwise. 
     */
    public static boolean verifyRecordMatchesInput(List<Long> segmentsToSeal, List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                   boolean isManualScale, EpochTransitionRecord record) {
        boolean newRangeMatch = newRanges.stream().allMatch(x ->
                record.getNewSegmentsWithRange().values().stream()
                        .anyMatch(y -> y.getKey().equals(x.getKey())
                                && y.getValue().equals(x.getValue())));
        boolean segmentsToSealMatch = record.getSegmentsToSeal().stream().allMatch(segmentsToSeal::contains) ||
                (isManualScale && record.getSegmentsToSeal().stream().map(StreamSegmentNameUtils::getSegmentNumber).collect(Collectors.toSet())
                        .equals(segmentsToSeal.stream().map(StreamSegmentNameUtils::getSegmentNumber).collect(Collectors.toSet())));

        return newRangeMatch && segmentsToSealMatch;
    }

    /**
     * Method to compute epoch transition record. It takes segments to seal and new ranges and all the tables and
     * computes the next epoch transition record.
     * @param currentEpoch current epoch record
     * @param segmentsToSeal segments to seal
     * @param newRanges new ranges
     * @param scaleTimestamp scale time
     * @return new epoch transition record based on supplied input
     */
    public static EpochTransitionRecord computeEpochTransition(EpochRecord currentEpoch, List<Long> segmentsToSeal,
                                                               List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        Preconditions.checkState(currentEpoch.getSegmentIds().containsAll(segmentsToSeal), "Invalid epoch transition request");

        int newEpoch = currentEpoch.getEpoch() + 1;
        int nextSegmentNumber = currentEpoch.getSegments().stream().mapToInt(StreamSegmentRecord::getSegmentNumber).max().getAsInt() + 1;
        Map<Long, AbstractMap.SimpleEntry<Double, Double>> newSegments = new HashMap<>();
        IntStream.range(0, newRanges.size()).forEach(x -> {
            newSegments.put(computeSegmentId(nextSegmentNumber + x, newEpoch), newRanges.get(x));
        });
        return new EpochTransitionRecord(currentEpoch.getEpoch(), scaleTimestamp, ImmutableSet.copyOf(segmentsToSeal),
                ImmutableMap.copyOf(newSegments));

    }
    // endregion

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
