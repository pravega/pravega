/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import com.google.common.base.Preconditions;

public class ClientMetricTags {

    // Metric Tag Names
    private static final String TAG_SCOPE = "scope";
    private static final String TAG_STREAM = "stream";
    private static final String TAG_SEGMENT = "segment";
    private static final String TAG_TRANSACTION = "transaction";
    private static final String TAG_EPOCH = "epoch";

    private static final String TRANSACTION_DELIMITER = "#transaction.";
    private static final String EPOCH_DELIMITER = ".#epoch.";

    /**
     * Generate segment tags (string array) on the input fully qualified segment name to be associated with a metric.
     *
     * @param qualifiedSegmentName fully qualified segment name.
     * @return string array as segment tag of metric.
     */
    public static String[] segmentTags(String qualifiedSegmentName) {
        Preconditions.checkNotNull(qualifiedSegmentName);
        String[] tags = {TAG_SCOPE, null, TAG_STREAM, null, TAG_SEGMENT, null, TAG_EPOCH, null};

        String segmentBaseName = getSegmentBaseName(qualifiedSegmentName);
        String[] tokens = segmentBaseName.split("[/]");

        int segmentIdIndex = (tokens.length == 1) ? 0 : (tokens.length) == 2 ? 1 : 2;
        if (tokens[segmentIdIndex].contains(EPOCH_DELIMITER)) {
            String[] segmentIdTokens = tokens[segmentIdIndex].split(EPOCH_DELIMITER);
            tags[5] = segmentIdTokens[0];
            tags[7] = segmentIdTokens[1];
        } else {
            tags[5] = tokens[segmentIdIndex];
            tags[7] = "0";
        }

        if (tokens.length == 3) {
            tags[1] = tokens[0];
            tags[3] = tokens[1];
        } else {
            tags[1] = "default";
            tags[3] = tokens[0];
        }
        return tags;
    }

    /**
     * Get base name of segment with the potential transaction delimiter removed.
     *
     * @param segmentQualifiedName fully qualified segment name.
     * @return the base name of segment.
     */
    private static String getSegmentBaseName(String segmentQualifiedName) {
        int endOfStreamNamePos = segmentQualifiedName.lastIndexOf(TRANSACTION_DELIMITER);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + TRANSACTION_DELIMITER.length() + Long.SIZE / 2 > segmentQualifiedName.length()) {
            //not a transaction segment.
            return segmentQualifiedName;
        }
        //transaction segment: only return the portion before transaction delimiter.
        return segmentQualifiedName.substring(0, endOfStreamNamePos);
    }
}
