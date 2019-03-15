/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared;

import com.google.common.base.Preconditions;

public class MetricsTags {

    // Metric Tag Names
    public static final String TAG_CONTAINER = "container";
    public static final String TAG_HOST = "host";
    public static final String TAG_SCOPE = "scope";
    public static final String TAG_STREAM = "stream";
    public static final String TAG_SEGMENT = "segment";
    public static final String TAG_TRANSACTION = "transaction";
    public static final String TAG_EPOCH = "epoch";

    private static final String TRANSACTION_DELIMITER = "#transaction.";
    private static final String EPOCH_DELIMITER = ".#epoch.";
    /**
     * The Transaction unique identifier is made of two parts, each having a length of 16 bytes (64 bits in Hex).
     */
    private static final int TRANSACTION_PART_LENGTH = Long.BYTES * 8 / 4;
    /**
     * The length of the Transaction unique identifier, in bytes (it is made of two parts).
     */
    private static final int TRANSACTION_ID_LENGTH = 2 * TRANSACTION_PART_LENGTH;

    /**
     * Generate a container tag (string array) on the input containerId to be associated with a metric.
     * @param containerId container id.
     * @return string array as the container tag of metric.
     */
    public static String[] containerTag(int containerId) {
        return new String[] {TAG_CONTAINER, String.valueOf(containerId)};
    }

    /**
     * Generate a host tag (string array) on the input hostname to be associated with a metric.
     * @param hostname hostname of the metric.
     * @return string array as the host tag of metric.
     */
    public static String[] hostTag(String hostname) {
        return new String[] {TAG_HOST, hostname};
    }

    /**
     * Generate stream tags (string array) on the input scope and stream name to be associated with a metric.
     * @param scope scope of the stream.
     * @param stream stream name.
     * @return string array as the stream tag of metric.
     */
    public static String[] streamTags(String scope, String stream) {
        return new String[] {TAG_SCOPE, scope, TAG_STREAM, stream};
    }

    /**
     * Generate transaction tags (string array) on the input scope, stream and transactionId to be associated with a metric.
     * @param scope scope of the stream.
     * @param stream stream name.
     * @param transactionId transaction id.
     * @return string array as transaction tag of metric.
     */
    public static String[] transactionTags(String scope, String stream, String transactionId) {
        return new String[] {TAG_SCOPE, scope, TAG_STREAM, stream, TAG_TRANSACTION, transactionId};
    }

    /**
     * Generate segment tags (string array) on the input fully qualified segment name to be associated with a metric.
     * @param qualifiedSegmentName fully qualified segment name.
     * @return string array as segment tag of metric.
     */
    public static String[] segmentTags(String qualifiedSegmentName) {
        Preconditions.checkNotNull(qualifiedSegmentName);
        String segmentBaseName = getSegmentBaseName(qualifiedSegmentName);

        String[] tags = {TAG_SCOPE, null, TAG_STREAM, null, TAG_SEGMENT, null, TAG_EPOCH, null};
        String[] tokens = segmentBaseName.split("[/]");
        int segmentIdIndex = tokens.length == 2 ? 1 : 2;
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
     * @param segmentQualifiedName fully qualified segment name.
     * @return the base name of segment.
     */
    private static String getSegmentBaseName(String segmentQualifiedName) {
        int endOfStreamNamePos = segmentQualifiedName.lastIndexOf(TRANSACTION_DELIMITER);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + TRANSACTION_DELIMITER.length() + TRANSACTION_ID_LENGTH > segmentQualifiedName.length()) {
            //not a transaction segment.
            return segmentQualifiedName;
        }
        //transaction segment: only return the portion before transaction delimiter.
        return segmentQualifiedName.substring(0, endOfStreamNamePos);
    }
}
