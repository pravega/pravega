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
package io.pravega.shared;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.net.InetAddress;
import java.net.UnknownHostException;

public final class MetricsTags {

    //The default key to lookup hostname system property or env var.
    public static final String DEFAULT_HOSTNAME_KEY = "HOSTNAME";

    // Metric Tag Names
    public static final String TAG_CONTAINER = "container";
    public static final String TAG_HOST = "host";
    public static final String TAG_SCOPE = "scope";
    public static final String TAG_STREAM = "stream";
    public static final String TAG_READER_GROUP = "readergroup";
    public static final String TAG_SEGMENT = "segment";
    public static final String TAG_TRANSACTION = "transaction";
    public static final String TAG_EPOCH = "epoch";
    public static final String TAG_CLASS = "class";
    public static final String TAG_EXCEPTION = "exception";
    public static final String TAG_THROTTLER = "throttler";
    public static final String TAG_EVENT_PROCESSOR = "eventprocessor";

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

    // For table segment name parsing, will depend on StreamSegmentNameUtils.
    private static final String TABLES = "_tables";
    private static final String TABLE_SEGMENT_DELIMITER = "/" + TABLES + "/";

    /**
     * Generate a container tag (string array) on the input containerId to be associated with a metric.
     * @param containerId container id.
     * @return string array as the container tag of metric.
     */
    public static String[] containerTag(int containerId) {
        return new String[] {TAG_CONTAINER, String.valueOf(containerId)};
    }

    /**
     * Generate a throttler tag (string array) on the input throttler to be associated with a metric.
     * @param containerId container id.
     * @param throttler throttler name.
     * @return string array as the throttler tag of metric.
     */
    public static String[] throttlerTag(int containerId, String throttler) {
        return new String[] {TAG_CONTAINER, String.valueOf(containerId), TAG_THROTTLER, throttler};
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
     * Generate reader group tags (string array) on the input scope and reader group name to be associated with a metric.
     * @param scope scope of the stream.
     * @param rgName Reader Group name.
     * @return string array as the reader group tag of metric.
     */
    public static String[] readerGroupTags(String scope, String rgName) {
        return new String[] {TAG_SCOPE, scope, TAG_READER_GROUP, rgName};
    }

    /**
     * Generate EventProcessor tags (String array) given the event processor name and container id.
     *
     * @param containerId         Container id for this EventProcessor.
     * @param eventProcessorName  Name of the EventProcessor.
     * @return                    String array with the EventProcessor tags.
     */
    public static String[] eventProcessorTag(int containerId, String eventProcessorName) {
        return new String[] {TAG_CONTAINER, String.valueOf(containerId), TAG_EVENT_PROCESSOR, eventProcessorName};
    }

    /**
     * Generate segment tags (string array) on the input fully qualified segment name to be associated with a metric.
     * @param qualifiedSegmentName fully qualified segment name.
     * @return string array as segment tag of metric.
     */
    public static String[] segmentTags(String qualifiedSegmentName) {
        Preconditions.checkNotNull(qualifiedSegmentName);
        String[] tags = {TAG_SCOPE, null, TAG_STREAM, null, TAG_SEGMENT, null, TAG_EPOCH, null};
        if (qualifiedSegmentName.contains(TABLE_SEGMENT_DELIMITER)) {
            String[] tokens = qualifiedSegmentName.split(TABLE_SEGMENT_DELIMITER);
            tags[1] = tokens[0];
            tags[3] = TABLES;
            tags[5] = tokens[1];
            tags[7] = "0";
            return tags;
        }

        String segmentBaseName = getSegmentBaseName(qualifiedSegmentName);
        String[] tokens = segmentBaseName.split("/");
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

    /**
     * Returns the Segment tag directly using the Segment name passed as input.
     *
     * @param segmentName Name of the Segment to generate Segment tags for.
     * @return Segment tags directly using input Segment name.
     */
    public static String[] segmentTagDirect(String segmentName) {
        return new String[] {TAG_SEGMENT, String.valueOf(segmentName)};
    }

    /**
     * Create host tag based on the system property, env var or local host config.
     * @param hostnameKey the lookup key for hostname system property or env var.
     * @return host tag.
     */
    public static String[] createHostTag(String hostnameKey) {
        String[] hostTag = {MetricsTags.TAG_HOST, null};

        //Always take system property if it's defined.
        hostTag[1] = System.getProperty(hostnameKey);
        if (!Strings.isNullOrEmpty(hostTag[1])) {
            return hostTag;
        }

        //Then take env variable if it's defined.
        hostTag[1] = System.getenv(hostnameKey);
        if (!Strings.isNullOrEmpty(hostTag[1])) {
            return hostTag;
        }

        //Finally use the resolved hostname based on localhost config.
        try {
            hostTag[1] = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostTag[1] = "unknown";
        }
        return hostTag;
    }

    /**
     * Generate an Exception tag (string array) on the input class name.
     *
     * @param loggingClassName   The name of the class that recorded the exception.
     * @param exceptionClassName The name of the exception class that was recorded. May be null.
     * @return A String array containing the necessary tags.
     */
    public static String[] exceptionTag(String loggingClassName, String exceptionClassName) {
        String[] result = new String[]{TAG_CLASS, getSimpleClassName(loggingClassName), TAG_EXCEPTION, "none"};
        if (exceptionClassName != null) {
            result[3] = getSimpleClassName(exceptionClassName);
        }
        return result;
    }

    private static String getSimpleClassName(String name) {
        int lastSeparator = name.lastIndexOf(".");
        if (lastSeparator < 0 || lastSeparator >= name.length() - 1) {
            return name;
        } else {
            return name.substring(lastSeparator + 1);
        }
    }
}
