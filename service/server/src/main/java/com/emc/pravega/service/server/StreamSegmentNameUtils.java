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

package com.emc.pravega.service.server;

import java.util.UUID;

/**
 * Utility methods for StreamSegment Names.
 */
public final class StreamSegmentNameUtils {
    //region Members

    /**
     * We append this to the end of the Parent StreamName, then we append a unique identifier.
     */
    private static final String DELIMITER = "#batch.";

    /**
     * The unique identifier is made of two parts, each having a length of 16 bytes (64 bits in Hex).
     */
    private static final int PART_LENGTH = 16;

    /**
     * The length of the unique identifier, in bytes (it is made of two parts).
     */
    private static final int ID_LENGTH = 2 * PART_LENGTH;

    /**
     * Custom String format that converts a 64 bit integer into a hex number, with leading zeroes.
     */
    private static final String PART_FORMAT = "%0" + PART_LENGTH + "x";

    //endregion

    /**
     * Returns the batch name for a Batch StreamSegment based on the name of the current Parent StreamSegment, and the batchId
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment for this batch.
     * @param batchId The unique Id for the batch.
     * @return The name of the Batch StreamSegmentId.
     */
    public static String getBatchNameFromId(String parentStreamSegmentName, UUID batchId) {
        StringBuilder result = new StringBuilder();
        result.append(parentStreamSegmentName);
        result.append(DELIMITER);
        result.append(String.format(PART_FORMAT, batchId.getMostSignificantBits()));
        result.append(String.format(PART_FORMAT, batchId.getLeastSignificantBits()));
        return result.toString();
    }

    /**
     * Attempts to extract the name of the Parent StreamSegment for the given Batch StreamSegment. This method returns a
     * valid value only if the batchStreamSegmentName was generated using the {@link #getBatchNameFromId(String, UUID)} method.
     *
     * @param batchStreamSegmentName The name of the Batch StreamSegment to extract the name of the Parent StreamSegment.
     * @return The name of the Parent StreamSegment, or null if not a valid StreamSegment.
     */
    public static String getParentStreamSegmentName(String batchStreamSegmentName) {
        // Check to see if the given name is a properly formatted batch.
        int endOfStreamNamePos = batchStreamSegmentName.lastIndexOf(DELIMITER);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + DELIMITER.length() + ID_LENGTH > batchStreamSegmentName.length()) {
            // Improperly formatted batch name.
            return null;
        }
        return batchStreamSegmentName.substring(0, endOfStreamNamePos);
    }
}
