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

import com.emc.pravega.common.StringHelpers;

/**
 * Utility methods for StreamSegment Names.
 */
public final class StreamSegmentNameUtils {
    //region Members

    /**
     * We append this to the end of the Parent StreamName, then we append a unique identifier.
     */
    private static final String DELIMITER = "#transaction.";

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
     * Generates a name for a Batch StreamSegment based on the name of the current Parent StreamSegment.
     * Every call to this method should generate a different name, as long as the calls are at least 1ns apart.
     * The return value from this method can be decomposed using the getParentStreamSegmentName method.
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment for this batch.
     * @return The name of the Batch StreamSegmentId.
     */
    public static String generateBatchStreamSegmentName(String parentStreamSegmentName) {
        // Part 1 is the the long HashCode for the parentStreamSegmentName.
        long part1 = StringHelpers.longHashCode(parentStreamSegmentName, 0, parentStreamSegmentName.length());

        // Part 2 is a combination of the current time, expressed both in Millis and in Nanos.
        long part2 = System.currentTimeMillis() & 0xffffffffL;
        part2 = part2 << Integer.SIZE | System.nanoTime() & 0xffffffffL;

        return parentStreamSegmentName + DELIMITER + String.format(PART_FORMAT, part1) + String.format(PART_FORMAT, part2);
    }

    /**
     * Attempts to extract the name of the Parent StreamSegment for the given Transaction StreamSegment. This method returns a
     * valid value only if the Transaction StreamSegmentName was generated using the generateTransactionStreamSegmentName method.
     *
     * @param transactionName The name of the Transaction StreamSegment to extract the name of the Parent StreamSegment.
     * @return The name of the Parent StreamSegment, or null if not a valid StreamSegment.
     */
    public static String getParentStreamSegmentName(String transactionName) {
        // Check to see if the given name is a properly formatted Transaction.
        int endOfStreamNamePos = transactionName.lastIndexOf(DELIMITER);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + DELIMITER.length() + ID_LENGTH > transactionName.length()) {
            // Improperly formatted Transaction name.
            return null;
        }

        // Extract the hashcode from Part 1 (We don't care about Part 2 here). The hash is the entire part1.
        int decodePos = endOfStreamNamePos + DELIMITER.length();
        long hash;
        try {
            hash = Long.parseUnsignedLong(transactionName.substring(decodePos, decodePos + PART_LENGTH), 16);
        } catch (NumberFormatException ex) {
            // Not a valid Transaction name.
            return null;
        }

        // Determine the hash of the "parent" name.
        long expectedHash = StringHelpers.longHashCode(transactionName, 0, endOfStreamNamePos);
        if (hash == expectedHash) {
            return transactionName.substring(0, endOfStreamNamePos);
        }

        // Hash mismatch. Not a valid Transaction.
        return null;
    }
}
