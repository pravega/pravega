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
     * Returns the transaction name for a TransactionStreamSegment based on the name of the current Parent
     * StreamSegment, and the transactionId.
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment for this transaction.
     * @param transactionId           The unique Id for the transaction.
     * @return The name of the Transaction StreamSegmentId.
     */
    public static String getTransactionNameFromId(String parentStreamSegmentName, UUID transactionId) {
        StringBuilder result = new StringBuilder();
        result.append(parentStreamSegmentName);
        result.append(DELIMITER);
        result.append(String.format(PART_FORMAT, transactionId.getMostSignificantBits()));
        result.append(String.format(PART_FORMAT, transactionId.getLeastSignificantBits()));
        return result.toString();
    }


    /**
     * Attempts to extract the name of the Parent StreamSegment for the given Transaction StreamSegment. This method
     * returns a valid value only if the Transaction StreamSegmentName was generated using the
     * generateTransactionStreamSegmentName method.
     *
     * @param transactionName The name of the Transaction StreamSegment to extract the name of the Parent
     *                        StreamSegment.
     * @return The name of the Parent StreamSegment, or null if not a valid StreamSegment.
     */
    public static String getParentStreamSegmentName(String transactionName) {
        // Check to see if the given name is a properly formatted Transaction.
        int endOfStreamNamePos = transactionName.lastIndexOf(DELIMITER);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + DELIMITER.length() + ID_LENGTH > transactionName.length()) {
            // Improperly formatted Transaction name.
            return null;
        }
        return transactionName.substring(0, endOfStreamNamePos);
    }
}
