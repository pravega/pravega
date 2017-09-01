/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.segment;

import com.google.common.base.Preconditions;
import java.util.UUID;

/**
 * Utility methods for StreamSegment Names.
 */
public final class StreamSegmentNameUtils {
    //region Members

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it stores its custom attributes.
     */
    private static final String STATE_SUFFIX_FIRST = "$state1";

    /**
     * This is appended to the end of the Segment/Transaction name to indicate it stores its custom attributes.
     */
    private static final String STATE_SUFFIX_SECOND = "$state2";

    /**
     * This is appended to the end of the Parent Segment Name, then we append a unique identifier.
     */
    private static final String TRANSACTION_DELIMITER = "#transaction.";

    /**
     * The Transaction unique identifier is made of two parts, each having a length of 16 bytes (64 bits in Hex).
     */
    private static final int TRANSACTION_PART_LENGTH = Long.BYTES * 8 / 4;

    /**
     * The length of the Transaction unique identifier, in bytes (it is made of two parts).
     */
    private static final int TRANSACTION_ID_LENGTH = 2 * TRANSACTION_PART_LENGTH;

    /**
     * Custom String format that converts a 64 bit integer into a hex number, with leading zeroes.
     */
    private static final String FULL_HEX_FORMAT = "%0" + TRANSACTION_PART_LENGTH + "x";

    //endregion

    /**
     * Returns the transaction name for a TransactionStreamSegment based on the name of the current Parent StreamSegment, and the transactionId.
     *
     * @param parentStreamSegmentName The name of the Parent StreamSegment for this transaction.
     * @param transactionId           The unique Id for the transaction.
     * @return The name of the Transaction StreamSegmentId.
     */
    public static String getTransactionNameFromId(String parentStreamSegmentName, UUID transactionId) {
        StringBuilder result = new StringBuilder();
        result.append(parentStreamSegmentName);
        result.append(TRANSACTION_DELIMITER);
        result.append(String.format(FULL_HEX_FORMAT, transactionId.getMostSignificantBits()));
        result.append(String.format(FULL_HEX_FORMAT, transactionId.getLeastSignificantBits()));
        return result.toString();
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
        int endOfStreamNamePos = transactionName.lastIndexOf(TRANSACTION_DELIMITER);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + TRANSACTION_DELIMITER.length() + TRANSACTION_ID_LENGTH > transactionName.length()) {
            // Improperly formatted Transaction name.
            return null;
        }
        return transactionName.substring(0, endOfStreamNamePos);
    }

    /**
     * Gets the name of the meta-Segment mapped to the given Segment Name that is responsible with storing Segment State.
     *
     * @param segmentName The name of the Segment to get the State segment name for.
     * @return The result.
     */
    public static String getFirstStateSegmentName(String segmentName) {
        Preconditions.checkArgument(!segmentName.contains(STATE_SUFFIX_FIRST), "segmentName is already a state segment name");
        return segmentName + STATE_SUFFIX_FIRST;
    }

    /**
     * Gets the name of the meta-Segment mapped to the given Segment Name that is responsible with storing Segment State.
     *
     * @param segmentName The name of the Segment to get the State segment name for.
     * @return The result.
     */
    public static String getSecondStateSegmentName(String segmentName) {
        Preconditions.checkArgument(!segmentName.contains(STATE_SUFFIX_SECOND), "segmentName is already a state segment name");
        return segmentName + STATE_SUFFIX_SECOND;
    }
}
