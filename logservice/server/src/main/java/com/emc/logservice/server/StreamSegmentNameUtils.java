package com.emc.logservice.server;

import com.emc.logservice.common.StringHelpers;

/**
 * Utility methods for StreamSegment Names.
 */
public final class StreamSegmentNameUtils {
    //region Members

    /**
     * We append this to the end of the Parent StreamName, then we append a unique identifier.
     */
    private static final String Delimiter = "#batch.";

    /**
     * The unique identifier is made of two parts, each having a length of 16 bytes (64 bits in Hex).
     */
    private static final int PartLength = 16;

    /**
     * The length of the unique identifier, in bytes (it is made of two parts).
     */
    private static final int IdLength = 2 * PartLength;

    /**
     * Custom String format that converts a 64 bit integer into a hex number, with leading zeroes.
     */
    private static final String PartFormat = "%0" + PartLength + "x";

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

        StringBuilder result = new StringBuilder();
        result.append(parentStreamSegmentName);
        result.append(Delimiter);
        result.append(String.format(PartFormat, part1));
        result.append(String.format(PartFormat, part2));
        return result.toString();
    }

    /**
     * Attempts to extract the name of the Parent StreamSegment for the given Batch StreamSegment. This method returns a
     * valid value only if the batchStreamSegmentName was generated using the generateBatchStreamSegmentName method.
     *
     * @param batchStreamSegmentName The name of the Batch StreamSegment to extract the name of the Parent StreamSegment.
     * @return The name of the Parent StreamSegment, or null if not a valid StreamSegment.
     */
    public static String getParentStreamSegmentName(String batchStreamSegmentName) {
        // Check to see if the given name is a properly formatted batch.
        int endOfStreamNamePos = batchStreamSegmentName.lastIndexOf(Delimiter);
        if (endOfStreamNamePos < 0 || endOfStreamNamePos + Delimiter.length() + IdLength > batchStreamSegmentName.length()) {
            // Improperly formatted batch name.
            return null;
        }

        // Extract the hashcode from Part 1 (We don't care about Part 2 here). The hash is the entire part1.
        int decodePos = endOfStreamNamePos + Delimiter.length();
        long hash;
        try {
            hash = Long.parseUnsignedLong(batchStreamSegmentName.substring(decodePos, decodePos + PartLength), 16);
        }
        catch (NumberFormatException ex) {
            // Not a valid batch name.
            return null;
        }

        // Determine the hash of the "parent" name.
        long expectedHash = StringHelpers.longHashCode(batchStreamSegmentName, 0, endOfStreamNamePos);
        if (hash == expectedHash) {
            return batchStreamSegmentName.substring(0, endOfStreamNamePos);
        }

        // Hash mismatch. Not a valid batch.
        return null;
    }
}
