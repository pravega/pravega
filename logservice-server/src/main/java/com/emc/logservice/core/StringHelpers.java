package com.emc.logservice.core;

/**
 * java.lang.String Extension methods.
 */
public class StringHelpers {

    /**
     * Generates a Long hashCode of the given string.
     *
     * @param s      The string to calculate the hashcode of.
     * @param start  The offset in the string to start calculating the offset at.
     * @param length The number of bytes to calculate the offset for.
     * @return The result.
     */
    public static long longHashCode(String s, int start, int length) {
        long h = 0;
        for (int i = 0; i < length; i++) {
            h = 131L * h + s.charAt(start + i);
        }

        return h;
    }
}
