/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common;

import com.google.common.base.Preconditions;

public class MathHelpers {

    public static int abs(int in) {
        return in & Integer.MAX_VALUE;
    }

    public static long abs(long in) {
        return in & Long.MAX_VALUE;
    }

    public static int minMax(int value, int min, int max) {
        Preconditions.checkArgument(min <= max, "min must be less than or equal to max");
        return Math.max(Math.min(value, max), min);
    }
    
    public static long minMax(long value, long min, long max) {
        Preconditions.checkArgument(min <= max, "min must be less than or equal to max");
        return Math.max(Math.min(value, max), min);
    }
}
