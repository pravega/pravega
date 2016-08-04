package com.emc.pravega.common;

public class MathHelpers {

    public static int abs(int in) {
        return in & Integer.MAX_VALUE;
    }
    
    public static long abs(long in) {
        return in & Long.MAX_VALUE;
    }
    
}
