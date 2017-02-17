/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.testcommon;

import java.util.Random;

public class TestUtils {
    
    static final Random RAND = new Random();

    public static int randomPort() {
        int maxValue = 49151 - 1024;
        return RAND.nextInt(maxValue) + 1024;
    }

}
