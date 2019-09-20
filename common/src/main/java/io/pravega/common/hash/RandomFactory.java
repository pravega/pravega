/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.hash;

import java.security.SecureRandom;
import java.util.Random;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;

/**
 * Acts as a constructor for java.util.Random objects.
 * This avoids the anti-pattern of
 * <code>
 * int randomInt = new Random().nextInt();
 * </code>
 * Which is not really random because the default seed is the current time.
 * So this class provides the seed automatically from a master random number generator.
 */
public class RandomFactory {

    @GuardedBy("$LOCK")
    private static final SecureRandom SEED_GENERATOR = new SecureRandom();
    
    /**
     * Returns a new random number generator.
     * @return a new random number generator.
     */
    @Synchronized
    public static Random create() {
        return new Random(SEED_GENERATOR.nextLong());
    }
    
    /**
     * Returns a good seed for a random number generator.
     * @return a good seed for a random number generator.
     */
    @Synchronized
    public static long getSeed() {
        return SEED_GENERATOR.nextLong();
    }
}
