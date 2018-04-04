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

public class RandomFactory {

    @GuardedBy("$LOCK")
    private static final SecureRandom SEED_GENERATOR = new SecureRandom();
    
    @Synchronized
    public static Random create() {
        return new Random(SEED_GENERATOR.nextLong());
    }
    
    @Synchronized
    public static long getSeed() {
        return SEED_GENERATOR.nextLong();
    }
}
