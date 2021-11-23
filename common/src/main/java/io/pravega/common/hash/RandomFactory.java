/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * Returns a new secure random number generator.
     * @return a new secure random number generator.
     */
    public static SecureRandom createSecure() {
        return new SecureRandom();
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
