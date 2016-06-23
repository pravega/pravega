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

package com.emc.logservice.server.core;

import java.util.Random;

/**
 * Generates Magic Numbers, which can be used for sequencing elements.
 */
public class MagicGenerator {
    /**
     * Default value for a Magic Number. This indicates no Magic Number has been assigned.
     */
    public static final int NO_MAGIC = Integer.MIN_VALUE;
    private static final Random GENERATOR = new Random();

    /**
     * Generates a new number that is different from NO_MAGIC.
     *
     * @return The newly generated number.
     */
    public static int newMagic() {
        int value;
        do {
            value = GENERATOR.nextInt();
        } while (value == NO_MAGIC);
        return value;
    }
}
