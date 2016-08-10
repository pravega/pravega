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

package com.emc.pravega.common;

/**
 * java.lang.String Extension methods.
 */
public final class StringHelpers {

    /**
     * Generates a Long hashCode of the given string.
     *
     * @param s      The string to calculate the hashcode of.
     * @param start  The offset in the string to start calculating the offset at.
     * @param length The number of bytes to calculate the offset for.
     * @return The result.
     */
    public static long longHashCode(String s, int start, int length) {
        // TODO: consider using one of http://google.github.io/guava/releases/19.0/api/docs/index.html?com/google/common/hash/Hashing.html
        long h = 0;
        for (int i = 0; i < length; i++) {
            h = 131L * h + s.charAt(start + i);
        }

        return h;
    }
}
