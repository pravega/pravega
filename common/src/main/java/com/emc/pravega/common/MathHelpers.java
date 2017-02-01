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

import com.google.common.base.Preconditions;

public class MathHelpers {

    /**
     * Returns absolute int value of given number.
     * @param in value to be absoluted
     * @return absolute value of in
     */
    public static int abs(int in) {
        return in & Integer.MAX_VALUE;
    }

    /**
     * Returns absolute long value of given number.
     * @param in number to needs to be converted to absolute value
     * @return absolute value of in
     */
    public static long abs(long in) {
        return in & Long.MAX_VALUE;
    }

    /**
     * Returns the value itself if it is between min and max numbers.
     * @param value actual value
     * @param min minimum value to check againsts
     * @param max maximum value to check againsts
     * @return the given value if it is between min and max, min if the value is less than min, or max if the value is greater than max
     */
    public static long minMax(long value, long min, long max) {
        Preconditions.checkArgument(min <= max, "min must be less than or equal to max");
        return Math.max(Math.min(value, max), min);
    }
}
