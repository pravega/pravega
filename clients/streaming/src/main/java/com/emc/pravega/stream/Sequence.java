/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream;

import lombok.Data;

/**
 * A wrapper for two numbers. Where one is treated as 'high order' and the other as 'low order' to
 * break ties when the former is the same.
 */
@Data(staticConstructor = "create")
public class Sequence implements Comparable<Sequence> {
    public static final Sequence MAX_VALUE = new Sequence(Long.MAX_VALUE, Long.MAX_VALUE);
    public static final Sequence MIN_VALUE = new Sequence(Long.MIN_VALUE, Long.MIN_VALUE);
    private final long highOrder;
    private final long lowOrder;
    
    @Override
    public int compareTo(Sequence o) {
        int result = Long.compare(highOrder, o.highOrder);
        if (result == 0) {
            return Long.compare(lowOrder, o.lowOrder);
        }
        return result;
    }
}
