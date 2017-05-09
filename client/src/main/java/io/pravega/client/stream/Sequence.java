/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import java.io.Serializable;

import lombok.Data;

/**
 * A wrapper for two numbers. Where one is treated as 'high order' and the other as 'low order' to
 * break ties when the former is the same.
 */
@Data(staticConstructor = "create")
public class Sequence implements Comparable<Sequence>, Serializable {
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
