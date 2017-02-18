/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
