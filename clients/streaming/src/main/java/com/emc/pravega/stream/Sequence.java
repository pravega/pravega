package com.emc.pravega.stream;

import lombok.Data;

/**
 * A wrapper for two numbers. Where one is treated as 'high order' and the other as 'low order' to
 * break ties when the former is the same.
 */
@Data
public class Sequence implements Comparable<Sequence> {
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
