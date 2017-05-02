/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream;

import io.pravega.stream.impl.segment.SequenceImpl;

/**
 * A wrapper for two numbers. Where one is treated as 'high order' and the other as 'low order' to
 * break ties when the former is the same.
 */
public interface Sequence extends Comparable<Sequence> {
    public static final Sequence MAX_VALUE = create(Long.MAX_VALUE, Long.MAX_VALUE);
    public static final Sequence ZERO = create(0L, 0L);
    public static final Sequence MIN_VALUE = create(Long.MIN_VALUE, Long.MIN_VALUE);
    
    public long getHighOrder();
    
    public long getLowOrder();
    
    public default int compareTo(Sequence o) {
        int result = Long.compare(getHighOrder(), o.getHighOrder());
        if (result == 0) {
            return Long.compare(getLowOrder(), o.getLowOrder());
        }
        return result;
    }
    
    public static Sequence create(long highOrder, long lowOrder) {
        return SequenceImpl.create(highOrder, lowOrder);
    }
    
    public SequenceImpl asImpl();
    
}
