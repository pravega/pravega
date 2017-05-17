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

import io.pravega.client.stream.impl.segment.SequenceImpl;
import java.io.Serializable;

/**
 * A wrapper for two numbers. Where one is treated as 'high order' and the other as 'low order' to
 * break ties when the former is the same.
 */
public interface Sequence extends Comparable<Sequence>, Serializable {
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
    
    /**
     * Creates a new Sequence from two longs.
     * @param highOrder The 'high order' long. (Signed)
     * @param lowOrder The 'low order' long.  (Signed)
     */
    public static Sequence create(long highOrder, long lowOrder) {
        return SequenceImpl.create(highOrder, lowOrder);
    }
    
    public SequenceImpl asImpl();
    
}
