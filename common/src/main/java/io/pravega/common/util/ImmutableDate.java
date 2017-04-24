/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.util;

import java.util.Date;

import lombok.Data;

@Data
public class ImmutableDate {
    private final long time;
    
    public ImmutableDate() {
        time = System.currentTimeMillis();
    }
    
    public ImmutableDate(long time) {
        this.time = time;
    }

    public Date asDate() {
        return new Date(time);
    }
}
