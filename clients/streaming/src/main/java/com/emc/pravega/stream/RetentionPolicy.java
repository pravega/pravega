/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import java.io.Serializable;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class RetentionPolicy implements Serializable {
    public static final RetentionPolicy INFINITE = new RetentionPolicy(Type.TIME, Long.MAX_VALUE);
    private static final long serialVersionUID = 1L;
    
    public enum Type {
        /**
         * Set retention based on how long data has been in the stream.
         */
        TIME,
        /**
         * Set retention based on the total size of the data in the stream.
         */
        SIZE,
    }

    private final Type type;
    private final long value;
    
    public static RetentionPolicy byTime(Duration duration) {
        return new RetentionPolicy(Type.TIME, duration.toMillis());
    }
    
    public static RetentionPolicy bySizeBytes(long size) {
        return new RetentionPolicy(Type.SIZE, size);
    }
}
