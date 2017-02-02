package com.emc.pravega.common;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Clock implements Supplier<Long> {

    private final AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
    
    @Override
    public Long get() {
        return lastTime.updateAndGet(old -> Math.max(old, System.currentTimeMillis()));
    }

}
