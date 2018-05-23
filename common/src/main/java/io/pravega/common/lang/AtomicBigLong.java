package io.pravega.common.lang;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.concurrent.GuardedBy;

/**
 * This class provides the ability to atomically update a BigLong value.
 */
public class AtomicBigLong {
    @GuardedBy("lock")
    private BigLong value;
    private final Object lock = new Object();

    public AtomicBigLong() {
        this.value = BigLong.ZERO;
    }

    @VisibleForTesting
    AtomicBigLong(int msb, long lsb) {
        this.value = new BigLong(msb, lsb);
    }

    public BigLong get() {
        return this.value;
    }

    public BigLong incrementAndGet() {
        synchronized (lock) {
            this.value = BigLong.add(this.value, 1);
            return this.value;
        }
    }

    public void set(int msb, long lsb) {
        synchronized (lock) {
            value = new BigLong(msb, lsb);
        }
    }
}