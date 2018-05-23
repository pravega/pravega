/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.lang;

import com.google.common.base.Preconditions;
import io.pravega.common.util.BitConverter;
import lombok.Data;

@Data
/**
 * This class represents a 96 bit number with 32 bit msb encoded as integer and 64 bit lsb encoded as long.
 * It is unsigned and only allows for non negative values. It also implements comparable interface and comparison involves
 * first compariging msbs and if msbs are equal then we compare lsbs.
 */
public class BigLong implements Comparable {
    public static final BigLong ZERO = new BigLong(0, 0L);
    private final int msb;
    private final long lsb;

    public BigLong(int msb, long lsb) {
        Preconditions.checkArgument(msb >= 0);
        Preconditions.checkArgument(lsb >= 0);

        this.msb = msb;
        this.lsb = lsb;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof BigLong)) {
            throw new RuntimeException("incomparable objects");
        }
        BigLong other = (BigLong) o;

        if (msb != other.msb) {
            return Integer.compare(msb, other.msb);
        } else {
            return Long.compare(lsb, other.lsb);
        }
    }

    public byte[] toBytes() {
        byte[] b = new byte[Integer.BYTES + Long.BYTES];
        BitConverter.writeInt(b, 0, msb);
        BitConverter.writeLong(b, Integer.BYTES, lsb);
        return b;
    }

    public static BigLong fromBytes(byte[] b) {
        int msb = BitConverter.readInt(b, 0);
        long lsb = BitConverter.readLong(b, Integer.BYTES);

        return new BigLong(msb, lsb);
    }

    public static BigLong add(BigLong counter, int increment) {
        Preconditions.checkArgument(increment >= 0);
        BigLong retVal;
        if (counter.lsb <= Long.MAX_VALUE - increment) {
            retVal = new BigLong(counter.msb, counter.lsb + increment);
        } else if (counter.msb < Integer.MAX_VALUE) {
            int remainder = increment - (int) (Long.MAX_VALUE - counter.lsb);
            retVal = new BigLong(counter.msb + 1, remainder);
        } else {
            // overflow: throw exception
            throw new ArithmeticException("Overflow");
        }

        return retVal;
    }
}
