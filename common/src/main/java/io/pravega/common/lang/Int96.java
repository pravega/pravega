/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.lang;

import com.google.common.base.Preconditions;
import io.pravega.common.util.BitConverter;
import lombok.Data;

/**
 * This class represents a 96 bit number with 32 bit msb encoded as integer and 64 bit lsb encoded as long.
 * It is unsigned and only allows for non negative values. It also implements comparable interface and comparison involves
 * first compariging msbs and if msbs are equal then we compare lsbs.
 */
@Data
public class Int96 implements Comparable<Int96> {
    public static final Int96 ZERO = new Int96(0, 0L);
    private final int msb;
    private final long lsb;

    public Int96(int msb, long lsb) {
        Preconditions.checkArgument(msb >= 0);
        Preconditions.checkArgument(lsb >= 0);

        this.msb = msb;
        this.lsb = lsb;
    }

    @Override
    public int compareTo(Int96 other) {
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

    public static Int96 fromBytes(byte[] b) {
        int msb = BitConverter.readInt(b, 0);
        long lsb = BitConverter.readLong(b, Integer.BYTES);

        return new Int96(msb, lsb);
    }

    public Int96 add(int increment) {
        Preconditions.checkArgument(increment >= 0);
        Int96 retVal;
        if (this.lsb <= Long.MAX_VALUE - increment) {
            retVal = new Int96(this.msb, this.lsb + increment);
        } else if (this.msb < Integer.MAX_VALUE) {
            int remainder = increment - (int) (Long.MAX_VALUE - this.lsb);
            retVal = new Int96(this.msb + 1, remainder);
        } else {
            // overflow: throw exception
            throw new ArithmeticException("Overflow");
        }

        return retVal;
    }
}
