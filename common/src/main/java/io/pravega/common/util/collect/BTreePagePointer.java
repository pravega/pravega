package io.pravega.common.util.collect;

import io.pravega.common.util.ByteArraySegment;
import lombok.Data;

@Data
class BTreePagePointer {
    private final ByteArraySegment key;
    private final long offset;
    private final int length;
}
