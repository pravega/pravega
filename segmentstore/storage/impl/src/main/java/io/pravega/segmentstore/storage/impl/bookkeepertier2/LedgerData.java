package io.pravega.segmentstore.storage.impl.bookkeepertier2;

import java.nio.ByteBuffer;
import lombok.Data;
import org.apache.bookkeeper.client.LedgerHandle;

@Data
public class LedgerData {
    // Retrieved from ZK
    private final LedgerHandle lh;
    //Retrieved from ZK
    private final int startOffset;

    // Temporary variables. These are not persisted to ZK.
    //These are interpreted from bookkeeper and may be updated inproc.
    private int length;
    private boolean isReadonly;
    private long lastAddConfirmed = -1;

    public byte[] serialize() {
        int size = Long.SIZE + Integer.SIZE;

        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.putLong(this.lh.getId());
        return bb.array();
    }

    public synchronized void increaseLengthBy(int size) {
        this.length += size;
    }

}
