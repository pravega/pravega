/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
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
    //Version to ensure CAS in ZK
    private final  int updateVersion;
    //EPOC under which the ledger is created
    private final long containerEpoc;

    // Temporary variables. These are not persisted to ZK.
    //These are interpreted from bookkeeper and may be updated inproc.
    private int length;
    private boolean isReadonly;
    private long lastAddConfirmed = -1;

    public byte[] serialize() {
        int size = Long.SIZE + Long.SIZE;

        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.putLong(this.lh.getId());
        bb.putLong(this.containerEpoc);
        return bb.array();
    }

    public synchronized void increaseLengthBy(int size) {
        this.length += size;
    }

}
