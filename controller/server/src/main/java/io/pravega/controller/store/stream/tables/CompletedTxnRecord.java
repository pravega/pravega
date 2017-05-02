/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.store.stream.tables;


import io.pravega.common.util.BitConverter;
import io.pravega.controller.store.stream.TxnStatus;
import lombok.Data;

@Data
public class CompletedTxnRecord {
    private static final int COMPLETED_TXN_RECORD_SIZE = Long.BYTES + Integer.BYTES;

    private final long completeTime;
    private final TxnStatus completionStatus;

    public static CompletedTxnRecord parse(final byte[] bytes) {
        final long completeTimeStamp = BitConverter.readLong(bytes, 0);

        final TxnStatus status = TxnStatus.values()[BitConverter.readInt(bytes, Long.BYTES)];

        return new CompletedTxnRecord(completeTimeStamp, status);
    }

    public byte[] toByteArray() {
        byte[] b = new byte[COMPLETED_TXN_RECORD_SIZE];
        BitConverter.writeLong(b, 0, completeTime);
        BitConverter.writeInt(b, Long.BYTES, completionStatus.ordinal());

        return b;
    }

}
