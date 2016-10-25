/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.stream.impl.TxStatus;
import lombok.Data;
import org.apache.commons.lang.ArrayUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Data
public class ActiveTxRecord {
    private final long txCreationTimestamp;
    private final TxStatus txStatus;

    public static ActiveTxRecord parse(final byte[] bytes) {
        final long txCreationTimestamp = Utilities.toLong(ArrayUtils.subarray(bytes, 0, Long.SIZE / 8));

        final TxStatus status = TxStatus.values()[Utilities.toInt(ArrayUtils.subarray(bytes, Long.SIZE / 8, bytes.length))];

        return new ActiveTxRecord(txCreationTimestamp, status);
    }

    public byte[] toByteArray() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            outputStream.write(Utilities.toByteArray(txCreationTimestamp));

            outputStream.write(Utilities.toByteArray(txStatus.ordinal()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return outputStream.toByteArray();
    }
}
