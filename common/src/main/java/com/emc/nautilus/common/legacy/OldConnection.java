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
package com.emc.nautilus.common.legacy;

import java.nio.ByteBuffer;

import com.emc.nautilus.common.netty.ConnectionFailedException;

public interface OldConnection {

    @FunctionalInterface
    interface DataAvailableCallback {
        void readPossible();
    }

    @FunctionalInterface
    interface CapactyAvailableCallback {
        void writePossible();
    }

    /**
     * @param cb callback to be invoked when there is capacity available to write data. NOTE: the
     *            callback might be invoked unnecessarily, so check the capacityAvailable. This
     *            method may only be called once.
     */
    void setCapacityAvailableCallback(CapactyAvailableCallback cb);

    /**
     * @param cb callback to be invoked when there is data available to be read. NOTE: the callback
     *            might be invoked unnecessarily, so check the dataAvailable. This method may only
     *            be called once.
     */
    void setDataAvailableCallback(DataAvailableCallback cb);

    /**
     * @return the number of bytes that can be read without blocking
     */
    int dataAvailable();

    /**
     * @return The number of bytes that can be written without blocking
     */
    int capacityAvailable();

    /**
     * @param buffer the data to be written. All remaining data in the buffer will be written. The
     *            position and the limit of the provided buffer will be unchanged. If the buffer is
     *            larger than capacityAvailable this operation will block.
     * @throws ConnectionFailedException The connection has died, and can no longer be used.
     */
    void write(ByteBuffer buffer) throws ConnectionFailedException;

    /**
     * @param buffer The buffer to read the data into. The buffer will be filled from the current
     *            position to the current limit. The position and the limit of the provided buffer
     *            will be unchanged. If the buffer has more remaining than the dataAvailable this
     *            operation will block.
     * @throws ConnectionFailedException The connection has died, and can no longer be used.
     */
    void read(ByteBuffer buffer) throws ConnectionFailedException;

    /**
     * Drop the connection. No further operations may be performed.
     */
    void drop();
}
