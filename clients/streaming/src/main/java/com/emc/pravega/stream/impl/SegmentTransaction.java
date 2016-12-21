/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.TxFailedException;

import java.util.UUID;

/**
 * The mirror of Transaction but that is specific to one segment.
 */
public interface SegmentTransaction<Type> {
    UUID getId();

    /**
     * Publishes the provided event to this transaction on this segment. This operation is asyncronus, the item is not
     * Guaranteed to be stored until after {@link #flush()} has been called.
     *
     * @param event The event to write.
     * @throws TxFailedException The item could be persisted because the transaction has failed. (Timed out or dropped)
     */
    void writeEvent(Type event) throws TxFailedException;

    /**
     * Blocks until all events passed to the writeEvent call have made it to durable storage.
     * After this the transaction can be committed.
     *
     * @throws TxFailedException Not all of the items could be persisted because the transaction has failed. (Timed out or dropped)
     */
    void flush() throws TxFailedException;
}