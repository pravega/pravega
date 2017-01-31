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
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.requests.TxTimeoutRequest;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.impl.TxnStatus;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class TransactionTimer implements RequestHandler<TxTimeoutRequest> {

    private final StreamTransactionMetadataTasks streamTxMetadataTasks;

    public TransactionTimer(StreamTransactionMetadataTasks streamTxMetadataTasks) {
        this.streamTxMetadataTasks = streamTxMetadataTasks;
    }

    @Override
    public CompletableFuture<Void> process(TxTimeoutRequest request, ScheduledExecutorService executor) {
        // If the transaction cannot be dropped right now, we will sleep on this thread, whereby blocking further fetches
        // from the stream. This is ok because requests are added into the stream approximately in the order of their
        // transaction.dropTime. So if this transaction cannot be dropped now, other transactions will also be delayed.
        // This works if each transaction has same fixed timeout-period.
        // If that changes, we will need to change this scheme.
        if (System.currentTimeMillis() < request.getTimeToDrop()) {
            Exceptions.handleInterrupted(() -> Thread.sleep(System.currentTimeMillis() - request.getTimeToDrop()));
        }

        return streamTxMetadataTasks.dropTx(request.getScope(), request.getStream(), UUID.fromString(request.getTxid()), null)
                .thenAccept(status -> {
                    if (status.equals(TxnStatus.OPEN)) {
                        throw new RetryableException("Failed to drop and transaction is still open. Retry.");
                    }
                });
    }
}
