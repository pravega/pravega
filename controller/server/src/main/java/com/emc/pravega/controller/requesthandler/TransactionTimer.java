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
import java.util.concurrent.TimeUnit;

/**
 * Request handler for handling txn timeout requests.
 * It receives a txn request and if the txn has not timed out, it schedules until the txn times out.
 * If it has timed out the drop treansaction flow is executed.
 *
 * Note: a transactions timeout could be well into future and we would have scheduled a request in the executor.
 * This has two risks - if a lot of txns are coming in, we need a mechanism to slow down our request handling
 * or we will put a lot of stress on controller's memory.
 *
 * So we need to slow down fetches from txn queue until the existing backlog clears.
 *
 * A simpler scheme is if all txns have a fixed timeout and are posted into the queue in approximate order of
 * their timeout period. Then we can simply sleep and block the execution here.
 *
 * Right now we have a block method that is called and requests are assumed to follow the timeToDrop order.
 * The order can be achieved if all txns have fixed timeout period and these requests are posted in order of txn creation.
 * This order is not strictly guaranteed as there could be race between txns created concurrently.
 * But that error is within acceptable range of acting when a timeout occurs.
 *
 * Note: however, the ordering can still break significantly if we fail to drop a txn after timeout and fail with
 * a retryable exception. That will lead to txn being put back into the txnTimeout stream. And once its put back
 * there is no ordering guarantee for this event. However, its ok as we did try to execute this txn at the time
 * when it had timed out and we need to give fair opportunity to other txns.
 *
 * Also, if a scale request for this stream comes in, it will try to sweep clean all such txns that were not cleaned up
 * through this scheme.
 */
public class TransactionTimer implements RequestHandler<TxTimeoutRequest> {

    private final StreamTransactionMetadataTasks streamTxMetadataTasks;

    public TransactionTimer(StreamTransactionMetadataTasks streamTxMetadataTasks) {
        this.streamTxMetadataTasks = streamTxMetadataTasks;
    }

    @Override
    public CompletableFuture<Void> process(TxTimeoutRequest request, ScheduledExecutorService executor) {

        block(request.getTimeToDrop());

        CompletableFuture<Void> result = new CompletableFuture<>();
        executor.schedule(() -> streamTxMetadataTasks.dropTx(request.getScope(), request.getStream(), UUID.fromString(request.getTxid()), null)
                .thenAccept(status -> {
                    if (status.equals(TxnStatus.OPEN)) {
                        result.completeExceptionally(new RetryableException("Failed to drop and transaction is still open. Retry."));
                    }

                }), request.getTimeToDrop() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        return result;
    }

    private void block(long timeToDrop) {
        // If the transaction cannot be dropped right now, we will sleep on this thread, whereby blocking further fetches
        // from the stream. This is ok because requests are added into the stream approximately in the order of their
        // transaction.dropTime. So if this transaction cannot be dropped now, other transactions will also be delayed.
        // This works if each transaction has same fixed timeout-period.
        // If that changes, we will need to change this scheme.
        if (System.currentTimeMillis() < timeToDrop) {
            Exceptions.handleInterrupted(() -> Thread.sleep(timeToDrop - System.currentTimeMillis()));
            }
    }
}
