/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.timeout;

import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.OperationOnTxNotAllowedException;
import com.emc.pravega.controller.store.stream.TransactionNotFoundException;
import com.emc.pravega.controller.store.stream.WriteConflictException;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Transaction ping manager. It maintains a local hashed timer wheel to manage txn timeouts.
 * It provides the following two methods.
 * 1. Set initial timeout.
 * 2. Increase timeout.
 */
@Slf4j
public class TimerWheelTimeoutService extends AbstractService implements TimeoutService {

    // region HashedWheelTimer parameters

    private static final ThreadFactory THREAD_FACTORY = Executors.defaultThreadFactory();
    private static final long TICK_DURATION = 400;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final int TICKS_PER_WHEEL = 512;
    private static final boolean LEAK_DETECTION = true;

    // endregion

    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final HashedWheelTimer hashedWheelTimer;
    private final ConcurrentHashMap<String, TxnData> map;
    @Getter
    private final long maxLeaseValue;
    @Getter
    private final long maxScaleGracePeriod;

    @Getter(value = AccessLevel.PACKAGE)
    @VisibleForTesting
    private final BlockingQueue<Optional<Throwable>> taskCompletionQueue;

    @AllArgsConstructor
    private class TxnTimeoutTask implements TimerTask {

        private final String scope;
        private final String stream;
        private final UUID txnId;

        @Override
        public void run(Timeout timeout) throws Exception {

            String key = getKey(scope, stream, txnId);
            TxnData txnData = map.get(key);

            log.debug("Executing timeout task for txn {}", key);
            streamTransactionMetadataTasks.abortTxn(scope, stream, txnId, Optional.of(txnData.getVersion()), null)
                    .handle((ok, ex) -> {
                        // If abort attempt fails because of (1) version mismatch, or (2) node not found,
                        // ignore the timeout task.
                        // In other cases, esp. because lock attempt fails, we reschedule this task for execution
                        // at a later point of time.
                        if (ex != null) {
                            Throwable error = getRealCause(ex);
                            if (error instanceof WriteConflictException ||
                                    error instanceof DataNotFoundException ||
                                    error instanceof TransactionNotFoundException ||
                                    error instanceof OperationOnTxNotAllowedException ) {
                                log.debug("Timeout task for tx {} failed because of {}. Ignoring timeout task.",
                                        key, error.getClass().getName());
                                map.remove(key);
                                notifyCompletion(error);
                            } else {
                                String errorMsg = String.format("Rescheduling timeout task for tx %s because " +
                                        "of transient or unknown error", key);
                                log.warn(errorMsg, ex);
                                hashedWheelTimer.newTimeout(this, 2 * TICK_DURATION, TIME_UNIT);
                            }
                        } else {
                            log.debug("Successfully executed abort on tx {} ", key);
                            map.remove(key);
                            notifyCompletion(null);
                        }
                        return null;
                    });
        }

        private void notifyCompletion(Throwable error) {
            if (taskCompletionQueue != null) {
                if (error != null) {
                    taskCompletionQueue.add(Optional.of(error));
                } else {
                    taskCompletionQueue.add(Optional.<Throwable>empty());
                }
            }
        }
    }

    @Data
    @AllArgsConstructor
    private static class TxnData {
        private final int version;
        private final long maxExecutionTimeExpiry;
        private final long scaleGracePeriod;
        private Timeout timeout;
    }

    public TimerWheelTimeoutService(final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                                    final long maxLeaseValue,
                                    final long maxScaleGracePeriod) {
        this(streamTransactionMetadataTasks, maxLeaseValue, maxScaleGracePeriod, null);
    }

    @VisibleForTesting
    TimerWheelTimeoutService(final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                             final long maxLeaseValue,
                             final long maxScaleGracePeriod,
                             final BlockingQueue<Optional<Throwable>> taskCompletionQueue) {
        this.streamTransactionMetadataTasks = streamTransactionMetadataTasks;
        this.hashedWheelTimer = new HashedWheelTimer(THREAD_FACTORY, TICK_DURATION, TIME_UNIT, TICKS_PER_WHEEL,
                LEAK_DETECTION);
        this.map = new ConcurrentHashMap<>();
        this.maxLeaseValue = maxLeaseValue;
        this.maxScaleGracePeriod = maxScaleGracePeriod;
        this.taskCompletionQueue = taskCompletionQueue;
        this.startAsync();
    }

    /**
     * Start the ping manager. This method starts the hashed wheel timer.
     */
    @Override
    protected void doStart() {
        hashedWheelTimer.start();
        notifyStarted();
    }

    /**
     * Stop the ping manager. This method stops the hashed wheel timer and clears the map.
     * It may be called on (a) service stop, or (b) when the process gets disconnected from cluster.
     * If this object receives a ping in stopped state, it will send DISCONNECTED status.
     */
    @Override
    protected void doStop() {
        hashedWheelTimer.stop();
        map.clear();
        notifyStopped();
    }

    @Override
    public void addTxn(final String scope, final String stream, final UUID txnId, final int version,
                       final long lease, final long maxExecutionTimeExpiry, final long scaleGracePeriod) {

        if (this.isRunning()) {
            final String key = getKey(scope, stream, txnId);
            final TxnTimeoutTask task = new TxnTimeoutTask(scope, stream, txnId);
            Timeout timeout = hashedWheelTimer.newTimeout(task, lease, TimeUnit.MILLISECONDS);
            map.put(key, new TxnData(version, maxExecutionTimeExpiry, scaleGracePeriod, timeout));
        }

    }

    @Override
    public void removeTxn(String scope, String stream, UUID txnId) {
        String key = getKey(scope, stream, txnId);
        final TxnData txnData = map.get(key);
        txnData.getTimeout().cancel();
        map.remove(key);
    }

    @Override
    public PingTxnStatus pingTxn(final String scope, final String stream, final UUID txnId, long lease) {

        if (!this.isRunning()) {
            return PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.DISCONNECTED).build();
        }

        final String key = getKey(scope, stream, txnId);
        Preconditions.checkState(map.containsKey(key), "Stream not found in the map");

        final TxnData txnData = map.get(key);

        if (lease > maxLeaseValue || lease > txnData.getScaleGracePeriod()) {
            return PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.LEASE_TOO_LARGE).build();
        }

        if (lease + System.currentTimeMillis() > txnData.getMaxExecutionTimeExpiry()) {
            return PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED).build();
        } else {
            Timeout timeout = txnData.getTimeout();
            timeout.cancel();
            Timeout newTimeout = hashedWheelTimer.newTimeout(timeout.task(), lease, TimeUnit.MILLISECONDS);
            txnData.setTimeout(newTimeout);
            return PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.OK).build();
        }

    }

    @Override
    public boolean containsTxn(final String scope, final String stream, final UUID txnId) {
        return map.containsKey(getKey(scope, stream, txnId));
    }

    private Throwable getRealCause(Throwable e) {
        if ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            return e.getCause();
        } else {
            return e;
        }
    }

    private String getKey(final String scope, final String stream, final UUID txid) {
        return scope + "/" + stream + "/" + txid;
    }
}
