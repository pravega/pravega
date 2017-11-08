/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Stream;
import io.pravega.common.concurrent.Futures;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.Futures.getException;

@Slf4j
public class TxnKeepAlive implements AutoCloseable {

    private final List<UUID> txnList = new CopyOnWriteArrayList<>();
    private final long txnLeaseMillis;
    private final Stream stream;
    private final Controller controller;
    private final ScheduledExecutorService executor;
    private final long pingIntervalMillis;
    private final AtomicBoolean isStarted = new AtomicBoolean();
    @GuardedBy("$lock")
    private ScheduledFuture<?> pingTxnTask;

    private static final


    public TxnKeepAlive(EventWriterConfig config, Stream stream, Controller controller, ScheduledExecutorService internalExecutor) {
        this.txnLeaseMillis = config.getTransactionTimeoutTime();
        this.pingIntervalMillis = getPingInterval(txnLeaseMillis);
        this.stream = stream;
        this.controller = controller;
        this.executor = internalExecutor;
    }

    void startTxnKeepAlive(UUID txnID) {
        txnList.add(txnID);
        startPeriodicPingTxn();
    }

    void stopTxnKeepAlive(UUID txnID) {
        txnList.remove(txnID);
    }

    /*
        Transaction pings are sent
     */
    private long getPingInterval(long txnLeaseMillis) {
        //Ping interval cannot be less than KeepAlive task interval of 10seconds.
        double ping = txnLeaseMillis * 0.7;
        Longs.
        //method to fetch the ping interval.
        // all computation and boundary check will be performed here.

        return txnLeaseMillis;
    }

    @Synchronized
    private void startPeriodicPingTxn() {
        if (!isStarted.get()) {
            pingTxnTask = executor.scheduleAtFixedRate(this::pingTransactions, 10, this.pingIntervalMillis,
                    TimeUnit.MILLISECONDS);
            isStarted.set(true);
        }
    }
    /*
        Ping all the transactions present in the list.
        Controller client performs retries in case of a failures. On a failure the particular transaction is removed
        from the ping list.
     */

    private void pingTransactions() {
        Map<UUID, CompletableFuture<Void>> pingResult =
                txnList.stream().collect(Collectors.toMap(Function.identity(),
                        uuid -> controller.pingTransaction(stream, uuid, txnLeaseMillis)));
        Futures.allOf(pingResult.values())
               .thenRun(() -> pingResult.entrySet().stream().filter(e -> !Futures.isSuccessful(e.getValue()))
                                        .forEach(e -> {
                                            log.warn("Ping Transaction for txn ID:{} failed", e.getKey(), getException(e.getValue()));
                                            txnList.remove(e.getKey()); //failed ping txn, remove it.
                                        }));
    }

    @Override
    public void close() {

        //resource Cleanup.
    }

    /*
    Questions
    1. DO we need sychronized
    2. Idempotent operations.
    3. Do we need a close method. yes.

     */

    public static void main(String[] args) {
    }
}
