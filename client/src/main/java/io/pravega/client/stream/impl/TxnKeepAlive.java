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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Stream;
import io.pravega.common.concurrent.Futures;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.Futures.getException;

/**
 * TxnKeepAlive is used to send KeepAlives to renew the transaction lease for active transactions.
 * It invokes io.pravega.client.stream.impl.Controller#pingTransaction() on the controller to renew the lease of active
 * transactions. Incase of a controller instance not being reachable the controller client takes care of retrying on a
 * different controller instance see io.pravega.client.stream.impl.ControllerResolverFactory for details.
 */
@Slf4j
public class TxnKeepAlive implements AutoCloseable {
    private static final double KEEP_ALIVE_INTERVAL_FACTOR = 0.7; //keepalive interval = factor * txn lease time.

    private final Stream stream;
    private final Controller controller;
    private final long txnLeaseMillis;
    private final long pingIntervalMillis;
    private final ScheduledExecutorService executor; // it is not owned by this class, hence won't be part of close()
    private final List<UUID> txnList = Collections.synchronizedList(new ArrayList<>());
    @GuardedBy("$lock")
    private ScheduledFuture<?> pingTxnTask;

    TxnKeepAlive(EventWriterConfig config, Stream stream, Controller controller, ScheduledExecutorService
            internalExecutor) {
        this.txnLeaseMillis = config.getTransactionTimeoutTime();
        this.pingIntervalMillis = getKeepAliveInterval(txnLeaseMillis);
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

    private long getKeepAliveInterval(long txnLeaseMillis) {
        double keepAliveInterval = txnLeaseMillis * KEEP_ALIVE_INTERVAL_FACTOR;
        if (keepAliveInterval < TimeUnit.SECONDS.toMillis(10)) {
            log.warn("Transaction Keep Alive interval is less than 10 seconds(lower bound)");
        }
        //Ping interval cannot be less than KeepAlive task interval of 10seconds.
        return Math.max(TimeUnit.SECONDS.toMillis(10), (long) keepAliveInterval);
    }

    @Synchronized
    private void startPeriodicPingTxn() {
        if (pingTxnTask == null) {
            log.info("Starting Transaction keep alive at an interval of {}ms ", this.pingIntervalMillis);
            pingTxnTask = executor.scheduleAtFixedRate(this::pingTransactions, 10, this.pingIntervalMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    /*
        Ping all the transactions present in the list. Controller client performs retries in case of a failures. On a
         failure the particular transaction it is removed from the ping list.
     */
    private void pingTransactions() {
        log.info("Start sending transaction keepAlive.");
        Map<UUID, CompletableFuture<Void>> pingResult =
                txnList.stream().collect(Collectors.toMap(Function.identity(),
                        uuid -> controller.pingTransaction(stream, uuid, txnLeaseMillis)));
        Futures.allOf(pingResult.values())
               .whenComplete((v, ex) -> pingResult.entrySet().stream()
                                                  .filter(e -> !Futures.isSuccessful(e.getValue()))
                                                  .forEach(e -> {
                                                      log.warn("Ping Transaction for txn ID:{} failed", e.getKey(),
                                                              getException(e.getValue()));
                                                      txnList.remove(e.getKey()); //ping txn failed, remove it.
                                                  }));
        log.trace("Completed sending transaction keepAlive.");
    }

    @Override
    public void close() {
        log.info("Closing Transaction keep alive periodic task");
        ScheduledFuture<?> pingTxnTaskReference = pingTxnTask;
        if (pingTxnTaskReference != null) {
            pingTxnTaskReference.cancel(true);
        }
    }
}
