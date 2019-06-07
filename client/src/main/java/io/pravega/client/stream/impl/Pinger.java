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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.Exceptions.unwrap;

/**
 * Pinger is used to send pings to renew the transaction lease for active transactions.
 * It invokes io.pravega.client.stream.impl.Controller#pingTransaction() on the controller to renew the lease of active
 * transactions. Incase of a controller instance not being reachable the controller client takes care of retrying on a
 * different controller instance see io.pravega.client.stream.impl.ControllerResolverFactory for details.
 */
@Slf4j
public class Pinger implements AutoCloseable {
    private static final double PING_INTERVAL_FACTOR = 0.5; //ping interval = factor * txn lease time.
    private static final long MINIMUM_PING_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

    private final Stream stream;
    private final Controller controller;
    private final long txnLeaseMillis;
    private final long pingIntervalMillis;
    private final ScheduledExecutorService executor;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final Set<UUID> txnList = new HashSet<>();
    @Getter(value = AccessLevel.PACKAGE)
    @VisibleForTesting
    @GuardedBy("lock")
    private final Set<UUID> completedTxns = new HashSet<>();
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private final AtomicReference<ScheduledFuture<?>> scheduledFuture = new AtomicReference<>();

    Pinger(EventWriterConfig config, Stream stream, Controller controller, ScheduledExecutorService executor) {
        this.txnLeaseMillis = config.getTransactionTimeoutTime();
        this.pingIntervalMillis = getPingInterval(txnLeaseMillis);
        this.stream = stream;
        this.controller = controller;
        this.executor = executor;
    }

    void startPing(UUID txnID) {
        synchronized (lock) {
            txnList.add(txnID);
        }
        startPeriodicPingTxn();
    }

    void stopPing(UUID txnID) {
        synchronized (lock) {
            txnList.remove(txnID);
        }
    }

    private long getPingInterval(long txnLeaseMillis) {
        double pingInterval = txnLeaseMillis * PING_INTERVAL_FACTOR;
        if (pingInterval < MINIMUM_PING_INTERVAL_MS) {
            log.warn("Transaction ping interval is less than 10 seconds(lower bound)");
        }
        //Ping interval cannot be less than KeepAlive task interval of 10seconds.
        return Math.max(MINIMUM_PING_INTERVAL_MS, (long) pingInterval);
    }

    private void startPeriodicPingTxn() {
        if (!isStarted.getAndSet(true)) {
            log.info("Starting Pinger at an interval of {}ms ", this.pingIntervalMillis);
            // scheduleAtFixedRate ensure that there are no concurrent executions of the command, pingTransactions()
            scheduledFuture.set(executor.scheduleAtFixedRate(this::pingTransactions, 10, this.pingIntervalMillis, TimeUnit.MILLISECONDS));
        }
    }

    /**
     *  Ping all the transactions present in the list. Controller client performs retries in case of a failures. On a
     *  failure the particular transaction it is removed from the ping list.
     */
    private void pingTransactions() {
        log.info("Start sending transaction pings.");
        synchronized (lock) {
            txnList.removeAll(completedTxns);  // remove completed transactions from the pingable transaction list.
            completedTxns.clear();
            txnList.forEach(uuid -> {
                try {
                    log.debug("Sending ping request for txn ID: {} with lease: {}", uuid, txnLeaseMillis);
                    controller.pingTransaction(stream, uuid, txnLeaseMillis)
                              .whenComplete((status, e) -> {
                                  if (e != null) {
                                      log.warn("Ping Transaction for txn ID:{} failed", uuid, unwrap(e));
                                  } else if (Transaction.PingStatus.ABORTED.equals(status) || Transaction.PingStatus.COMMITTED.equals(status)) {
                                      completedTxns.add(uuid);
                                  }
                              });
                } catch (Exception e) {
                    // Suppressing exception to prevent future pings from not being executed.
                    log.warn("Encountered exception when attempting to ping transactions", e);
                }
            });
        }
        log.trace("Completed sending transaction pings.");
    }

    @Override
    public void close() {
        log.info("Closing Pinger periodic task");
        ScheduledFuture<?> future = scheduledFuture.getAndSet(null);
        if (future != null) {
            future.cancel(false);
        }
    }
}
