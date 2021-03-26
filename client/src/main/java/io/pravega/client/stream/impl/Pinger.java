/**
 * Copyright Pravega Authors.
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
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.common.Exceptions;
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
 * It invokes io.pravega.client.control.impl.Controller#pingTransaction() on the controller to renew the lease of active
 * transactions. Incase of a controller instance not being reachable the controller client takes care of retrying on a
 * different controller instance see io.pravega.client.control.impl.ControllerResolverFactory for details.
 */
@Slf4j
public class Pinger implements AutoCloseable {
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

    Pinger(long txnLeaseMillis, Stream stream, Controller controller, ScheduledExecutorService executor) {
        this.txnLeaseMillis = txnLeaseMillis;
        this.pingIntervalMillis = getPingInterval();
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

    private long getPingInterval() {
        //Provides a good number of attempts: 1 for <4s, 2 for <9s, 3 for <16s, 4 for <25s, ... 10 for <100s
        //while at the same time allowing the interval to grow as the timeout gets larger.
        double targetNumPings = Math.max(1, Math.sqrt(txnLeaseMillis / 1000.0));
        return Math.round(txnLeaseMillis / targetNumPings);
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
                                      Throwable unwrap = Exceptions.unwrap(e);
                                      if (unwrap instanceof StatusRuntimeException && 
                                              ((StatusRuntimeException) unwrap).getStatus().equals(Status.NOT_FOUND)) {
                                          log.info("Ping Transaction for txn ID:{} did not find the transaction", uuid);
                                          completedTxns.add(uuid);
                                      }
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
