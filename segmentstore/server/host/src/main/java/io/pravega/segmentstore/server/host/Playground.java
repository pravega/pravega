/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.LoggerFactory;

/**
 * Playground Test class.
 */
public class Playground {
    private static final WriteArgs[] WRITE_SIZES = new WriteArgs[]{
            new WriteArgs(1000, 100),
            new WriteArgs(1000, 10000),
            new WriteArgs(250, 1000000),
    };
    private static final List<Integer> BOOKIE_PORTS = Arrays.asList(8101); //, 8102, 8103);
    private static final int ZK_PORT = 2181;
    private static final String LEDGERS_PATH = "/test/ledgers";

    static void testBookKeeperSync() throws Exception {
        // Start BK.
        @Cleanup
        val context = start();

        for (WriteArgs args : WRITE_SIZES) {
            byte[] data = new byte[args.writeSize];
            val appendData = new ByteArraySegment(data);

            long[] latencies = new long[args.count];
            val appendFutures = new CompletableFuture[args.count];
            Timer overallTimer = new Timer();
            for (int i = 0; i < args.count; i++) {
                Timer t = new Timer();
                val f = context.log.append(appendData, Duration.ZERO);
                f.join();
                latencies[i] = t.getElapsedNanos();
                appendFutures[i] = f;
            }

            checkOrder(appendFutures);
            printStats(latencies, overallTimer.getElapsedNanos(), args);
        }
    }

    static void testBookKeeperAsyncNoLimit() throws Exception {
        testBookKeeperAsyncWithLimit(10000);
    }

    static void testBookKeeperAsyncWithLimit(int limit) throws Exception {
        // Start BK.
        @Cleanup
        val context = start();

        for (WriteArgs args : WRITE_SIZES) {
            byte[] data = new byte[args.writeSize];
            val appendData = new ByteArraySegment(data);
            long[] latencies = new long[args.count];
            val appendFutures = new CompletableFuture[args.count];
            Timer overallTimer = new Timer();
            Semaphore s = new Semaphore(limit, true);
            for (int i = 0; i < args.count; i++) {
                val index = i;
                Timer t = new Timer();
                val f = runThrottled(
                        () -> context.log.append(appendData, Duration.ZERO)
                                         .thenApplyAsync(r -> {
                                             latencies[index] = t.getElapsedNanos();
                                             return r;
                                         }, context.executor),
                        s, context.executor);
                appendFutures[i] = f;
            }

            CompletableFuture.allOf(appendFutures).join();

            //      checkOrder(appendFutures);
            printStats(latencies, overallTimer.getElapsedNanos(), args);
        }
    }

    static void testBookKeeperAsyncWithLimitAndBatches(int limit) throws Exception {
        // Start BK.
        @Cleanup
        val context = start();

        for (WriteArgs args : WRITE_SIZES) {
            byte[] data = new byte[args.writeSize];
            val appendData = new ByteArraySegment(data);
            long[] latencies = new long[args.count];
            val appendFutures = new CompletableFuture[args.count];
            Timer overallTimer = new Timer();
            Semaphore s = new Semaphore(limit, true);
            for (int i = 0; i < args.count; ) {
                Timer t = new Timer();
                val tempFutures = new CompletableFuture[limit];
                for (int j = 0; j < limit; j++) {
                    val index = i;
                    val f = context.log.append(appendData, Duration.ZERO)
                                       .thenApplyAsync(r -> {
                                           latencies[index] = t.getElapsedNanos();
                                           return r;
                                       }, context.executor);
                    appendFutures[i] = f;
                    tempFutures[j] = f;
                    i++;
                }
                CompletableFuture.allOf(tempFutures).join();
            }

            CompletableFuture.allOf(appendFutures).join();

            //      checkOrder(appendFutures);
            printStats(latencies, overallTimer.getElapsedNanos(), args);
        }
    }

    private static CompletableFuture<LogAddress> runThrottled(Supplier<CompletableFuture<LogAddress>> futureSupplier, Semaphore semaphore, Executor executor) {
        Exceptions.handleInterrupted(() -> semaphore.acquire());
        try {
            val result = futureSupplier.get();
            result.whenCompleteAsync((r, e) -> semaphore.release(), executor);
            return result;
        } catch (Throwable ex) {
            semaphore.release();
            throw ex;
        }
    }

    @SuppressWarnings("unchecked")
    private static void checkOrder(CompletableFuture[] appendFutures) {
        LogAddress lastAddress = null;
        for (CompletableFuture<LogAddress> f : appendFutures) {
            val address = f.join();
            if (lastAddress != null) {
                boolean isInOrder = lastAddress.getSequence() < address.getSequence();
                if (!isInOrder) {
                    System.out.println(String.format("OutOfOrder: '%s' should be before '%s'.", lastAddress, address));
                }
            }

            lastAddress = address;
        }
    }

    private static void printStats(long[] latencies, long elapsedNanos, WriteArgs writeArgs) {
        int overallElapsed = (int) nanosToMillis(elapsedNanos);
        Arrays.sort(latencies);
        double avgLatency = nanosToMillis(Arrays.stream(latencies).average().orElse(-1));
        double p90Latency = nanosToMillis(latencies[(int) (latencies.length * 0.9)]);
        double p95Latency = nanosToMillis(latencies[(int) (latencies.length * 0.95)]);
        double p99Latency = nanosToMillis(latencies[(int) (latencies.length * 0.99)]);
        double maxLatency = nanosToMillis(latencies[latencies.length - 1]);
        double tput = writeArgs.count * writeArgs.writeSize / 1024.0 / 1024.0 / (overallElapsed / 1000.0);
        System.out.println(String.format(
                "WriteCount = %d, WriteSize = %d, Duration = %dms, TPut = %.2fMB/s. Latencies (ms): Avg = %.1f, P90 = %.1f, P95 = %.1f, P99 = %.1f, Max = %.1f",
                writeArgs.count, writeArgs.writeSize, overallElapsed, tput, avgLatency, p90Latency, p95Latency, p99Latency, maxLatency));
    }

    private static double nanosToMillis(double nanos) {
        return nanos / 1000000.0;
    }

    private static Context start() throws Exception {
        // Start BK.
        val serviceRunner = BookKeeperServiceRunner.builder()
                                                   .bookiePorts(BOOKIE_PORTS)
                                                   .ledgersPath(LEDGERS_PATH)
                                                   .zkPort(ZK_PORT)
                                                   .startZk(true)
                                                   .build();
        serviceRunner.start();

        // ZK Client.
        val zkClient = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + ZK_PORT)
                .namespace("test/logs")
                .retryPolicy(new ExponentialBackoffRetry(5000, 5))
                .build();
        zkClient.start();

        // BK Client.
        val config = BookKeeperConfig.builder()
                                     .with(BookKeeperConfig.BK_LEDGER_PATH, LEDGERS_PATH)
                                     .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + ZK_PORT)
                                     .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, BOOKIE_PORTS.size())
                                     .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, BOOKIE_PORTS.size())
                                     .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, BOOKIE_PORTS.size())
                                     .build();
        val executor = Executors.newScheduledThreadPool(10);
        val factory = new BookKeeperLogFactory(config, zkClient, executor);
        factory.initialize();
        val log = factory.createDurableDataLog(0);
        log.initialize(Duration.ZERO);
        return Context.builder()
                      .executor(executor)
                      .serviceRunner(serviceRunner)
                      .zkClient(zkClient)
                      .factory(factory)
                      .log(log)
                      .build();
    }

    @RequiredArgsConstructor
    private static class WriteArgs {
        final int count;
        final int writeSize;
    }

    @Builder
    private static class Context implements AutoCloseable {
        final BookKeeperServiceRunner serviceRunner;
        final CuratorFramework zkClient;
        final ScheduledExecutorService executor;
        final BookKeeperLogFactory factory;
        final DurableDataLog log;

        @Override
        public void close() throws Exception {
            this.log.close();
            this.factory.close();
            this.executor.shutdown();
            this.zkClient.close();
            this.serviceRunner.close();
        }
    }

    public static void main(String[] args) throws Exception {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.OFF);
        //context.reset();

        //testSemaphore();

        try {
            //testBookKeeperSync();
            //testBookKeeperAsyncNoLimit();
            testBookKeeperAsyncWithLimit(10000);
            //testBookKeeperAsyncWithLimitAndBatches(10);
        } catch (Exception ex) {
            System.err.println(ex);
            ex.printStackTrace();
        }
    }

    private static void testSemaphore() {
        int capacity = 5;
        int supply = capacity * 10;
        Semaphore s = new Semaphore(capacity, true);
        @Cleanup("shutdown")
        val executor = Executors.newScheduledThreadPool(supply + 10);
        for (int i = 0; i < supply; i++) {
            val index = i;
            executor.execute(() -> {
                Exceptions.handleInterrupted(() -> {
                    s.acquire();
                    try {
                        System.out.println("Started " + index);
                        Thread.sleep(1000 + new Random().nextInt(1000));
                        System.out.println("Finished " + index);
                    } finally {
                        s.release();
                    }
                });
            });
        }
    }
}
