/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import lombok.val;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiKeyReaderWriterSchedulerTests {
    @Test
    public void testParallelReads() {
        val scheduler = new MultiKeyReaderWriterScheduler();
        val array = new MultiKeyReaderWriterScheduler.MultiKeyReaderWriterAsyncLock[10];
        val seq = new int[10];
        val futures = new CompletableFuture[10];
        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            val readLock = scheduler.getReadLock(new String[]{"common"});
            val n = i;
            futures[i] = readLock.lock()
                    .thenApplyAsync(v -> {
                        val c = counter.getAndIncrement();
                        array[c] = readLock;
                        seq[c] = n;
                        return null;
                    }).whenCompleteAsync((v, ex) -> {
                        readLock.unlock();
                    });
        }
        CompletableFuture.allOf(futures).join();
    }

    @Test
    public void testParallelWrites() {
        val scheduler = new MultiKeyReaderWriterScheduler();
        val array = new MultiKeyReaderWriterScheduler.MultiKeyReaderWriterAsyncLock[10];
        val seq = new int[10];
        val futures = new CompletableFuture[10];
        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            val writeLock = scheduler.getWriteLock(new String[]{"key" + i});
            val n = i;
            futures[i] = writeLock.lock()
                    .thenApplyAsync(v -> {
                        val c = counter.getAndIncrement();
                        array[c] = writeLock;
                        seq[c] = n;
                        return null;
                    }).whenCompleteAsync((v, ex) -> {
                        writeLock.unlock();
                    });
        }
        CompletableFuture.allOf(futures).join();
    }

    @Test
    public void testSequentialWrites() {
        val scheduler = new MultiKeyReaderWriterScheduler();
        val array = new MultiKeyReaderWriterScheduler.MultiKeyReaderWriterAsyncLock[10];
        val seq = new int[10];
        val futures = new CompletableFuture[10];
        AtomicInteger counter = new AtomicInteger();
        for (int i = 0; i < 10; i++) {
            val writeLock = scheduler.getWriteLock(new String[]{"common"});
            val n = i;
            futures[i] = writeLock.lock()
                    .thenApplyAsync(v -> {
                        val c = counter.getAndIncrement();
                        array[c] = writeLock;
                        seq[c] = n;
                        return null;
                    }).whenCompleteAsync((v, ex) -> {
                        writeLock.unlock();
                    });
        }
        CompletableFuture.allOf(futures).join();
    }

    @Test
    public void testParallelReadsAfterWrite() {
        val scheduler = new MultiKeyReaderWriterScheduler();
        val array = new MultiKeyReaderWriterScheduler.MultiKeyReaderWriterAsyncLock[11];
        val seq = new int[11];
        val futures = new CompletableFuture[11];
        AtomicInteger counter = new AtomicInteger();
        val writeLock = scheduler.getWriteLock(new String[]{"common"});
        futures[0] = writeLock.lock()
                .thenApplyAsync(v -> {
                    val c = counter.getAndIncrement();
                    array[c] = writeLock;
                    seq[c] = 0;
                    return null;
                }).whenCompleteAsync((v, ex) -> {
                    writeLock.unlock();
                });
        for (int i = 1; i < 11; i++) {
            val readLock = scheduler.getReadLock(new String[]{"common"});
            val n = i;
            futures[i] = readLock.lock()
                    .thenApplyAsync(v -> {
                        val c = counter.getAndIncrement();
                        array[c] = readLock;
                        seq[c] = n;
                        return null;
                    }).whenCompleteAsync((v, ex) -> {
                        readLock.unlock();
                    });
        }
        CompletableFuture.allOf(futures).join();
    }

    @Test
    public void testParallelReadsAfterFailingWrite() {
        val scheduler = new MultiKeyReaderWriterScheduler();
        val array = new MultiKeyReaderWriterScheduler.MultiKeyReaderWriterAsyncLock[11];
        val seq = new int[11];
        val futures = new CompletableFuture[11];
        AtomicInteger counter = new AtomicInteger();
        val writeLock = scheduler.getWriteLock(new String[]{"common"});
        futures[0] = writeLock.lock()
                .thenApplyAsync(v -> {
                    val c = counter.getAndIncrement();
                    array[c] = writeLock;
                    seq[c] = 0;
                    throw new CompletionException(new Exception("throw"));
                }).whenCompleteAsync((v, ex) -> {
                    writeLock.unlock();
                });
        for (int i = 1; i < 11; i++) {
            val readLock = scheduler.getReadLock(new String[]{"common"});
            val n = i;
            futures[i] = readLock.lock()
                    .thenApplyAsync(v -> {
                        val c = counter.getAndIncrement();
                        array[c] = readLock;
                        seq[c] = n;
                        return null;
                    }).whenCompleteAsync((v, ex) -> {
                        readLock.unlock();
                    });
        }

        CompletableFuture.allOf(futures).handle((v, e) -> {
            return null;
        }).join();

    }

    @Test
    public void testWriteAfterParallelReads() {
        val scheduler = new MultiKeyReaderWriterScheduler();
        val array = new MultiKeyReaderWriterScheduler.MultiKeyReaderWriterAsyncLock[11];
        val seq = new int[11];
        val futures = new CompletableFuture[11];
        AtomicInteger counter = new AtomicInteger();

        for (int i = 0; i < 10; i++) {
            val readLock = scheduler.getReadLock(new String[]{"common"});
            val n = i;
            futures[i] = readLock.lock()
                    .thenApplyAsync(v -> {
                        val c = counter.getAndIncrement();
                        array[c] = readLock;
                        seq[c] = n;
                        return null;
                    }).whenCompleteAsync((v, ex) -> {
                        readLock.unlock();
                    });
        }

        val writeLock = scheduler.getWriteLock(new String[]{"common"});
        futures[10] = writeLock.lock()
                .thenApplyAsync(v -> {
                    val c = counter.getAndIncrement();
                    array[c] = writeLock;
                    seq[c] = 10;
                    return null;
                }).whenCompleteAsync((v, ex) -> {
                    writeLock.unlock();
                });
        CompletableFuture.allOf(futures).join();
    }

    @Test
    public void testWriteAfterFailingParallelReads() {
        val scheduler = new MultiKeyReaderWriterScheduler();
        val array = new MultiKeyReaderWriterScheduler.MultiKeyReaderWriterAsyncLock[11];
        val seq = new int[11];
        val futures = new CompletableFuture[11];
        AtomicInteger counter = new AtomicInteger();

        for (int i = 0; i < 10; i++) {
            val readLock = scheduler.getReadLock(new String[]{"common"});
            val n = i;
            futures[i] = readLock.lock()
                    .thenApplyAsync(v -> {
                        val c = counter.getAndIncrement();
                        array[c] = readLock;
                        seq[c] = n;
                        throw new CompletionException(new Exception("throw"));
                    }).whenCompleteAsync((v, ex) -> {
                        readLock.unlock();
                    });
        }

        val writeLock = scheduler.getWriteLock(new String[]{"common"});
        futures[10] = writeLock.lock()
                .thenApplyAsync(v -> {
                    val c = counter.getAndIncrement();
                    array[c] = writeLock;
                    seq[c] = 10;
                    return null;
                }).whenCompleteAsync((v, ex) -> {
                    writeLock.unlock();
                });
        CompletableFuture.allOf(futures)
                .handle((v, e) -> null).join();
    }

    @Test
    public void testReadWritesInLoop() {
        val scheduler = new MultiKeyReaderWriterScheduler();
        val array = new MultiKeyReaderWriterScheduler.MultiKeyReaderWriterAsyncLock[1100];
        val seq = new int[1100];
        val futures = new CompletableFuture[1100];
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger writerCount = new AtomicInteger();
        int k = 0;
        for (int j = 0; j < 100; j++) {
            for (int i = 0; i < 10; i++) {
                val readLock = scheduler.getReadLock(new String[]{"common"});
                val n = k++;
                futures[n] = readLock.lock()
                        .thenApplyAsync(v -> {
                            val w = writerCount.get();
                            val c = counter.getAndIncrement();
                            array[c] = readLock;
                            seq[c] = n;
                            return null;
                        }).whenCompleteAsync((v, ex) -> {
                            readLock.unlock();
                        });
            }

            val writeLock = scheduler.getWriteLock(new String[]{"common"});
            val n = k++;
            futures[n] = writeLock.lock()
                    .thenApplyAsync(v -> {
                        val w = writerCount.getAndIncrement();
                        val c = counter.getAndIncrement();
                        array[c] = writeLock;
                        seq[c] = n;
                        return null;
                    }).whenCompleteAsync((v, ex) -> {
                        writeLock.unlock();
                    });
        }
        CompletableFuture.allOf(futures).join();
    }

}
