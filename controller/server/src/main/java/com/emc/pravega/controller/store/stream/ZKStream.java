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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata store.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 * <p>
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
class ZKStream extends PersistentStreamBase {
    private static final String CONFIGURATION_PATH = "/streams/%s/configurationPath";
    private static final String SEGMENT_PATH = "/streams/%s/segmentPath";
    private static final String HISTORY_PATH = "/streams/%s/historyPath";
    private static final String INDEX_PATH = "/streams/%s/indexPath";
    private static final String TASK_PATH = "/streams/%s/taskPath";
    private static final String LOCK_PATH = "/streams/%s/lockPath";

    private final CuratorFramework client;

    private final NodeCache configurationCache;
    private final PathChildrenCache segmentCache;
    private final NodeCache indexCache;
    private final NodeCache historyCache;
    private final NodeCache taskCache;

    private final String configurationPath;
    private final String segmentPath;
    private final String segmentChunkPathTemplate;
    private final String historyPath;
    private final String indexPath;
    private final String taskPath;
    private final InterProcessMutex mutex;

    /**
     * single threaded executor to handle a quirk from curator's mutex which requires same
     * thread that acquires the lock to release it
     */
    private final ExecutorService mutexExecutor = Executors.newSingleThreadExecutor();

    public ZKStream(final String name, final String connectionString) {
        super(name);

        client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
        client.start();
        configurationPath = String.format(CONFIGURATION_PATH, name);
        segmentPath = String.format(SEGMENT_PATH, name);
        segmentChunkPathTemplate = segmentPath + "/%s";
        historyPath = String.format(HISTORY_PATH, name);
        indexPath = String.format(INDEX_PATH, name);
        taskPath = String.format(TASK_PATH, name);
        final String lockPath = String.format(LOCK_PATH, name);

        segmentCache = new PathChildrenCache(client, segmentPath, true);
        indexCache = new NodeCache(client, indexPath);
        historyCache = new NodeCache(client, historyPath);
        configurationCache = new NodeCache(client, configurationPath);
        taskCache = new NodeCache(client, taskPath);

        mutex = new InterProcessMutex(client, lockPath);
    }

    public void init() {
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        futures.add(CompletableFuture.supplyAsync(() -> start(configurationCache)));
        futures.add(CompletableFuture.supplyAsync(() -> start(segmentCache)));
        futures.add(CompletableFuture.supplyAsync(() -> start(historyCache)));
        futures.add(CompletableFuture.supplyAsync(() -> start(indexCache)));

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void tearDown() {
        final List<CompletableFuture<Void>> futures = new ArrayList<>();

        futures.add(CompletableFuture.supplyAsync(() -> close(configurationCache)));
        futures.add(CompletableFuture.supplyAsync(() -> close(segmentCache)));
        futures.add(CompletableFuture.supplyAsync(() -> close(historyCache)));
        futures.add(CompletableFuture.supplyAsync(() -> close(indexCache)));

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        client.close();
    }

    @Override
    public CompletableFuture<Void> acquireDistributedLock() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Note: curator mutex has a constraint that lock has to be taken and
                // released in the same thread!!
                mutex.acquire();
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, mutexExecutor);
    }

    @Override
    public CompletableFuture<Void> releaseDistributedLock() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                mutex.release();
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, mutexExecutor);
    }

    @Override
    public CompletableFuture<byte[]> getTaskDataIfExists() {
        return checkExists(taskPath)
                .thenCompose(x -> x == null ? CompletableFuture.<byte[]>completedFuture(null) : getData(taskCache, taskPath));
    }

    @Override
    public CompletableFuture<Void> createTask(final byte[] serialize) {
        return createZNodeIfNotExist(taskPath, serialize).thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Void> deleteTask() {
        return deletePath(taskPath);
    }

    @Override
    public CompletionStage<Void> createConfiguration(final StreamConfiguration configuration, final Create create) {
        return createZNodeIfNotExist(configurationPath, SerializationUtils.serialize(configuration)).thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Void> createSegmentTable(final Create create) {
        return createZNodeIfNotExist(segmentPath).thenApply(x -> null);
    }

    @Override
    CompletableFuture<Void> createSegmentChunk(final int chunkNumber, final byte[] data) {
        return createZNodeIfNotExist(String.format(segmentChunkPathTemplate, chunkNumber), data).thenApply(x -> null);
    }

    @Override
    public CompletionStage<Void> createIndexTable(final Create create) {
        final byte[] indexTable = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
        return createZNodeIfNotExist(indexPath, indexTable).thenApply(x -> null);
    }

    @Override
    public CompletionStage<Void> createHistoryTable(final Create create) {
        final int numSegments = create.getConfiguration().getScalingingPolicy().getMinNumSegments();
        final byte[] historyTable = TableHelper.updateHistoryTable(new byte[0],
                create.getEventTime(),
                IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

        return createZNodeIfNotExist(historyPath, historyTable).thenApply(y -> null);
    }

    @Override
    public CompletableFuture<Void> updateHistoryTable(final byte[] updated) {
        return setData(historyPath, updated).thenApply(x -> null);
    }

    @Override
    public CompletionStage<Void> createSegmentFile(final Create create) {
        final int numSegments = create.getConfiguration().getScalingingPolicy().getMinNumSegments();
        final int chunkFileName = 0;
        final double keyRangeChunk = 1.0 / numSegments;

        final int startingSegmentNumber = 0;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());
        final int toCreate = newRanges.size();

        final byte[] segmentTable = TableHelper.updateSegmentTable(startingSegmentNumber,
                new byte[0],
                toCreate,
                newRanges,
                create.getEventTime()
        );

        return createZNodeIfNotExist(String.format(segmentChunkPathTemplate, chunkFileName), segmentTable).thenApply(y -> null);
    }

    @Override
    public CompletableFuture<Void> setConfigurationData(final StreamConfiguration configuration) {
        return setData(configurationPath, SerializationUtils.serialize(configuration))
                .thenApply(x -> null);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfigurationData() {
        return getData(configurationCache, configurationPath)
                .thenApply(x -> (StreamConfiguration) SerializationUtils.deserialize(x));
    }

    @Override
    public CompletableFuture<Segment> getSegmentRow(final int number) {
        // compute the file name based on segment number
        final int znodeName = number / SegmentRecord.SEGMENT_CHUNK_SIZE;

        return getData(segmentCache, String.format(segmentChunkPathTemplate, znodeName))
                .thenApply(x -> TableHelper.getSegment(number, x));
    }

    @Override
    public CompletableFuture<byte[]> getSegmentTableChunk(final int chunkNumber) {
        return getData(segmentCache, String.format(segmentChunkPathTemplate, chunkNumber));
    }

    @Override
    CompletableFuture<Void> setSegmentTableChunk(final int chunkNumber, final byte[] data) {
        return setData(String.format(segmentChunkPathTemplate, chunkNumber), data).thenApply(x -> null);
    }

    @Override
    public CompletableFuture<List<String>> getSegmentChunks() {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return client.getChildren().forPath(segmentPath);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public CompletableFuture<byte[]> getHistoryTable() {
        return getData(historyCache, historyPath);
    }

    @Override
    public CompletableFuture<byte[]> getIndexTable() {
        return getData(indexCache, indexPath);
    }

    @Override
    CompletableFuture<Void> updateIndexTable(final byte[] updated) {
        return setData(indexPath, updated).thenApply(x -> null);
    }

    private Void start(final NodeCache cache) {
        try {
            cache.start();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void start(final PathChildrenCache cache) {
        try {
            cache.start();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void close(final NodeCache cache) {
        try {
            cache.close();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void close(final PathChildrenCache cache) {
        try {
            cache.close();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<Stat> checkExists(final String path) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return client.checkExists().forPath(path);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private CompletableFuture<Void> deletePath(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.delete().forPath(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private CompletableFuture<byte[]> getData(final NodeCache cache, final String path) {
        if (cache.getCurrentData() != null)
            return CompletableFuture.completedFuture(cache.getCurrentData().getData());
        else return getData(path);
    }

    private CompletableFuture<byte[]> getData(final PathChildrenCache cache, final String path) {
        if (cache.getCurrentData(path) != null)
            return CompletableFuture.completedFuture(cache.getCurrentData(path).getData());
        else return getData(path);
    }

    private CompletableFuture<byte[]> getData(final String path) {
        return checkExists(path)
                .thenApply(x -> {
                    if (x != null) {
                        try {
                            return client.getData().forPath(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else throw new RuntimeException(String.format("path %s not Found", path));
                });
    }

    private CompletableFuture<Stat> setData(final String path, final byte[] data) {
        return checkExists(path)
                .thenApply(x -> {
                    if (x != null) {
                        try {
                            return client.setData().forPath(path, data);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else throw new RuntimeException(String.format("path %s not Found", path));
                });
    }

    private CompletableFuture<String> createZNodeIfNotExist(final String path) {
        return checkExists(path)
                .thenApply(x -> {
                    if (x == null) {
                        try {
                            return client.create().forPath(path);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else return path;
                });
    }

    private CompletableFuture<String> createZNodeIfNotExist(final String path, final byte[] data) {

        return checkExists(path)
                .thenApply(
                        x -> {
                            if (x == null) {
                                try {
                                    return client.create().forPath(path, data);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            } else return path;
                        });
    }

}
