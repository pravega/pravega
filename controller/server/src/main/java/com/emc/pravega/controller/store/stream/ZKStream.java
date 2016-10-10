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

import com.emc.pravega.common.concurrent.FutureCollectionHelper;
import com.emc.pravega.controller.store.stream.tables.Create;
import com.emc.pravega.controller.store.stream.tables.Scale;
import com.emc.pravega.controller.store.stream.tables.HistoryRecord;
import com.emc.pravega.controller.store.stream.tables.IndexRecord;
import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.emc.pravega.controller.store.stream.tables.Task;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata store.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 *
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
class ZKStream implements Stream {
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

    private String name;
    private final String configurationPath;
    private final String segmentPath;
    private final String historyPath;
    private final String indexPath;
    private final String taskPath;
    private final InterProcessMutex mutex;

    /**
     * single threaded executor to handle a quirk from curator's mutex which requires same
     * thread that acquires the lock to release it
     */
    private final ExecutorService mutexExecutor = Executors.newSingleThreadExecutor();

    public ZKStream(String name, String connectionString) {
        this.name = name;

        client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(1000, 3));
        client.start();
        configurationPath = String.format(CONFIGURATION_PATH, name);
        segmentPath = String.format(SEGMENT_PATH, name);
        historyPath = String.format(HISTORY_PATH, name);
        indexPath = String.format(INDEX_PATH, name);
        taskPath = String.format(TASK_PATH, name);
        final String lockPath = String.format(LOCK_PATH, name);

        segmentCache = new PathChildrenCache(client, segmentPath, true);
        indexCache = new NodeCache(client, indexPath);
        historyCache = new NodeCache(client, historyPath);
        configurationCache = new NodeCache(client, configurationPath);

        mutex = new InterProcessMutex(client, lockPath);
    }

    @Override
    public String getName() {
        return this.name;
    }

    /***
     * Creates a new stream record in the stream store.
     * Create a new task of type Create.
     * If create task already exists, use that and bring it to completion
     * If no task exists, fall through all create steps. They are all idempotent
     * <p>
     * Create Steps:
     * 0. Take distributed mutex
     * 1. Create task/Fetch existing task
     * 2. Create a new znode to store configuration
     * 3. Create a new znode for segment table.
     * 4. Create a new znode for history table.
     * 5. Create a new znode for index
     * 6. delete task
     * 7. release mutex
     *
     * @param configuration stream configuration.
     * @return
     */
    @Override
    public CompletableFuture<Boolean> create(StreamConfiguration configuration) {
        final int numSegments = configuration.getScalingingPolicy().getMinNumSegments();

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Note: curator mutex has a constraint that lock has to be taken and
                // released in the same thread!!
                mutex.acquire();
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, mutexExecutor)
                .thenCompose(x -> checkExists(taskPath))
                .thenApply(x -> {
                    Create create;
                    if (x != null) {
                        try {
                            Task task = (Task) SerializationUtils.deserialize(client.getData().forPath(taskPath));
                            // if the task is anything other than create, throw error and exit immediately
                            assert task.getType().equals(Create.class);

                            create = task.asCreate();
                            // assert that configuration is same, else fail create attempt
                            assert create.getConfiguration().equals(configuration);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        try {
                            // ensure that a previous create did not complete, if it did, we should throw an exception
                            // TODO: throw appropriate exception to let the caller know that previous attempt to create
                            // completed successfully. This is not a bad error and is recoverable.
                            assert client.checkExists().forPath(configurationPath) == null;
                            create = new Create(System.currentTimeMillis(), configuration);
                            client.create().forPath(taskPath, SerializationUtils.serialize(create));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return create;
                })
                        // Let create object fall through to all future callbacks as their own return values are uninteresting
                .thenCompose(create ->
                        createZNodeIfNotExist(configurationPath, SerializationUtils.serialize(configuration))
                                .thenApply(y -> create))
                .thenCompose(create -> createZNodeIfNotExist(segmentPath).thenApply(y -> create))
                .thenCompose(create -> {
                    final int chunkFileName = 0;
                    final double keyRangeChunk = 1.0 / numSegments;

                    final int startingSegmentNumber = 0;
                    final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                            .boxed()
                            .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                            .collect(Collectors.toList());
                    final int toCreate = newRanges.size();

                    byte[] segmentTable = TableHelper.updateSegmentTable(startingSegmentNumber,
                            new byte[0],
                            toCreate,
                            newRanges,
                            create.getEventTime()
                    );

                    return createZNodeIfNotExist(segmentPath + "/" + chunkFileName, segmentTable).thenApply(y -> create);
                })
                .thenCompose(create -> {
                    byte[] historyTable = TableHelper.updateHistoryTable(new byte[0],
                            create.getEventTime(),
                            IntStream.range(0, numSegments).boxed().collect(Collectors.toList()));

                    return createZNodeIfNotExist(historyPath, historyTable).thenApply(y -> create);
                })
                .thenCompose(create -> {
                    byte[] indexTable = TableHelper.updateIndexTable(new byte[0], create.getEventTime(), 0);
                    return createZNodeIfNotExist(indexPath, indexTable);
                })
                .thenCompose(y -> deletePath(taskPath))
                .handle((ok, ex) -> {
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            mutex.release();
                            return null;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, mutexExecutor);
                    if (ex == null)
                        return true;
                    else {
                        throw new RuntimeException(ex);
                    }
                });
    }

    /**
     * Update configuration in zk at configurationPath
     *
     * @param configuration new stream configuration.
     * @return
     */
    @Override
    public CompletableFuture<Boolean> updateConfiguration(StreamConfiguration configuration) {
        // replace the configurationPath zknode with new configurationPath
        return setData(configurationPath, SerializationUtils.serialize(configuration))
                .thenApply(x -> true);
    }

    /**
     * Fetch configuration from zk at configurationPath
     *
     * @return
     */
    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration() {
        return getData(configurationCache, configurationPath)
                .thenApply(x -> (StreamConfiguration) SerializationUtils.deserialize(x));
    }

    /**
     * Compute correct znode name for the segment chunk that contains entry for this segment.
     * Fetch the segment table chunk and retrieve the segment
     *
     * @param number segment number.
     * @return
     */
    @Override
    public CompletableFuture<Segment> getSegment(int number) {
        // compute the file name based on segment number
        int znodeName = number / SegmentRecord.SEGMENT_CHUNK_SIZE;

        return getData(segmentCache, segmentPath + "/" + znodeName)
                .thenApply(x -> TableHelper.getSegment(number, x));
    }

    /**
     * Given segment number, find its successor candidates and then compute overlaps with its keyrange
     * to find successors
     *
     * @param number segment number.
     * @return
     */
    @Override
    public CompletableFuture<List<Integer>> getSuccessors(int number) {

        CompletableFuture[] futures = new CompletableFuture[3];

        futures[0] = getSegment(number);
        futures[1] = getData(indexCache, indexPath);
        futures[2] = getData(historyCache, historyPath);

        return CompletableFuture.allOf(futures).thenCompose(x -> {

            try {
                final Segment segment = (Segment) futures[0].get();
                byte[] indexTable = (byte[]) futures[1].get();
                byte[] historyTable = (byte[]) futures[2].get();
                CompletableFuture<List<Segment>> segmentSuccessorCandidates = FutureCollectionHelper.sequence(
                        TableHelper.findSegmentSuccessorCandidates(segment,
                                indexTable,
                                historyTable)
                                .stream()
                                .map(this::getSegment)
                                .collect(Collectors.toList()));
                return segmentSuccessorCandidates.thenApply(y -> new ImmutablePair<Segment, List<Segment>>(segment, y));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> TableHelper.getOverlaps(x.getKey(), x.getValue()));
    }

    /**
     * Find predecessor candidates and find overlaps with given segment's key range
     *
     * @param number segment number.
     * @return
     */
    @Override
    public CompletableFuture<List<Integer>> getPredecessors(int number) {
        CompletableFuture[] futures = new CompletableFuture[3];

        futures[0] = getSegment(number);
        futures[1] = getData(indexCache, indexPath);
        futures[2] = getData(historyCache, historyPath);

        return CompletableFuture.allOf(futures).thenCompose(x -> {
            try {
                final Segment segment = (Segment) futures[0].get();
                byte[] indexTable = (byte[]) futures[1].get();
                byte[] historyTable = (byte[]) futures[2].get();
                CompletableFuture<List<Segment>> segmentPredecessorCandidates = FutureCollectionHelper.sequence(
                        TableHelper.findSegmentPredecessorCandidates(segment,
                                indexTable,
                                historyTable)
                                .stream()
                                .map(this::getSegment)
                                .collect(Collectors.toList()));
                return segmentPredecessorCandidates.thenApply(y -> new ImmutablePair<Segment, List<Segment>>(segment, y));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenApply(x -> TableHelper.getOverlaps(x.getKey(), x.getValue()));
    }

    @Override
    public CompletableFuture<List<Integer>> getActiveSegments() {
        return getData(historyCache, historyPath).thenApply(TableHelper::getActiveSegments);
    }

    /**
     * if timestamp is < create time of stream, we will return empty list
     * 1. perform binary searchIndex on index table to find timestamp
     * 2. fetch the record from history table for the pointer in index.
     * Note: index may be stale so we may need to fall through
     * 3. parse the row and return the list of integers
     *
     * @param timestamp point in time.
     * @return
     */
    @Override
    public CompletableFuture<List<Integer>> getActiveSegments(long timestamp) {
        CompletableFuture<byte[]> indexFuture = getData(indexCache, indexPath);

        CompletableFuture<byte[]> historyFuture = getData(historyCache, historyPath);

        return indexFuture.thenCombine(historyFuture,
                (byte[] indexTable, byte[] historyTable) -> TableHelper.getActiveSegments(timestamp,
                        indexTable,
                        historyTable));
    }

    /**
     * Scale and create are two tasks where we update the table. For scale to be legitimate, it has to be
     * preceeded by create. Which means all appropriate tables exist.
     * Scale Steps:
     * 0. Take distributed mutex
     * 1. Scale task/Fetch existing task
     * 2. Verify if new scale input is same as existing scale task.
     * If not, existing takes precedence. TODO: Notify caller!
     * 3. Add new segment information in segment table.
     * Segments could spillover into a new chunk.
     * 4. Add entry into the history table.
     * 5. Add entry into the index table.
     * 6. delete task
     * 7. release mutex
     *
     * @param sealedSegments segments to be sealed
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @return
     */
    @Override
    public CompletableFuture<List<Segment>> scale(List<Integer> sealedSegments,
                                                  List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  long scaleTimestamp) {
        // TODO Question: How do we know if we have lost the lock because of network issue. This lock implementation
        // is an ephemeral node in zk. So connection loss means auto lock release while we continue with our computation
        // following blog is interesting in suggesting solutions:
        // https://fpj.me/2016/02/10/note-on-fencing-and-distributed-locks/
        return CompletableFuture.supplyAsync(() -> {
            try {
                mutex.acquire();
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, mutexExecutor).thenCompose(x -> checkExists(taskPath))
                .thenApply(x -> {
                    Scale scale;
                    if (x != null) {
                        try {
                            final Task task = (Task) SerializationUtils.deserialize(client.getData().forPath(taskPath));
                            // if the task is anything other than scale, throw error and exit immediately
                            assert task.getType().equals(Scale.class);

                            scale = task.asScale();
                            // If existing scale task is different from requested scale task, we will complete the partially
                            // done scale task from the store and ignore the current task. We need to let the caller know
                            // in case we ignore so that there are no surprises.
                            assert scale.getSealedSegments().equals(sealedSegments) &&
                                    scale.getNewRanges().equals(newRanges);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        try {
                            scale = new Scale(sealedSegments, newRanges, scaleTimestamp);
                            client.create().forPath(taskPath, SerializationUtils.serialize(scale));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return scale;
                })
                .thenCompose(scale -> {
                    // Note: we do not know the segment ids from the input. We need to compute the segment id while making
                    // sure we do not create duplicate entries.
                    // create new records in segment table if
                    // No new segment from requested 'newRanges' matches the range for last entry in segment table.
                    // (stronger check would be to check all new segments against last n segments where n is new segment count
                    return CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return client.getChildren().forPath(segmentPath);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .thenCompose(segmentChunks -> {
                                final int latestChunk = segmentChunks.size() - 1;

                                return getData(segmentCache, segmentPath + "/" + latestChunk)
                                        .thenApply(x -> new ImmutablePair<Integer, byte[]>(latestChunk, x));
                            })
                            .thenCompose(latestSegmentData -> {
                                byte[] data = latestSegmentData.right;
                                int latestChunk = latestSegmentData.left;

                                final int startingSegmentNumber = latestChunk *
                                        SegmentRecord.SEGMENT_CHUNK_SIZE +
                                        (data.length / SegmentRecord.SEGMENT_RECORD_SIZE);

                                final int maxSegmentNumberForChunk = (latestChunk + 1) *
                                        SegmentRecord.SEGMENT_CHUNK_SIZE;

                                final int toCreate = Integer.min(maxSegmentNumberForChunk - startingSegmentNumber,
                                        newRanges.size());

                                byte[] segmentTable = TableHelper.updateSegmentTable(startingSegmentNumber,
                                        data,
                                        toCreate,
                                        newRanges,
                                        scale.getScaleTimestamp()
                                );

                                return setData(segmentPath + "/" + 0, segmentTable)
                                        .thenApply(x ->
                                                new ImmutablePair<>(Integer.max(newRanges.size() - toCreate, 0),
                                                        startingSegmentNumber));
                            })
                            .thenCompose(spillover -> {
                                final int startingSegmentNumber = spillover.right;
                                final int chunkFileName = startingSegmentNumber /
                                        SegmentRecord.SEGMENT_RECORD_SIZE;
                                final int toCreate = spillover.left;

                                byte[] segmentTable = TableHelper.updateSegmentTable(startingSegmentNumber,
                                        new byte[0],
                                        toCreate,
                                        newRanges,
                                        scale.getScaleTimestamp()
                                );

                                return createZNodeIfNotExist(segmentPath + "/" + chunkFileName, segmentTable)
                                        .thenApply(x -> startingSegmentNumber);
                            })
                            .thenApply(x -> new ImmutablePair<>(scale, x));
                })
                .thenCompose(pair -> {
                    // update history table if not already updated
                    // fetch last record from history table.
                    // if eventTime is different from scale.scaleTimeStamp do nothing, else create record
                    final Scale scale = pair.left;
                    final int startingSegmentNumber = pair.right;

                    return getData(historyCache, historyPath)
                            .thenCompose(historyTable -> {
                                Optional<HistoryRecord> lastRecordOpt = HistoryRecord.readLatestRecord(historyTable);

                                // scale task is not allowed unless create is done which means at least one
                                // record in history table
                                assert lastRecordOpt.isPresent();

                                HistoryRecord lastRecord = lastRecordOpt.get();

                                int highestSegmentNumber = lastRecord
                                        .getSegments()
                                        .stream()
                                        .max(Comparator.naturalOrder())
                                        .get();
                                List<Integer> newActiveSegments = lastRecord.getSegments();
                                newActiveSegments.removeAll(sealedSegments);
                                newActiveSegments.addAll(
                                        IntStream.range(highestSegmentNumber + 1,
                                                highestSegmentNumber + newRanges.size() + 1)
                                                .boxed()
                                                .collect(Collectors.toList()));
                                if (lastRecord.getEventTime() < scale.getScaleTimestamp()) {
                                    byte[] updated = TableHelper.updateHistoryTable(historyTable,
                                            scale.getScaleTimestamp(),
                                            newActiveSegments);
                                    return setData(historyPath, updated).thenApply(y -> historyTable.length);
                                } else {
                                    CompletableFuture<Integer> empty = new CompletableFuture<>();
                                    empty.complete(historyTable.length);
                                    return empty;
                                }
                            })
                            .thenApply(historyOffset -> new ImmutableTriple<>(scale, startingSegmentNumber, historyOffset));
                })
                .thenCompose(triple -> {
                    // update index if not already updated
                    // fetch last record from index table. if eventTime is same as scale.scaleTimeStamp do nothing,
                    // else create record
                    final Scale scale = triple.left;
                    final int historyOffset = triple.right;
                    final int startingSegmentNumber = triple.middle;

                    return getData(indexCache, indexPath)
                            .thenCompose(indexTable -> {
                                Optional<IndexRecord> lastRecord = IndexRecord.readLatestRecord(indexTable);
                                if (lastRecord.isPresent() && lastRecord.get().getEventTime() < scale.getScaleTimestamp()) {
                                    byte[] updated = TableHelper.updateIndexTable(indexTable,
                                            scale.getScaleTimestamp(),
                                            historyOffset);
                                    return setData(indexPath, updated).thenApply(y -> startingSegmentNumber);
                                } else return new CompletableFuture<Integer>()
                                        .thenApply(y -> startingSegmentNumber);
                            });
                })
                .thenCompose(startingSegmentNumber -> deletePath(taskPath).thenApply(z -> startingSegmentNumber))
                .thenCompose(startingSegmentNumber -> {
                    List<CompletableFuture<Segment>> segments = IntStream.range(startingSegmentNumber,
                            startingSegmentNumber + newRanges.size())
                            .boxed()
                            .map(this::getSegment)
                            .collect(Collectors.<CompletableFuture<Segment>>toList());
                    return FutureCollectionHelper.sequence(segments);
                })
                .handle((ok, ex) -> {
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            mutex.release();
                            return null;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, mutexExecutor);

                    if (ex == null) {
                        return ok;
                    } else {
                        throw new RuntimeException(ex);
                    }
                });
    }

    public void init() {
        List<CompletableFuture<Void>> futures = new ArrayList();

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
        List<CompletableFuture<Void>> futures = new ArrayList();

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

    private CompletableFuture<Stat> checkExists(String path) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return client.checkExists().forPath(path);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private CompletableFuture<Void> deletePath(String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.delete().forPath(path);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private CompletableFuture<byte[]> getData(NodeCache cache, String path) {
        if (cache.getCurrentData() != null)
            return CompletableFuture.completedFuture(cache.getCurrentData().getData());
        else return getData(path);
    }

    private CompletableFuture<byte[]> getData(PathChildrenCache cache, String path) {
        if (cache.getCurrentData(path) != null)
            return CompletableFuture.completedFuture(cache.getCurrentData(path).getData());
        else return getData(path);
    }

    private CompletableFuture<byte[]> getData(String path) {
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

    private CompletableFuture<Stat> setData(String path, byte[] data) {
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

    private CompletableFuture<String> createZNodeIfNotExist(String path) {
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

    private CompletableFuture<String> createZNodeIfNotExist(String path, byte[] data) {

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

    private Void start(NodeCache cache) {
        try {
            cache.start();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void start(PathChildrenCache cache) {
        try {
            cache.start();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void close(NodeCache cache) {
        try {
            cache.close();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Void close(PathChildrenCache cache) {
        try {
            cache.close();
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
