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
package io.pravega.client.stream.mock;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.Flow;
import io.pravega.client.control.impl.CancellableRequest;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionInfo;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ConnectionClosedException;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.client.stream.impl.WriterPosition;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableSegments;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.BucketType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.DeleteSegment;
import io.pravega.shared.protocol.netty.WireCommands.WrongHost;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;

import io.pravega.shared.security.auth.AccessOperation;
import lombok.AllArgsConstructor;
import lombok.Synchronized;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;
import static io.pravega.shared.NameUtils.getScopedReaderGroupName;
import static io.pravega.shared.NameUtils.getStreamForReaderGroup;

@AllArgsConstructor
public class MockController implements Controller {

    private final String endpoint;
    private final int port;
    private final ConnectionPool connectionPool;
    @GuardedBy("$lock")
    private final Map<String, MockScope> createdScopes = new HashMap<>();

    private final Supplier<Long> idGenerator = () -> Flow.create().asLong();
    private final boolean callServer;

    private static class MockScope {
        private final Map<Stream, StreamConfiguration> streams = new HashMap<>();
        private final Map<Stream, Boolean> streamSealStatus = new HashMap<>();
        private final Map<KeyValueTableInfo, KeyValueTableConfiguration> keyValueTables = new HashMap<>();
        private final Map<String, ReaderGroupConfig> readerGroups = new HashMap<>();
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkScopeExists(String scopeName) {
        return CompletableFuture.completedFuture(createdScopes.containsKey(scopeName));
    }

    @Override
    public AsyncIterator<String> listScopes() {
        Set<String> collect = createdScopes.keySet();
        return new AsyncIterator<String>() {
            Object lock = new Object();
            @GuardedBy("lock")
            Iterator<String> iterator = collect.iterator();
            @Override
            public CompletableFuture<String> getNext() {
                String next;
                synchronized (lock) {
                    if (!iterator.hasNext()) {
                        next = null;
                    } else {
                        next = iterator.next();
                    }
                }

                return CompletableFuture.completedFuture(next);
            }
        };
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(String scopeName, String streamName) {
        MockScope mockScope = this.createdScopes.get(scopeName);
        if (mockScope != null) {
            return CompletableFuture.completedFuture(mockScope.streams.containsKey(new StreamImpl(scopeName, streamName)));
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public CompletableFuture<StreamConfiguration> getStreamConfiguration(String scopeName, String streamName) {
        MockScope mockScope = this.createdScopes.get(scopeName);
        Stream stream = Stream.of(scopeName, streamName);
        if (mockScope != null && mockScope.streams.get(stream) != null) {
            return CompletableFuture.completedFuture(mockScope.streams.get(stream));
        } else {
            CompletableFuture<StreamConfiguration> cf = new CompletableFuture<>();
            cf.completeExceptionally(new IllegalStateException("Scope does not exist"));
            return cf;
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> createScope(final String scopeName) {
        if (createdScopes.get(scopeName) != null) {
            return CompletableFuture.completedFuture(false);
        }
        createdScopes.put(scopeName, new MockScope());
        return CompletableFuture.completedFuture(true);
    }

    @Override
    @Synchronized
    public AsyncIterator<Stream> listStreams(String scopeName) {
        return list(scopeName, s -> s.streams.keySet(), Stream::getStreamName);
    }

    @Override
    public AsyncIterator<Stream> listStreamsForTag(String scopeName, String tag) {
        return list(scopeName, ms -> ms.streams.entrySet()
                                               .stream()
                                               .filter(e -> e.getValue().getTags().contains(tag))
                                               .map(Map.Entry::getKey).collect(Collectors.toList()),
                    Stream::getStreamName);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        if (createdScopes.get(scopeName) == null) {
            return CompletableFuture.completedFuture(false);
        }

        if (!createdScopes.get(scopeName).streams.isEmpty()) {
            return Futures.failedFuture(new IllegalStateException("Scope is not empty."));
        }

        createdScopes.remove(scopeName);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> deleteScopeRecursive(String scopeName) {
        if (createdScopes.get(scopeName) == null) {
            return CompletableFuture.completedFuture(false);
        }
        createdScopes.remove(scopeName);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> createStream(String scope, String streamName, StreamConfiguration streamConfig) {
        String markStream = NameUtils.getMarkStreamForStream(streamName);
        StreamConfiguration markStreamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();

        return createStreamInternal(scope, markStream, markStreamConfig)
                .thenCompose(v -> createStreamInternal(scope, streamName, streamConfig));
    }

    private CompletableFuture<Boolean> createStreamInternal(String scope, String streamName, StreamConfiguration streamConfig) {
        return createInScope(scope, new StreamImpl(scope, streamName), streamConfig, s -> s.streams,
                this::getSegmentsForStream, Segment::getScopedName, this::createSegment);
    }

    @Synchronized
    public CompletableFuture<Boolean> createRGStream(String scope, String rgName) {
        String rgStream = NameUtils.getStreamForReaderGroup(rgName);
        StreamConfiguration rgStreamConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        return createStreamInternal(scope, rgStream, rgStreamConfig);
    }

    @Synchronized
    List<Segment> getSegmentsForStream(Stream stream) {
        StreamConfiguration config = getStreamConfig(stream);
        Preconditions.checkArgument(config != null, "Stream " + stream.getScopedName() + " must be created first");
        ScalingPolicy scalingPolicy = config.getScalingPolicy();
        if (scalingPolicy.getScaleType() != ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS) {
            throw new IllegalArgumentException("Dynamic scaling not supported with a mock controller");
        }
        List<Segment> result = new ArrayList<>(scalingPolicy.getMinNumSegments());
        for (int i = 0; i < scalingPolicy.getMinNumSegments(); i++) {
            result.add(new Segment(stream.getScope(), stream.getStreamName(), i));
        }
        return result;
    }

    @Synchronized
    List<Segment> getSegmentsForKeyValueTable(KeyValueTableInfo kvt) {
        KeyValueTableConfiguration config = getKeyValueTableConfig(kvt);
        Preconditions.checkArgument(config != null, "Key-Value Table " + kvt.getScopedName() + " must be created first");
        List<Segment> result = new ArrayList<>(config.getPartitionCount());
        for (int i = 0; i < config.getPartitionCount(); i++) {
            result.add(new Segment(kvt.getScope(), kvt.getKeyValueTableName(), i));
        }
        return result;
    }

    @Synchronized
    List<Segment> getSegmentsForReaderGroup(String scopedRgName) {
        Stream rgStream = Stream.of(scopedRgName);
        return getSegmentsForStream(rgStream);
    }

    @Synchronized
    List<SegmentWithRange> getSegmentsWithRanges(Stream stream) {
        StreamConfiguration config = getStreamConfig(stream);
        Preconditions.checkArgument(config != null, "Stream must be created first");
        ScalingPolicy scalingPolicy = config.getScalingPolicy();
        if (scalingPolicy.getScaleType() != ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS) {
            throw new IllegalArgumentException("Dynamic scaling not supported with a mock controller");
        }
        List<SegmentWithRange> result = new ArrayList<>();
        for (int i = 0; i < scalingPolicy.getMinNumSegments(); i++) {
            result.add(createRange(stream.getScope(), stream.getStreamName(), scalingPolicy.getMinNumSegments(), i));
        }
        return result;
    }

    private StreamConfiguration getStreamConfig(Stream stream) {
        for (MockScope scope : createdScopes.values()) {
            StreamConfiguration sc = scope.streams.get(stream);
            if (sc != null) {
                return sc;
            }
        }
        return null;
    }

    private KeyValueTableConfiguration getKeyValueTableConfig(KeyValueTableInfo kvtInfo) {
        for (MockScope scope : createdScopes.values()) {
            KeyValueTableConfiguration c = scope.keyValueTables.get(kvtInfo);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    private ReaderGroupConfig getReaderGroupConfiguration(String rgStream) {
        for (MockScope scope : createdScopes.values()) {
            ReaderGroupConfig c = scope.readerGroups.get(rgStream);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    @Override
    public CompletableFuture<Boolean> updateStream(String scope, String streamName, StreamConfiguration streamConfig) {
        MockScope mockScope = this.createdScopes.get(scope);
        if (mockScope != null) {
            mockScope.streams.put(Stream.of(scope, streamName), streamConfig);
            return CompletableFuture.completedFuture(true);
        } else {
            return CompletableFuture.completedFuture(false);
        }
    }

    @Override
    public CompletableFuture<ReaderGroupConfig> createReaderGroup(String scopeName, String rgName, ReaderGroupConfig config) {
        createRGStream(scopeName, rgName);
        createInScope(scopeName, getScopedReaderGroupName(scopeName, getStreamForReaderGroup(rgName)), config, s -> s.readerGroups,
                this::getSegmentsForReaderGroup, Segment::getScopedName, this::createSegment);
        return CompletableFuture.completedFuture(config);
    }

    @Override
    public CompletableFuture<ReaderGroupConfig> getReaderGroupConfig(String scope, String rgName) {
        return CompletableFuture.completedFuture(getReaderGroupConfiguration(getScopedReaderGroupName(scope, getStreamForReaderGroup(rgName))));
    }

    @Override
    public CompletableFuture<Boolean> deleteReaderGroup(String scope, String rgName, final UUID readerGroupId) {
        String key = getScopedReaderGroupName(scope, rgName);
        deleteFromScope(scope, key, s -> s.readerGroups, this::getSegmentsForReaderGroup, Segment::getScopedName, this::deleteSegment);
        return deleteStream(scope, getStreamForReaderGroup(rgName));
    }

    @Override
    @Synchronized
    public CompletableFuture<Long> updateReaderGroup(String scopeName, String rgName, ReaderGroupConfig config) {
        String key = getScopedReaderGroupName(scopeName, getStreamForReaderGroup(rgName));
        MockScope scopeMeta = createdScopes.get(scopeName);
        assert scopeMeta != null : "Scope not created";
        assert scopeMeta.readerGroups.containsKey(key) : "ReaderGroup is not created";
        long newGen = scopeMeta.readerGroups.get(key).getGeneration() + 1;
        scopeMeta.readerGroups.replace(key, scopeMeta.readerGroups.get(key),
                ReaderGroupConfig.cloneConfig(config, config.getReaderGroupId(), newGen));
        return CompletableFuture.completedFuture(newGen);
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers(String scope, String streamName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> updateSubscriberStreamCut(String scope, String streamName, String subscriber,
                                                                UUID readerGroupId, long generation, StreamCut streamCut) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> truncateStream(final String scope, final String stream, final StreamCut cut) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> startScale(Stream stream, List<Long> sealedSegments, Map<Double, Double> newKeyRanges) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CancellableRequest<Boolean> scaleStream(Stream stream, List<Long> sealedSegments, Map<Double, Double> newKeyRanges,
                                                   ScheduledExecutorService executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Boolean> checkScaleStatus(Stream stream, int epoch) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> sealStream(String scope, String streamName) {
        MockScope scopeMeta = createdScopes.get(scope);
        assert scopeMeta != null : "Scope not created";
        Stream stream = Stream.of(scope, streamName);
        assert scopeMeta.streams.containsKey(stream) : "Stream not created";
        scopeMeta.streamSealStatus.put(stream, true);
        return CompletableFuture.completedFuture(true);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> deleteStream(String scope, String streamName) {
        return deleteFromScope(scope, new StreamImpl(scope, NameUtils.getMarkStreamForStream(streamName)), s -> s.streams, this::getSegmentsForStream,
                Segment::getScopedName, this::deleteSegment)
                .thenCompose(v -> deleteFromScope(scope, new StreamImpl(scope, streamName), s -> s.streams, this::getSegmentsForStream,
                Segment::getScopedName, this::deleteSegment));
    }

    private boolean createSegment(String name) {
        if (!callServer) {
            return true;
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = createReplyProcessorCreateSegment(result);
        CreateSegment command = new WireCommands.CreateSegment(idGenerator.get(), name, WireCommands.CreateSegment.NO_SCALE, 0, "", 0);
        sendRequestOverNewConnection(command, replyProcessor, result);
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    private boolean createTableSegment(String name) {
        if (!callServer) {
            return true;
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = createReplyProcessorCreateSegment(result);

        WireCommands.CreateTableSegment command = new WireCommands.CreateTableSegment(idGenerator.get(), name, false, 0, "", 0);
        sendRequestOverNewConnection(command, replyProcessor, result);
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    private FailingReplyProcessor createReplyProcessorCreateSegment(CompletableFuture<Boolean> result) {
        return new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {
                result.complete(false);
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {
                result.complete(true);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
    }

    private boolean deleteSegment(String name) {
        if (!callServer) {
            return true;
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = createReplyProcessorDeleteSegment(result);
        DeleteSegment command = new WireCommands.DeleteSegment(idGenerator.get(), name, "");
        sendRequestOverNewConnection(command, replyProcessor, result);
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    private boolean deleteTableSegment(String name) {
        if (!callServer) {
            return true;
        }
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        FailingReplyProcessor replyProcessor = createReplyProcessorDeleteSegment(result);
        WireCommands.DeleteTableSegment command = new WireCommands.DeleteTableSegment(idGenerator.get(), name, false, "");
        sendRequestOverNewConnection(command, replyProcessor, result);
        return getAndHandleExceptions(result, RuntimeException::new);
    }

    private FailingReplyProcessor createReplyProcessorDeleteSegment(CompletableFuture<Boolean> result) {
        return new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                result.complete(true);
            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {
                result.complete(false);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
    }

    @Override
    public CompletableFuture<StreamSegments> getEpochSegments(String scope, String stream, int epoch) {
        return CompletableFuture.completedFuture(getCurrentSegments(new StreamImpl(scope, stream)));
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String scope, String stream) {
        return CompletableFuture.completedFuture(getCurrentSegments(new StreamImpl(scope, stream)));
    }

    private StreamSegments getCurrentSegments(Stream stream) {
        if (isStreamSealed(stream)) {
            return new StreamSegments(new TreeMap<>());
        } else {
            List<Segment> segmentsInStream = getSegmentsForStream(stream);
            TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
            for (int i = 0; i < segmentsInStream.size(); i++) {
                SegmentWithRange s = createRange(stream.getScope(), stream.getStreamName(), segmentsInStream.size(), i);
                segments.put(s.getRange().getHigh(), s);
            }
            return new StreamSegments(segments);
        }
    }

    private KeyValueTableSegments getCurrentSegments(KeyValueTableInfo kvt) {
        List<Segment> segmentsInStream = getSegmentsForKeyValueTable(kvt);
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        for (int i = 0; i < segmentsInStream.size(); i++) {
            SegmentWithRange s = createRange(kvt.getScope(), kvt.getKeyValueTableName(), segmentsInStream.size(), i);
            segments.put(s.getRange().getHigh(), s);
        }
        return new KeyValueTableSegments(segments);
    }

    @Synchronized
    private boolean isStreamSealed(Stream stream) {
        MockScope scopeMeta = createdScopes.get(stream.getScope());
        assert scopeMeta != null : "Scope not created";
        assert scopeMeta.streams.containsKey(stream) : "Stream is not created";
        return scopeMeta.streamSealStatus.getOrDefault(stream, false );
    }

    private SegmentWithRange createRange(String scope, String stream, int numSegments, int segmentNumber) {
        double increment = 1.0 / numSegments;
        return new SegmentWithRange(new Segment(scope, stream, segmentNumber),
                segmentNumber * increment, (segmentNumber + 1) * increment);
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, final String writerId, final Long timestamp, UUID txId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Segment segment : getSegmentsForStream(stream)) {
            futures.add(commitTxSegment(txId, segment));            
        }
        return Futures.allOf(futures);
    }
    
    private CompletableFuture<Void> commitTxSegment(UUID txId, Segment segment) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (!callServer) {
            result.complete(null);
            return result;
        }
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
                result.complete(null);
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {
                result.completeExceptionally(new TxnFailedException("Transaction already aborted."));
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
        sendRequestOverNewConnection(new WireCommands.MergeSegments(idGenerator.get(), segment.getScopedName(),
                NameUtils.getTransactionNameFromId(segment.getScopedName(), txId), ""), replyProcessor, result);
        return result;
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Segment segment : getSegmentsForStream(stream)) {
            futures.add(abortTxSegment(txId, segment));            
        }
        return Futures.allOf(futures);
    }
    
    private CompletableFuture<Void> abortTxSegment(UUID txId, Segment segment) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (!callServer) {
            result.complete(null);
            return result;
        }
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {
                result.completeExceptionally(new TxnFailedException("Transaction already committed."));
            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted transactionAborted) {
                result.complete(null);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
        String transactionName = NameUtils.getTransactionNameFromId(segment.getScopedName(), txId);
        sendRequestOverNewConnection(new DeleteSegment(idGenerator.get(), transactionName, ""), replyProcessor, result);
        return result;
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<List<TransactionInfo>> listCompletedTransactions(Stream stream) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<TxnSegments> createTransaction(final Stream stream, final long lease) {
        UUID txId = UUID.randomUUID();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        StreamSegments currentSegments = getCurrentSegments(stream);
        for (Segment segment : currentSegments.getSegments()) {
            futures.add(createSegmentTx(txId, segment));            
        }
        return Futures.allOf(futures).thenApply(v -> new TxnSegments(currentSegments, txId));
    }

    private CompletableFuture<Void> createSegmentTx(UUID txId, Segment segment) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (!callServer) {
            result.complete(null);
            return result;
        }
        FailingReplyProcessor replyProcessor = new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {
                result.completeExceptionally(new ConnectionClosedException());
            }

            @Override
            public void wrongHost(WrongHost wrongHost) {
                result.completeExceptionally(new UnsupportedOperationException());
            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated transactionCreated) {
                result.complete(null);
            }

            @Override
            public void processingFailure(Exception error) {
                result.completeExceptionally(error);
            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {
                result.completeExceptionally(new AuthenticationException(authTokenCheckFailed.toString()));
            }
        };
        String transactionName = NameUtils.getTransactionNameFromId(segment.getScopedName(), txId);
        sendRequestOverNewConnection(new CreateSegment(idGenerator.get(), transactionName, WireCommands.CreateSegment.NO_SCALE,
                0, "", 0), replyProcessor, result);
        return result;
    }

    @Override
    public CompletableFuture<Transaction.PingStatus> pingTransaction(Stream stream, UUID txId, long lease) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(Stream stream, long timestamp) {
        return CompletableFuture.completedFuture(getSegmentsForStream(stream).stream().collect(Collectors.toMap(s -> s, s -> 0L)));
    }
    
    @Override
    public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
        final Stream segmentStream = Stream.of(segment.getScopedStreamName());
        final CompletableFuture<StreamSegmentsWithPredecessors> result = new CompletableFuture<>();
        if (getStreamConfig(segmentStream) == null) {
            result.completeExceptionally(new RuntimeException("Stream is deleted"));
        } else {
            result.complete(new StreamSegmentsWithPredecessors(Collections.emptyMap(), ""));
        }
        return result;
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSuccessors(StreamCut from) {
        StreamConfiguration configuration = getStreamConfig(from.asImpl().getStream());
        if (configuration.getScalingPolicy().getScaleType() != ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS) {
            throw new IllegalArgumentException("getSuccessors not supported with dynamic scaling on mock controller");
        }
        return CompletableFuture.completedFuture(new StreamSegmentSuccessors(Collections.emptySet(), ""));
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSegments(StreamCut fromStreamCut, StreamCut toStreamCut) {
        Set<Segment> segments = ImmutableSet.<Segment>builder().addAll(fromStreamCut.asImpl().getPositions().keySet())
                                                               .addAll(toStreamCut.asImpl().getPositions().keySet()).build();
        return CompletableFuture.completedFuture(new StreamSegmentSuccessors(segments, ""));
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        return CompletableFuture.completedFuture(new PravegaNodeUri(endpoint, port));
    }

    private <T> void sendRequestOverNewConnection(WireCommand request, ReplyProcessor replyProcessor, CompletableFuture<T> resultFuture) {
        ClientConnection connection = getAndHandleExceptions(connectionPool
            .getClientConnection(Flow.from(((Request) request).getRequestId()), new PravegaNodeUri(endpoint, port), replyProcessor),
                                                             RuntimeException::new);
        resultFuture.whenComplete((result, e) -> {
            connection.close();
        });
        try {
            connection.send(request);
        } catch (ConnectionFailedException cfe) {
            resultFuture.completeExceptionally(cfe);
        }
    }

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(Segment segment) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public void close() {
    }

    @Override
    public CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName, AccessOperation accessOperation) {
        return CompletableFuture.completedFuture("");
    }

    @Override
    public CompletableFuture<Void> noteTimestampFromWriter(String writer, Stream stream, long timestamp, WriterPosition lastWrittenPosition) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeWriter(String writerId, Stream stream) {
        return CompletableFuture.completedFuture(null);
    }

    //region KeyValueTables

    @Override
    @Synchronized
    public CompletableFuture<Boolean> createKeyValueTable(String scope, String kvtName, KeyValueTableConfiguration kvtConfig) {
        return createInScope(scope, new KeyValueTableInfo(scope, kvtName), kvtConfig, s -> s.keyValueTables,
                this::getSegmentsForKeyValueTable, Segment::getKVTScopedName, this::createTableSegment);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> deleteKeyValueTable(String scope, String kvtName) {
        return deleteFromScope(scope, new KeyValueTableInfo(scope, kvtName), s -> s.keyValueTables,
                this::getSegmentsForKeyValueTable, Segment::getKVTScopedName, this::deleteTableSegment);
    }

    @Override
    @Synchronized
    public AsyncIterator<KeyValueTableInfo> listKeyValueTables(String scopeName) {
        return list(scopeName, s -> s.keyValueTables.keySet(), KeyValueTableInfo::getKeyValueTableName);
    }

    @Override
    @Synchronized
    public CompletableFuture<KeyValueTableConfiguration> getKeyValueTableConfiguration(String scope, String kvtName) {
        return CompletableFuture.completedFuture(getKeyValueTableConfig(new KeyValueTableInfo(scope, kvtName)));
    }

    @Override
    @Synchronized
    public CompletableFuture<KeyValueTableSegments> getCurrentSegmentsForKeyValueTable(String scope, String kvtName) {
        return CompletableFuture.completedFuture(getCurrentSegments(new KeyValueTableInfo(scope, kvtName)));
    }

    @Override
    public CompletableFuture<Map<String, List<Integer>>> getControllerToBucketMapping(BucketType bucketType) {
        return CompletableFuture.completedFuture(ImmutableMap.of("controller1", ImmutableList.of(1)));
    }

    @Override
    public void updateStaleValueInCache(String segmentName, PravegaNodeUri errNodeUri) {
    }

    //endregion

    //region Helpers

    @Synchronized
    private <ItemT, ConfigT> CompletableFuture<Boolean> createInScope(String scope, ItemT item, ConfigT config,
                                                                      Function<MockScope, Map<ItemT, ConfigT>> getScopeContents,
                                                                      Function<ItemT, List<Segment>> getSegments,
                                                                      Function<Segment, String> getSegmentName,
                                                                      Consumer<String> createSegment) {
        MockScope s = createdScopes.get(scope);
        if (s == null) {
            return Futures.failedFuture(new IllegalArgumentException("Scope does not exist."));
        }

        Map<ItemT, ConfigT> scopeContents = getScopeContents.apply(s);
        if (scopeContents.containsKey(item)) {
            return CompletableFuture.completedFuture(false);
        }

        scopeContents.put(item, config);
        for (Segment segment : getSegments.apply(item)) {
            createSegment.accept(getSegmentName.apply(segment));
        }
        return CompletableFuture.completedFuture(true);
    }

    @Synchronized
    private <T> CompletableFuture<Boolean> deleteFromScope(String scope, T toDelete, Function<MockScope, Map<T, ?>> getItems,
                                                           Function<T, List<Segment>> getSegments,
                                                           Function<Segment, String> getSegmentName, Consumer<String> deleteSegment) {
        MockScope s = createdScopes.get(scope);
        if (s == null || !getItems.apply(s).containsKey(toDelete)) {
            return CompletableFuture.completedFuture(false);
        }

        for (Segment segment : getSegments.apply(toDelete)) {
            deleteSegment.accept(getSegmentName.apply(segment));
        }
        getItems.apply(s).remove(toDelete);
        return CompletableFuture.completedFuture(true);
    }

    @Synchronized
    private <T> AsyncIterator<T> list(String scopeName, Function<MockScope, Collection<T>> get, Function<T, String> getName) {
        Set<T> collect = get.apply(createdScopes.get(scopeName))
                .stream()
                .filter(s -> !getName.apply(s).startsWith(NameUtils.INTERNAL_NAME_PREFIX))
                .collect(Collectors.toSet());
        return new AsyncIterator<T>() {
            Object lock = new Object();
            @GuardedBy("lock")
            Iterator<T> iterator = collect.iterator();

            @Override
            public CompletableFuture<T> getNext() {
                T next;
                synchronized (lock) {
                    if (!iterator.hasNext()) {
                        next = null;
                    } else {
                        next = iterator.next();
                    }
                }

                return CompletableFuture.completedFuture(next);
            }
        };
    }

    //endregion
}

