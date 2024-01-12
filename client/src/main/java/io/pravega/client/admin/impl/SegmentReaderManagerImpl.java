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
package io.pravega.client.admin.impl;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.SegmentReaderManager;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.util.Retry;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

@Beta
@Slf4j
public class SegmentReaderManagerImpl implements SegmentReaderManager {

    private final Controller controller;
    private final ConnectionPool connectionPool;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;
    private final StreamCutHelper streamCutHelper;
    private final Retry.RetryWithBackoff retryWithBackoff;
    private final ClientConfig clientConfig;

    public SegmentReaderManagerImpl(Controller controller, ClientConfig clientConfig, ConnectionFactory connectionFactory) {
        this(controller, clientConfig, connectionFactory, Retry.withExpBackoff(1, 10, 10, Duration.ofSeconds(30).toMillis()));
    }

    @VisibleForTesting
    public SegmentReaderManagerImpl(Controller controller, ClientConfig clientConfig, ConnectionFactory connectionFactory, Retry.RetryWithBackoff retryWithBackoff) {
        this.controller = controller;
        this.connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        this.inputStreamFactory = new SegmentInputStreamFactoryImpl(controller, connectionPool);
        this.segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(controller, connectionPool);
        this.streamCutHelper = new StreamCutHelper(controller, connectionPool);
        this.retryWithBackoff = retryWithBackoff;
        this.clientConfig = clientConfig;
    }

    @Override
    public List<SegmentReader> getSegmentReaders(final Stream stream, final StreamCut startStreamCut) {
        Preconditions.checkNotNull(stream, "stream");
        return listSegmentReaders(stream, Optional.ofNullable(startStreamCut));
    }

    private List<SegmentReader> listSegmentReaders(final Stream stream, final Optional<StreamCut> startStreamCut) {
        val startCut = startStreamCut.filter(sc -> !sc.equals(StreamCut.UNBOUNDED));
        // if startStreamCut is not provided use the streamCut at the start of the stream.
        CompletableFuture<StreamCut> startSCFuture = startCut.isPresent() ?
                CompletableFuture.completedFuture(startCut.get()) : streamCutHelper.fetchHeadStreamCut(stream);
        StreamCut streamCutRequested = startSCFuture.join();
        Map<Segment, Long> segmentPosition = streamCutRequested.asImpl().getPositions();
        List<SegmentReader> segmentReaderList = new ArrayList<>();
        for(Entry<Segment, Long> entry: segmentPosition.entrySet())
        {
            SegmentReader sg = getSegmentReader(entry);
            segmentReaderList.add(sg);
        }
        return segmentReaderList;
    }

    private SegmentReader getSegmentReader(Entry<Segment, Long> entry) {
        // Based on the entry<key, value> Return SegmentReader from here.

    }

    @Override
    public void close() {
        controller.close();
        connectionPool.close();
    }

}
