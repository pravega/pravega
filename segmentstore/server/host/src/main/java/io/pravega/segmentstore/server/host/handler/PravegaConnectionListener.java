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
package io.pravega.segmentstore.server.host.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import io.pravega.shared.health.HealthServiceManager;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.RequestProcessor;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.segmentstore.server.store.ServiceConfig.TLS_PROTOCOL_VERSION;
import static io.pravega.shared.metrics.MetricNotifier.NO_OP_METRIC_NOTIFIER;
import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;

/**
 * Hands off any received data from a client to the CommandProcessor.
 */
@Slf4j
public final class PravegaConnectionListener extends AbstractConnectionListener {
    //region Members
    private final StreamSegmentStore store;
    private final TableStore tableStore;
    private final SegmentStatsRecorder statsRecorder;
    private final TableSegmentStatsRecorder tableStatsRecorder;

    private final DelegationTokenVerifier tokenVerifier;
    private final ScheduledExecutorService tokenExpiryHandlerExecutor; // Used for running token expiry handling tasks.

    private final boolean replyWithStackTraceOnError;
    private final IndexAppendProcessor indexAppendProcessor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the PravegaConnectionListener class listening on localhost with no StatsRecorder.
     *
     * @param enableTls           Whether to enable SSL/TLS.
     * @param port                The port to listen on.
     * @param streamSegmentStore  The SegmentStore to delegate all requests to.
     * @param tableStore          The SegmentStore to delegate all requests to.
     * @param tokenExpiryExecutor The executor to be used for running token expiration handling tasks.
     * @param tlsProtocolVersion the version of the TLS protocol
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     */
    @VisibleForTesting
    public PravegaConnectionListener(boolean enableTls, int port, StreamSegmentStore streamSegmentStore,
                                     TableStore tableStore, ScheduledExecutorService tokenExpiryExecutor,
                                     String[] tlsProtocolVersion, IndexAppendProcessor indexAppendProcessor) {
        this(enableTls, false, "localhost", port, streamSegmentStore, tableStore,
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), new PassingTokenVerifier(), null,
                null, true, tokenExpiryExecutor, tlsProtocolVersion, null, indexAppendProcessor);
    }

    /**
     * Creates a new instance of the PravegaConnectionListener class listening on localhost with no StatsRecorder.
     *
     * @param enableTls           Whether to enable SSL/TLS.
     * @param port                The port to listen on.
     * @param streamSegmentStore  The SegmentStore to delegate all requests to.
     * @param tableStore          The SegmentStore to delegate all requests to.
     * @param tokenExpiryExecutor The executor to be used for running token expiration handling tasks.
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     */
    @VisibleForTesting
    public PravegaConnectionListener(boolean enableTls, int port, StreamSegmentStore streamSegmentStore,
                                     TableStore tableStore, ScheduledExecutorService tokenExpiryExecutor, IndexAppendProcessor indexAppendProcessor) {
        this(enableTls, port, streamSegmentStore, tableStore, tokenExpiryExecutor,
                TLS_PROTOCOL_VERSION.getDefaultValue().split(","), indexAppendProcessor);
    }

    /**
     * Creates a new instance of the PravegaConnectionListener class with HealthServiceManager.
     *
     * @param enableTls          Whether to enable SSL/TLS.
     * @param enableTlsReload    Whether to reload TLS when the X.509 certificate file is replaced.
     * @param host               The name of the host to listen to.
     * @param port               The port to listen on.
     * @param streamSegmentStore The SegmentStore to delegate all requests to.
     * @param tableStore         The TableStore to delegate all requests to.
     * @param statsRecorder      (Optional) A StatsRecorder for Metrics for Stream Segments.
     * @param tableStatsRecorder (Optional) A Table StatsRecorder for Metrics for Table Segments.
     * @param tokenVerifier      The object to verify delegation token.
     * @param certFile           Path to the certificate file to be used for TLS.
     * @param keyFile            Path to be key file to be used for TLS.
     * @param replyWithStackTraceOnError Whether to send a server-side exceptions to the client in error messages.
     * @param executor           The executor to be used for running token expiration handling tasks.
     * @param tlsProtocolVersion the version of the TLS protocol
     * @param healthServiceManager The healthService to register new health contributors related to the listeners.
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     */
    public PravegaConnectionListener(boolean enableTls, boolean enableTlsReload, String host, int port, StreamSegmentStore streamSegmentStore, TableStore tableStore,
                                     SegmentStatsRecorder statsRecorder, TableSegmentStatsRecorder tableStatsRecorder,
                                     DelegationTokenVerifier tokenVerifier, String certFile, String keyFile,
                                     boolean replyWithStackTraceOnError, ScheduledExecutorService executor, String[] tlsProtocolVersion,
                                     HealthServiceManager healthServiceManager, IndexAppendProcessor indexAppendProcessor) {
        super(enableTls, enableTlsReload, host, port, certFile, keyFile, tlsProtocolVersion, healthServiceManager);
        this.store = Preconditions.checkNotNull(streamSegmentStore, "streamSegmentStore");
        this.tableStore = Preconditions.checkNotNull(tableStore, "tableStore");
        this.statsRecorder = Preconditions.checkNotNull(statsRecorder, "statsRecorder");
        this.tableStatsRecorder = Preconditions.checkNotNull(tableStatsRecorder, "tableStatsRecorder");
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
        this.tokenVerifier = (tokenVerifier != null) ? tokenVerifier : new PassingTokenVerifier();
        this.tokenExpiryHandlerExecutor = executor;
        this.indexAppendProcessor = indexAppendProcessor;
    }

    /**
     * Creates a new instance of the PravegaConnectionListener class.
     *
     * @param enableTls          Whether to enable SSL/TLS.
     * @param enableTlsReload    Whether to reload TLS when the X.509 certificate file is replaced.
     * @param host               The name of the host to listen to.
     * @param port               The port to listen on.
     * @param streamSegmentStore The SegmentStore to delegate all requests to.
     * @param tableStore         The TableStore to delegate all requests to.
     * @param statsRecorder      (Optional) A StatsRecorder for Metrics for Stream Segments.
     * @param tableStatsRecorder (Optional) A Table StatsRecorder for Metrics for Table Segments.
     * @param tokenVerifier      The object to verify delegation token.
     * @param certFile           Path to the certificate file to be used for TLS.
     * @param keyFile            Path to be key file to be used for TLS.
     * @param replyWithStackTraceOnError Whether to send a server-side exceptions to the client in error messages.
     * @param executor           The executor to be used for running token expiration handling tasks.
     * @param tlsProtocolVersion the version of the TLS protocol
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     */
    public PravegaConnectionListener(boolean enableTls, boolean enableTlsReload, String host, int port, StreamSegmentStore streamSegmentStore, TableStore tableStore,
                                     SegmentStatsRecorder statsRecorder, TableSegmentStatsRecorder tableStatsRecorder,
                                     DelegationTokenVerifier tokenVerifier, String certFile, String keyFile,
                                     boolean replyWithStackTraceOnError, ScheduledExecutorService executor,
                                     String[] tlsProtocolVersion, IndexAppendProcessor indexAppendProcessor) {
        this(enableTls, enableTlsReload, host, port, streamSegmentStore, tableStore, statsRecorder, tableStatsRecorder,
                 tokenVerifier, certFile, keyFile, replyWithStackTraceOnError, executor, tlsProtocolVersion, null, indexAppendProcessor);
    }

    @Override
    public RequestProcessor createRequestProcessor(TrackedConnection c) {
        PravegaRequestProcessor prp = new PravegaRequestProcessor(store, tableStore, c, statsRecorder,
                tableStatsRecorder, tokenVerifier, replyWithStackTraceOnError, indexAppendProcessor);
        return new AppendProcessor(store, c, prp, statsRecorder, tokenVerifier, replyWithStackTraceOnError,
                tokenExpiryHandlerExecutor, indexAppendProcessor);
    }

    @Override
    public List<ChannelHandler> createEncodingStack(String connectionName) {
        List<ChannelHandler> stack = new ArrayList<>();
        stack.add(new ExceptionLoggingHandler(connectionName));
        stack.add(new CommandEncoder(null, NO_OP_METRIC_NOTIFIER));
        stack.add(new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4));
        stack.add(new CommandDecoder());
        stack.add(new AppendDecoder());
        return stack;
    }

    //endregion

}
