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

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.shared.health.HealthServiceManager;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.RequestProcessor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static io.pravega.shared.metrics.MetricNotifier.NO_OP_METRIC_NOTIFIER;
import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;

/**
 * Manages connections from Admin CLI clients sending debug and/or repair commands.
 */
@Slf4j
public class AdminConnectionListener extends AbstractConnectionListener {
    //region Members
    private final StreamSegmentStore store;
    private final TableStore tableStore;
    private final DelegationTokenVerifier tokenVerifier;
    private final IndexAppendProcessor indexAppendProcessor;

    /**
     * Creates a new instance of the PravegaConnectionListener class.
     *
     * @param enableTls          Whether to enable SSL/TLS.
     * @param enableTlsReload    Whether to reload TLS when the X.509 certificate file is replaced.
     * @param host               The name of the host to listen to.
     * @param port               The port to listen on.
     * @param streamSegmentStore The SegmentStore to delegate all requests to.
     * @param tableStore         The TableStore to delegate all requests to.
     * @param tokenVerifier      The object to verify delegation token.
     * @param certFile           Path to the certificate file to be used for TLS.
     * @param keyFile            Path to be key file to be used for TLS.
     * @param tlsProtocolVersion the version of the TLS protocol
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     */
    public AdminConnectionListener(boolean enableTls, boolean enableTlsReload, String host, int port,
                                   StreamSegmentStore streamSegmentStore, TableStore tableStore,
                                   DelegationTokenVerifier tokenVerifier, String certFile, String keyFile, String[] tlsProtocolVersion,
                                   IndexAppendProcessor indexAppendProcessor) {
        this(enableTls, enableTlsReload, host, port, streamSegmentStore, tableStore, tokenVerifier, certFile, keyFile,
                tlsProtocolVersion, null, indexAppendProcessor);
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
     * @param tokenVerifier      The object to verify delegation token.
     * @param certFile           Path to the certificate file to be used for TLS.
     * @param keyFile            Path to be key file to be used for TLS.
     * @param tlsProtocolVersion the version of the TLS protocol
     * @param healthServiceManager The healService to register new health contributors related to the listeners.
     * @param indexAppendProcessor Index append processor to be used for appending on index segment.
     */
    public AdminConnectionListener(boolean enableTls, boolean enableTlsReload, String host, int port,
                                   StreamSegmentStore streamSegmentStore, TableStore tableStore,
                                   DelegationTokenVerifier tokenVerifier, String certFile, String keyFile, String[] tlsProtocolVersion,
                                   HealthServiceManager healthServiceManager, IndexAppendProcessor indexAppendProcessor) {
        super(enableTls, enableTlsReload, host, port, certFile, keyFile, tlsProtocolVersion, healthServiceManager);
        this.store = Preconditions.checkNotNull(streamSegmentStore, "streamSegmentStore");
        this.tableStore = Preconditions.checkNotNull(tableStore, "tableStore");
        this.tokenVerifier = (tokenVerifier != null) ? tokenVerifier : new PassingTokenVerifier();
        this.indexAppendProcessor = indexAppendProcessor;
    }

    @Override
    public RequestProcessor createRequestProcessor(TrackedConnection c) {
        return new AdminRequestProcessorImpl(store, tableStore, c, tokenVerifier, indexAppendProcessor);
    }

    @Override
    public List<ChannelHandler> createEncodingStack(String connectionName) {
        List<ChannelHandler> stack = new ArrayList<>();
        stack.add(new ExceptionLoggingHandler(connectionName));
        stack.add(new CommandEncoder(null, NO_OP_METRIC_NOTIFIER));
        stack.add(new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4));
        stack.add(new CommandDecoder());
        return stack;
    }
}
