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

import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.DelegationTokenVerifier;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.shared.protocol.netty.AdminRequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Request processor for admin commands issues from Admin CLI.
 *
 * This class is a gateway to allow administrators performing arbitrary operations against Segments for debug and/or
 * recovery purposes. Note that enabling this gateway will allow an administrator to perform destructive operations
 * on data, so it should be used carefully. By default, the Pravega request processor is disabled.
 */
@Slf4j
public class AdminRequestProcessorImpl extends PravegaRequestProcessor implements AdminRequestProcessor {

    //region Constructor

    public AdminRequestProcessorImpl(@NonNull StreamSegmentStore segmentStore, @NonNull TableStore tableStore,
                                     @NonNull TrackedConnection connection, @NonNull DelegationTokenVerifier tokenVerifier) {
        super(segmentStore, tableStore, connection, SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(),
                tokenVerifier, true);
    }

    //endregion

    //region RequestProcessor Implementation

    @Override
    public void hello(WireCommands.Hello hello) {
        log.info("Received hello from connection: {}", getConnection());
        getConnection().send(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
        if (hello.getLowVersion() > WireCommands.WIRE_VERSION || hello.getHighVersion() < WireCommands.OLDEST_COMPATIBLE_VERSION) {
            log.warn("Incompatible wire protocol versions {} from connection {}", hello, getConnection());
            getConnection().close();
        }
    }

    @Override
    public void keepAlive(WireCommands.KeepAlive keepAlive) {
        log.info("Received a keepAlive from connection: {}", getConnection());
        getConnection().send(keepAlive);
    }

    //endregion
}
