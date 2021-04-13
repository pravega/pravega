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
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.RequestProcessor;
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
public class AdminRequestProcessorImpl extends FailingRequestProcessor implements RequestProcessor {

    //region Members

    private final RequestProcessor pravegaRequestProcessor;
    private final TrackedConnection connection;

    //endregion

    //region Constructor

    public AdminRequestProcessorImpl(@NonNull StreamSegmentStore segmentStore, @NonNull TableStore tableStore,
                                     @NonNull TrackedConnection connection, @NonNull DelegationTokenVerifier tokenVerifier) {
        this.connection = connection;
        this.pravegaRequestProcessor = new PravegaRequestProcessor(segmentStore, tableStore, connection,
                SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(), tokenVerifier, false);
    }

    //endregion

    //region RequestProcessor Implementation

    @Override
    public void hello(WireCommands.Hello hello) {
        log.info("Received hello from connection: {}", connection);
        connection.send(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
        if (hello.getLowVersion() > WireCommands.WIRE_VERSION || hello.getHighVersion() < WireCommands.OLDEST_COMPATIBLE_VERSION) {
            log.warn("Incompatible wire protocol versions {} from connection {}", hello, connection);
            connection.close();
        }
    }

    @Override
    public void keepAlive(WireCommands.KeepAlive keepAlive) {
        log.info("Received a keepAlive from connection: {}", connection);
        connection.send(keepAlive);
    }

    @Override
    public void getStreamSegmentInfo(WireCommands.GetStreamSegmentInfo getStreamSegmentInfo) {
        this.pravegaRequestProcessor.getStreamSegmentInfo(getStreamSegmentInfo);
    }

    @Override
    public void setupAppend(WireCommands.SetupAppend setupAppend) {
        this.pravegaRequestProcessor.setupAppend(setupAppend);
    }

    @Override
    public void append(Append appendData) {
        this.pravegaRequestProcessor.append(appendData);
    }

    @Override
    public void readSegment(WireCommands.ReadSegment readSegment) {
        this.pravegaRequestProcessor.readSegment(readSegment);
    }

    @Override
    public void updateSegmentAttribute(WireCommands.UpdateSegmentAttribute updateSegmentAttribute) {
        this.pravegaRequestProcessor.updateSegmentAttribute(updateSegmentAttribute);
    }

    @Override
    public void getSegmentAttribute(WireCommands.GetSegmentAttribute getSegmentAttribute) {
        this.pravegaRequestProcessor.getSegmentAttribute(getSegmentAttribute);
    }

    @Override
    public void createSegment(WireCommands.CreateSegment createStreamsSegment) {
        this.pravegaRequestProcessor.createSegment(createStreamsSegment);
    }

    @Override
    public void updateSegmentPolicy(WireCommands.UpdateSegmentPolicy updateSegmentPolicy) {
        this.pravegaRequestProcessor.updateSegmentPolicy(updateSegmentPolicy);
    }

    @Override
    public void createTableSegment(WireCommands.CreateTableSegment createTableSegment) {
        this.pravegaRequestProcessor.createTableSegment(createTableSegment);
    }

    @Override
    public void deleteTableSegment(WireCommands.DeleteTableSegment deleteSegment) {
        this.pravegaRequestProcessor.deleteTableSegment(deleteSegment);
    }

    @Override
    public void updateTableEntries(WireCommands.UpdateTableEntries tableEntries) {
        this.pravegaRequestProcessor.updateTableEntries(tableEntries);
    }

    @Override
    public void removeTableKeys(WireCommands.RemoveTableKeys tableKeys) {
        this.pravegaRequestProcessor.removeTableKeys(tableKeys);
    }

    @Override
    public void readTable(WireCommands.ReadTable readTable) {
        this.pravegaRequestProcessor.readTable(readTable);
    }

    @Override
    public void readTableKeys(WireCommands.ReadTableKeys readTableKeys) {
        this.pravegaRequestProcessor.readTableKeys(readTableKeys);
    }

    @Override
    public void readTableEntries(WireCommands.ReadTableEntries readTableEntries) {
        this.pravegaRequestProcessor.readTableEntries(readTableEntries);
    }

    @Override
    public void mergeSegments(WireCommands.MergeSegments mergeSegments) {
        this.pravegaRequestProcessor.mergeSegments(mergeSegments);
    }

    @Override
    public void mergeTableSegments(WireCommands.MergeTableSegments mergeSegments) {
        this.pravegaRequestProcessor.mergeTableSegments(mergeSegments);
    }

    @Override
    public void sealSegment(WireCommands.SealSegment sealSegment) {
        this.pravegaRequestProcessor.sealSegment(sealSegment);
    }

    @Override
    public void sealTableSegment(WireCommands.SealTableSegment sealTableSegment) {
        this.pravegaRequestProcessor.sealTableSegment(sealTableSegment);
    }

    @Override
    public void truncateSegment(WireCommands.TruncateSegment truncateSegment) {
        this.pravegaRequestProcessor.truncateSegment(truncateSegment);
    }

    @Override
    public void deleteSegment(WireCommands.DeleteSegment deleteSegment) {
        this.pravegaRequestProcessor.deleteSegment(deleteSegment);
    }

    @Override
    public void readTableEntriesDelta(WireCommands.ReadTableEntriesDelta readTableEntriesDelta) {
        this.pravegaRequestProcessor.readTableEntriesDelta(readTableEntriesDelta);
    }

    //endregion
}
