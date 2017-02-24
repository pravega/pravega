/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rpc.v1;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.controller.stream.api.v1.TxnId;
import com.emc.pravega.controller.stream.api.v1.TxnState;
import com.emc.pravega.controller.stream.api.v1.TxnStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.impl.ModelHelper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;

/**
 * Synchronous controller service implementation.
 */
public class ControllerServiceSyncImpl implements com.emc.pravega.controller.stream.api.v1.ControllerService.Iface {

    private final ControllerService controllerService;
    private final Executor executor;

    public ControllerServiceSyncImpl(final StreamMetadataStore streamStore,
                                     final HostControllerStore hostStore,
                                     final StreamMetadataTasks streamMetadataTasks,
                                     final StreamTransactionMetadataTasks streamTransactionMetadataTasks,
                                     final SegmentHelper segmentHelper,
                                     final Executor executor) {
        this.executor = executor;
        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, segmentHelper, this.executor);
    }

    /**
     * Create the stream metadata in the metadata streamStore.
     * Start with creation of minimum number of segments.
     * Asynchronously call createSegment on pravega hosts notifying them about new segments in the stream.
     */
    @Override
    public CreateStreamStatus createStream(final StreamConfig streamConfig) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.createStream(ModelHelper.encode(streamConfig),
                System.currentTimeMillis()), RuntimeException::new);
    }

    @Override
    public UpdateStreamStatus alterStream(final StreamConfig streamConfig) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public UpdateStreamStatus sealStream(String scope, String stream) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.sealStream(scope, stream), RuntimeException::new);
    }

    @Override
    public List<SegmentRange> getCurrentSegments(final String scope, final String stream) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getCurrentSegments(scope, stream), RuntimeException::new);
    }

    @Override
    public NodeUri getURI(final SegmentId segment) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getURI(segment), RuntimeException::new);
    }

    @Override
    public boolean isSegmentValid(final String scope, final String stream, final int segmentNumber) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.isSegmentValid(scope, stream, segmentNumber), RuntimeException::new);
    }

    @Override
    public List<Position> getPositions(final String scope, final String stream, final long timestamp, final int count) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getPositions(scope, stream, timestamp, count), RuntimeException::new);
    }

    @Override
    public Map<SegmentId, List<Integer>> getSegmentsImmediatlyFollowing(SegmentId segment) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getSegmentsImmediatlyFollowing(segment), RuntimeException::new);
    }

    @Override
    public ScaleResponse scale(final String scope, final String stream, final List<Integer> sealedSegments, final Map<Double, Double> newKeyRanges, final long scaleTimestamp) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.scale(scope, stream, sealedSegments, newKeyRanges, scaleTimestamp), RuntimeException::new);
    }

    @Override
    public TxnId createTransaction(final String scope, final String stream) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.createTransaction(scope, stream), RuntimeException::new);
    }

    @Override
    public TxnStatus commitTransaction(final String scope, final String stream, final TxnId txnid) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.commitTransaction(scope, stream, txnid),
                RuntimeException::new);
    }

    @Override
    public TxnStatus abortTransaction(final String scope, final String stream, final TxnId txnid) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.abortTransaction(scope, stream, txnid), RuntimeException::new);
    }

    @Override
    public TxnState checkTransactionStatus(final String scope, final String stream, final TxnId txnid) throws
            TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.checkTransactionStatus(scope, stream, txnid), RuntimeException::new);
    }

    /**
     * Controller Service Sync API to create scope.
     *
     * @param scope Name of scope to be created.
     * @return Status of create scope.
     * @throws TException exception class for Thrift.
     */
    @Override
    public CreateScopeStatus createScope(String scope) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.createScope(scope), RuntimeException::new);
    }

    /**
     * Controller Service Async API to delete scope.
     *
     * @param scope Name of scope to be deleted.
     * @return Status of delete scope.
     * @throws TException exception class for Thrift.
     */
    @Override
    public DeleteScopeStatus deleteScope(String scope) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.deleteScope(scope), RuntimeException::new);
    }
}
