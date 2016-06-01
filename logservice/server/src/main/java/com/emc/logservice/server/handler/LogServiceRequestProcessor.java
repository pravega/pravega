package com.emc.logservice.server.handler;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.contracts.SegmentProperties;
import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.nautilus.common.netty.FailingRequestProcessor;
import com.emc.nautilus.common.netty.RequestProcessor;
import com.emc.nautilus.common.netty.ServerConnection;
import com.emc.nautilus.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.StreamSegmentInfo;

public class LogServiceRequestProcessor extends FailingRequestProcessor implements RequestProcessor {

	private static final Duration TIMEOUT = Duration.ofMinutes(1);

	private final StreamSegmentStore segmentStore;

	private final ServerConnection connection;

	public LogServiceRequestProcessor(StreamSegmentStore segmentStore, ServerConnection connection) {
		this.segmentStore = segmentStore;
		this.connection = connection;
	}

	@Override
	public void readSegment(ReadSegment readSegment) {
		CompletableFuture<ReadResult> future = segmentStore.read(readSegment.getSegment(), readSegment.getOffset(),
				readSegment.getSuggestedLength(), TIMEOUT);
		future.thenRun(new Runnable() {
			@Override
			public void run() {
				// TODO: Return data...
				// This really should stream the data out in multiple results as
				// it is available.
			}
		});
	}

	@Override
	public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamSegmentInfo) {
		String segmentName = getStreamSegmentInfo.getSegmentName();
		CompletableFuture<SegmentProperties> future = segmentStore
				.getStreamSegmentInfo(segmentName, TIMEOUT);
		future.thenRun(new Runnable() {
			@Override
			public void run() {
				try {
					SegmentProperties properties = future.getNow(null);
					StreamSegmentInfo result = new StreamSegmentInfo(properties.getName(), true, properties.isSealed(),
							properties.isDeleted(), properties.getLastModified().getTime(), properties.getLength());
					connection.sendAsync(result);
				} catch (CompletionException e) {
					connection.sendAsync(new StreamSegmentInfo(segmentName, false, true, true, 0, 0));
				}
			}
		});
	}

//	@Override
//	public void createSegment(CreateSegment createStreamsSegment) {
//		getNextRequestProcessor().createSegment(createStreamsSegment);
//	}
//
//	@Override
//	public void createBatch(CreateBatch createBatch) {
//		getNextRequestProcessor().createBatch(createBatch);
//	}
//
//	@Override
//	public void mergeBatch(MergeBatch mergeBatch) {
//		getNextRequestProcessor().mergeBatch(mergeBatch);
//	}
//
//	@Override
//	public void sealSegment(SealSegment sealSegment) {
//		getNextRequestProcessor().sealSegment(sealSegment);
//	}
//
//	@Override
//	public void deleteSegment(DeleteSegment deleteSegment) {
//		getNextRequestProcessor().deleteSegment(deleteSegment);
//	}

}
