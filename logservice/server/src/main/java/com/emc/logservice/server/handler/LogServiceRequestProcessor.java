package com.emc.nautilus.common.netty.server;

import com.emc.nautilus.common.netty.DelegatingRequestProcessor;
import com.emc.nautilus.common.netty.RequestProcessor;
import com.emc.nautilus.common.netty.WireCommands.CreateBatch;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.DeleteSegment;
import com.emc.nautilus.common.netty.WireCommands.GetStreamInfo;
import com.emc.nautilus.common.netty.WireCommands.MergeBatch;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SealSegment;

public class LogServiceRequestProcessor extends DelegatingRequestProcessor {

	private final RequestProcessor next;
	
	LogServiceRequestProcessor(RequestProcessor next) {
		this.next = next;
	}

	@Override
	public void readSegment(ReadSegment readSegment) {
		getNextRequestProcessor().readSegment(readSegment);
	}

	@Override
	public void getStreamInfo(GetStreamInfo getStreamInfo) {
		getNextRequestProcessor().getStreamInfo(getStreamInfo);
	}

	@Override
	public void createSegment(CreateSegment createStreamsSegment) {
		getNextRequestProcessor().createSegment(createStreamsSegment);
	}
	
	@Override
	public void createBatch(CreateBatch createBatch) {
		getNextRequestProcessor().createBatch(createBatch);
	}

	@Override
	public void mergeBatch(MergeBatch mergeBatch) {
		getNextRequestProcessor().mergeBatch(mergeBatch);
	}

	@Override
	public void sealSegment(SealSegment sealSegment) {
		getNextRequestProcessor().sealSegment(sealSegment);
	}

	@Override
	public void deleteSegment(DeleteSegment deleteSegment) {
		getNextRequestProcessor().deleteSegment(deleteSegment);
	}

	@Override
	public RequestProcessor getNextRequestProcessor() {
		return next;
	}
	
}
