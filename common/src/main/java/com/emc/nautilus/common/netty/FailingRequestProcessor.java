package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.*;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FailingRequestProcessor implements RequestProcessor {

	@Override
	public void setupAppend(SetupAppend setupAppend) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void appendData(AppendData appendData) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void readSegment(ReadSegment readSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void createSegment(CreateSegment createStreamsSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void createBatch(CreateBatch createBatch) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void mergeBatch(MergeBatch mergeBatch) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void sealSegment(SealSegment sealSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void deleteSegment(DeleteSegment deleteSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void keepAlive(KeepAlive keepAlive) {
		log.debug("Received KeepAlive");
	}

}
