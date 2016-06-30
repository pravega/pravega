package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.*;

public abstract class DelegatingRequestProcessor implements RequestProcessor {

	public abstract RequestProcessor getNextRequestProcessor();

	@Override
	public void setupAppend(SetupAppend setupAppend) {
		getNextRequestProcessor().setupAppend(setupAppend);
	}

	@Override
	public void appendData(AppendData appendData) {
		getNextRequestProcessor().appendData(appendData);
	}

	@Override
	public void readSegment(ReadSegment readSegment) {
		getNextRequestProcessor().readSegment(readSegment);
	}

	@Override
	public void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo) {
		getNextRequestProcessor().getStreamSegmentInfo(getStreamInfo);
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
	public void keepAlive(KeepAlive keepAlive) {
		getNextRequestProcessor().keepAlive(keepAlive);
	}

}
