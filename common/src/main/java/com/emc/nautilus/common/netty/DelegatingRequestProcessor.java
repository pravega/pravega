package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.CreateBatch;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.DeleteSegment;
import com.emc.nautilus.common.netty.WireCommands.GetStreamSegmentInfo;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.MergeBatch;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SealSegment;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;

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
