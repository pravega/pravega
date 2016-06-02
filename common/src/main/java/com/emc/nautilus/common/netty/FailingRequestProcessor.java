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
