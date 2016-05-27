package com.emc.nautilus.common.netty.server;

import com.emc.nautilus.common.netty.CommandProcessor;
import com.emc.nautilus.common.netty.DelegatingCommandProcessor;
import com.emc.nautilus.common.netty.WireCommands.CreateBatch;
import com.emc.nautilus.common.netty.WireCommands.CreateStreamsSegment;
import com.emc.nautilus.common.netty.WireCommands.DeleteSegment;
import com.emc.nautilus.common.netty.WireCommands.GetStreamInfo;
import com.emc.nautilus.common.netty.WireCommands.MergeBatch;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SealSegment;

public class LogServiceCommandProcessor extends DelegatingCommandProcessor {

	private final CommandProcessor next;
	
	LogServiceCommandProcessor(CommandProcessor next) {
		this.next = next;
	}

	@Override
	public void readSegment(ReadSegment readSegment) {
		getNextCommandProcessor().readSegment(readSegment);
	}

	@Override
	public void getStreamInfo(GetStreamInfo getStreamInfo) {
		getNextCommandProcessor().getStreamInfo(getStreamInfo);
	}

	@Override
	public void createStreamsSegment(CreateStreamsSegment createStreamsSegment) {
		getNextCommandProcessor().createStreamsSegment(createStreamsSegment);
	}
	
	@Override
	public void createBatch(CreateBatch createBatch) {
		getNextCommandProcessor().createBatch(createBatch);
	}

	@Override
	public void mergeBatch(MergeBatch mergeBatch) {
		getNextCommandProcessor().mergeBatch(mergeBatch);
	}

	@Override
	public void sealSegment(SealSegment sealSegment) {
		getNextCommandProcessor().sealSegment(sealSegment);
	}

	@Override
	public void deleteSegment(DeleteSegment deleteSegment) {
		getNextCommandProcessor().deleteSegment(deleteSegment);
	}

	@Override
	public CommandProcessor getNextCommandProcessor() {
		return next;
	}
	
}
