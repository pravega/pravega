package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.BatchCreated;
import com.emc.nautilus.common.netty.WireCommands.BatchMerged;
import com.emc.nautilus.common.netty.WireCommands.CreateBatch;
import com.emc.nautilus.common.netty.WireCommands.CreateStreamsSegment;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.DeleteSegment;
import com.emc.nautilus.common.netty.WireCommands.GetStreamInfo;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.MergeBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.NoSuchStream;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SealSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentDeleted;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.netty.WireCommands.SegmentSealed;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.StreamInfo;
import com.emc.nautilus.common.netty.WireCommands.StreamsSegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

public class FailingCommandProcessor implements CommandProcessor {

	@Override
	public void wrongHost(WrongHost wrongHost) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
		throw new IllegalStateException("Unexpected operation");
	}

//	@Override
//	public void endOfStream(EndOfStream endOfStream) {
//		throw new IllegalStateException("Unexpected operation");
//	}

	@Override
	public void noSuchStream(NoSuchStream noSuchStream) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void noSuchSegment(NoSuchSegment noSuchSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void noSuchBatch(NoSuchBatch noSuchBatch) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void setupAppend(SetupAppend setupAppend) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void appendSetup(AppendSetup appendSetup) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void appendData(AppendData appendData) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void dataAppended(DataAppended dataAppended) {
		throw new IllegalStateException("Unexpected operation");
	}

//	@Override
//	public void setupRead(SetupRead setupRead) {
//		throw new IllegalStateException("Unexpected operation");
//	}
//	
//
//	@Override
//	public void readSetup(ReadSetup readSetup) {
//		throw new IllegalStateException("Unexpected operation");
//	}

	@Override
	public void readSegment(ReadSegment readSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentRead(SegmentRead data) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void getStreamInfo(GetStreamInfo getStreamInfo) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void streamInfo(StreamInfo streamInfo) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void createStreamsSegment(CreateStreamsSegment createStreamsSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void streamsSegmentCreated(StreamsSegmentCreated streamsSegmentCreated) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void createBatch(CreateBatch createBatch) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void batchCreated(BatchCreated batchCreated) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void mergeBatch(MergeBatch mergeBatch) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void batchMerged(BatchMerged batchMerged) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void sealSegment(SealSegment sealSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentSealed(SegmentSealed segmentSealed) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void deleteSegment(DeleteSegment deleteSegment) {
		throw new IllegalStateException("Unexpected operation");
	}

	@Override
	public void segmentDeleted(SegmentDeleted segmentDeleted) {
		throw new IllegalStateException("Unexpected operation");
	}
	
	@Override
	public void keepAlive(KeepAlive keepAlive) {
		throw new IllegalStateException("Unexpected operation");
	}

}
