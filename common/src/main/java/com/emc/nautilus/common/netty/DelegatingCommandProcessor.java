package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.BatchCreated;
import com.emc.nautilus.common.netty.WireCommands.BatchMerged;
import com.emc.nautilus.common.netty.WireCommands.CreateBatch;
import com.emc.nautilus.common.netty.WireCommands.CreateStreamsSegment;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.DeleteSegment;
import com.emc.nautilus.common.netty.WireCommands.EndOfStream;
import com.emc.nautilus.common.netty.WireCommands.GetStreamInfo;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.MergeBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.NoSuchStream;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.ReadSetup;
import com.emc.nautilus.common.netty.WireCommands.SealSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.netty.WireCommands.SegmentDeleted;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SegmentSealed;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.SetupRead;
import com.emc.nautilus.common.netty.WireCommands.StreamInfo;
import com.emc.nautilus.common.netty.WireCommands.StreamsSegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

public abstract class DelegatingCommandProcessor implements CommandProcessor {

	public abstract CommandProcessor getNextCommandProcessor();
	
	@Override
	public void wrongHost(WrongHost wrongHost) {
		getNextCommandProcessor().wrongHost(wrongHost);
	}

	@Override
	public void segmentIsSealed(SegmentIsSealed segmentIsSealed) {
		getNextCommandProcessor().segmentIsSealed(segmentIsSealed);
	}

	@Override
	public void endOfStream(EndOfStream endOfStream) {
		getNextCommandProcessor().endOfStream(endOfStream);
	}

	@Override
	public void noSuchStream(NoSuchStream noSuchStream) {
		getNextCommandProcessor().noSuchStream(noSuchStream);
	}

	@Override
	public void noSuchSegment(NoSuchSegment noSuchSegment) {
		getNextCommandProcessor().noSuchSegment(noSuchSegment);
	}

	@Override
	public void noSuchBatch(NoSuchBatch noSuchBatch) {
		getNextCommandProcessor().noSuchBatch(noSuchBatch);
	}

	@Override
	public void setupAppend(SetupAppend setupAppend) {
		getNextCommandProcessor().setupAppend(setupAppend);
	}

	@Override
	public void appendSetup(AppendSetup appendSetup) {
		getNextCommandProcessor().appendSetup(appendSetup);
	}

	@Override
	public void appendData(AppendData appendData) {
		getNextCommandProcessor().appendData(appendData);
	}

	@Override
	public void dataAppended(DataAppended dataAppended) {
		getNextCommandProcessor().dataAppended(dataAppended);
	}

	@Override
	public void setupRead(SetupRead setupRead) {
		getNextCommandProcessor().setupRead(setupRead);
	}
	

	@Override
	public void readSetup(ReadSetup readSetup) {
		getNextCommandProcessor().readSetup(readSetup);
	}

	@Override
	public void readSegment(ReadSegment readSegment) {
		getNextCommandProcessor().readSegment(readSegment);
	}

	@Override
	public void segmentRead(SegmentRead data) {
		getNextCommandProcessor().segmentRead(data);
	}

	@Override
	public void getStreamInfo(GetStreamInfo getStreamInfo) {
		getNextCommandProcessor().getStreamInfo(getStreamInfo);
	}

	@Override
	public void streamInfo(StreamInfo streamInfo) {
		getNextCommandProcessor().streamInfo(streamInfo);
	}

	@Override
	public void createStreamsSegment(CreateStreamsSegment createStreamsSegment) {
		getNextCommandProcessor().createStreamsSegment(createStreamsSegment);
	}

	@Override
	public void streamsSegmentCreated(StreamsSegmentCreated streamsSegmentCreated) {
		getNextCommandProcessor().streamsSegmentCreated(streamsSegmentCreated);
	}

	@Override
	public void createBatch(CreateBatch createBatch) {
		getNextCommandProcessor().createBatch(createBatch);
	}

	@Override
	public void batchCreated(BatchCreated batchCreated) {
		getNextCommandProcessor().batchCreated(batchCreated);
	}

	@Override
	public void mergeBatch(MergeBatch mergeBatch) {
		getNextCommandProcessor().mergeBatch(mergeBatch);
	}

	@Override
	public void batchMerged(BatchMerged batchMerged) {
		getNextCommandProcessor().batchMerged(batchMerged);
	}

	@Override
	public void sealSegment(SealSegment sealSegment) {
		getNextCommandProcessor().sealSegment(sealSegment);
	}

	@Override
	public void segmentSealed(SegmentSealed segmentSealed) {
		getNextCommandProcessor().segmentSealed(segmentSealed);
	}

	@Override
	public void deleteSegment(DeleteSegment deleteSegment) {
		getNextCommandProcessor().deleteSegment(deleteSegment);
	}

	@Override
	public void segmentDeleted(SegmentDeleted segmentDeleted) {
		getNextCommandProcessor().segmentDeleted(segmentDeleted);
	}
	
	@Override
	public void keepAlive(KeepAlive keepAlive) {
		getNextCommandProcessor().keepAlive(keepAlive);
	}

}
