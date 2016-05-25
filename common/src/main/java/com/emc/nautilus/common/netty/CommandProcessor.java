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
import com.emc.nautilus.common.netty.WireCommands.SegmentDeleted;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.netty.WireCommands.SegmentSealed;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.common.netty.WireCommands.SetupRead;
import com.emc.nautilus.common.netty.WireCommands.StreamInfo;
import com.emc.nautilus.common.netty.WireCommands.StreamsSegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

/**
 * Handler for commands from the network. All implementations are expected to be non-blocking.
 * @see FailingCommandProcessor 
 * @see DelegatingCommandProcessor
 */
public interface CommandProcessor {
	void wrongHost(WrongHost wrongHost);
	void segmentIsSealed(SegmentIsSealed segmentIsSealed);
	void endOfStream(EndOfStream endOfStream);
	void noSuchStream(NoSuchStream noSuchStream);
	void noSuchSegment(NoSuchSegment noSuchSegment);
	void noSuchBatch(NoSuchBatch noSuchBatch);
	void setupAppend(SetupAppend setupAppend);
	void appendSetup(AppendSetup appendSetup);
	void appendData(AppendData appendData);
	void dataAppended(DataAppended dataAppended);
	void setupRead(SetupRead setupRead);
	void readSetup(ReadSetup readSetup);
	void readSegment(ReadSegment readSegment);
	void segmentRead(SegmentRead segmentRead);
	void getStreamInfo(GetStreamInfo getStreamInfo);
	void streamInfo(StreamInfo streamInfo);
	void createStreamsSegment(CreateStreamsSegment createStreamsSegment);
	void streamsSegmentCreated(StreamsSegmentCreated streamsSegmentCreated);
	void createBatch(CreateBatch createBatch);
	void batchCreated(BatchCreated batchCreated);
	void mergeBatch(MergeBatch mergeBatch);
	void batchMerged(BatchMerged batchMerged);
	void sealSegment(SealSegment sealSegment);
	void segmentSealed(SegmentSealed segmentSealed);
	void deleteSegment(DeleteSegment deleteSegment);
	void segmentDeleted(SegmentDeleted segmentDeleted);
	void keepAlive(KeepAlive keepAlive);
}