package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.BatchCreated;
import com.emc.nautilus.common.netty.WireCommands.BatchMerged;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.NoSuchBatch;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.SegmentDeleted;
import com.emc.nautilus.common.netty.WireCommands.SegmentIsSealed;
import com.emc.nautilus.common.netty.WireCommands.SegmentRead;
import com.emc.nautilus.common.netty.WireCommands.SegmentSealed;
import com.emc.nautilus.common.netty.WireCommands.StreamInfo;
import com.emc.nautilus.common.netty.WireCommands.WrongHost;

public interface ReplyProcessor {
	void wrongHost(WrongHost wrongHost);
	void segmentIsSealed(SegmentIsSealed segmentIsSealed);
	void noSuchSegment(NoSuchSegment noSuchSegment);
	void noSuchBatch(NoSuchBatch noSuchBatch);
	
	void appendSetup(AppendSetup appendSetup);
	void dataAppended(DataAppended dataAppended);
	void segmentRead(SegmentRead segmentRead);
	void streamInfo(StreamInfo streamInfo);
	void segmentCreated(SegmentCreated segmentCreated);
	void batchCreated(BatchCreated batchCreated);
	void batchMerged(BatchMerged batchMerged);
	void segmentSealed(SegmentSealed segmentSealed);
	void segmentDeleted(SegmentDeleted segmentDeleted);
	void keepAlive(KeepAlive keepAlive);
}
