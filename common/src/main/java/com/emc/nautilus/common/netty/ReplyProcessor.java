package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.*;

public interface ReplyProcessor {
	void wrongHost(WrongHost wrongHost);

	void segmentAlreadyExists(SegmentAlreadyExists segmentAlreadyExists);

	void segmentIsSealed(SegmentIsSealed segmentIsSealed);

	void noSuchSegment(NoSuchSegment noSuchSegment);

	void noSuchBatch(NoSuchBatch noSuchBatch);

	void appendSetup(AppendSetup appendSetup);

	void dataAppended(DataAppended dataAppended);

	void segmentRead(SegmentRead segmentRead);

	void streamSegmentInfo(StreamSegmentInfo streamInfo);

	void segmentCreated(SegmentCreated segmentCreated);

	void batchCreated(BatchCreated batchCreated);

	void batchMerged(BatchMerged batchMerged);

	void segmentSealed(SegmentSealed segmentSealed);

	void segmentDeleted(SegmentDeleted segmentDeleted);

	void keepAlive(KeepAlive keepAlive);
}
