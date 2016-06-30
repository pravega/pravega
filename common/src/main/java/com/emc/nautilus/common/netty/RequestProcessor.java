package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.*;

public interface RequestProcessor {
	void setupAppend(SetupAppend setupAppend);

	void appendData(AppendData appendData);

	void readSegment(ReadSegment readSegment);

	void getStreamSegmentInfo(GetStreamSegmentInfo getStreamInfo);

	void createSegment(CreateSegment createSegment);

	void createBatch(CreateBatch createBatch);

	void mergeBatch(MergeBatch mergeBatch);

	void sealSegment(SealSegment sealSegment);

	void deleteSegment(DeleteSegment deleteSegment);

	void keepAlive(KeepAlive keepAlive);
}
