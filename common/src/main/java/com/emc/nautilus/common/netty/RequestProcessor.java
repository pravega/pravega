package com.emc.nautilus.common.netty;

import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.CreateBatch;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.DeleteSegment;
import com.emc.nautilus.common.netty.WireCommands.GetStreamInfo;
import com.emc.nautilus.common.netty.WireCommands.KeepAlive;
import com.emc.nautilus.common.netty.WireCommands.MergeBatch;
import com.emc.nautilus.common.netty.WireCommands.ReadSegment;
import com.emc.nautilus.common.netty.WireCommands.SealSegment;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;

public interface RequestProcessor {
	void setupAppend(SetupAppend setupAppend);
	void appendData(AppendData appendData);
	void readSegment(ReadSegment readSegment);
	void getStreamInfo(GetStreamInfo getStreamInfo);
	void createSegment(CreateSegment createSegment);
	void createBatch(CreateBatch createBatch);
	void mergeBatch(MergeBatch mergeBatch);
	void sealSegment(SealSegment sealSegment);
	void deleteSegment(DeleteSegment deleteSegment);
	void keepAlive(KeepAlive keepAlive);
}
