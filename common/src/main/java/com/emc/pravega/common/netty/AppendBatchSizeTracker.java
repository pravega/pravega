package com.emc.pravega.common.netty;

public interface AppendBatchSizeTracker {

    void noteAppend(int size);

    void noteAck();

    int getAppendBlockSize();

}