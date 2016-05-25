package com.emc.nautilus.streaming.impl;

import com.emc.nautilus.logclient.EndOfLogException;
import com.emc.nautilus.streaming.LogId;

public interface LogConsumer<Type> {
	LogId getLogId();
	Type getNextEvent(long timeout) throws EndOfLogException;
	long getOffset();
	void setOffset(long offset);
	void close();
}
