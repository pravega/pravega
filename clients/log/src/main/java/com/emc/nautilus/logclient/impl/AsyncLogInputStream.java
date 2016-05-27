package com.emc.nautilus.logclient.impl;

import java.util.concurrent.Future;

import com.emc.nautilus.common.netty.WireCommands.SegmentRead;

abstract class AsyncLogInputStream implements AutoCloseable {
	
	public abstract Future<SegmentRead> read(long offset, int length);

	@Override
	public abstract void close();
}