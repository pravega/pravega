package com.emc.nautilus.logclient;

import java.nio.ByteBuffer;

//Defines a Stream Reader.
public abstract class SegmentInputStream implements AutoCloseable {
	// Sets the offset for reading from the stream.
	public abstract void setOffset(long offset);

	public abstract long getOffset();
	
	public abstract int available();
	
	public abstract void read(ByteBuffer toFill) throws EndOfSegmentException;

	@Override
	public abstract void close();
}