package com.emc.nautilus.logclient;

import java.io.InputStream;

//Defines a Stream Reader.
public abstract class LogInputStream extends InputStream implements AutoCloseable {
	// Sets the offset for reading from the stream.
	public abstract void setOffset(long offset);

	public abstract long getOffset();

}