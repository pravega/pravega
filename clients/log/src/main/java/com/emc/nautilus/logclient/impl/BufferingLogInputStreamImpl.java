package com.emc.nautilus.logclient.impl;

import java.nio.ByteBuffer;

import com.emc.nautilus.logclient.EndOfLogException;
import com.emc.nautilus.logclient.LogInputStream;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BufferingLogInputStreamImpl extends LogInputStream {

	private final LogInputStream input;
	
	@Override
	public void setOffset(long offset) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getOffset() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int available() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ByteBuffer read(int length) throws EndOfLogException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		input.close();
	}

	
	
}
