package com.emc.nautilus.common.netty;

public class ConnectionFailedException extends Exception {

	public ConnectionFailedException() {
		super();
	}
	
	public ConnectionFailedException(String string) {
		super(string);
	}
	
	public ConnectionFailedException(Throwable throwable) {
		super(throwable);
	}

}
