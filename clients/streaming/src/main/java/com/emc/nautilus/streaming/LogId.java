package com.emc.nautilus.streaming;

public class LogId {
	int number;
	public String getQualifiedName() {
		return null;//TODO
	}
	/**
	 * @return True if this log is a replacement or partial replacement for the one passed.
	 */
	public boolean succeeds(LogId other) {
		return false; //TODO
	}
}
