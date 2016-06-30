package com.emc.nautilus.streaming;

public interface Consumer<T> {
	T getNextEvent(long timeout);

	ConsumerConfig getConfig();

	Position getPosition();

	void setPosition(Position state);
}
