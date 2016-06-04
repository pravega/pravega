package com.emc.nautilus.streaming.impl;

import java.util.UUID;

import com.emc.nautilus.streaming.TxFailedException;

public interface LogTransaction<Type> {
	UUID getId();
	void publish(Type event) throws TxFailedException;
	void flush() throws TxFailedException;
}