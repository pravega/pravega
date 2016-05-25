package com.emc.nautilus.streaming;

public interface Transaction<Type> {
	enum Status {
		COMMITTED,
		OPEN,
		DROPPED
	}
	void publish(Type event) throws TxFailedException;
	void commit() throws TxFailedException;
	void drop();
	Status checkStatus();
}