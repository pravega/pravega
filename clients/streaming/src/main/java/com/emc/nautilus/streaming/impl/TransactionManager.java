package com.emc.nautilus.streaming.impl;

import java.util.UUID;

import com.emc.nautilus.streaming.Stream;
import com.emc.nautilus.streaming.Transaction;
import com.emc.nautilus.streaming.TxFailedException;

public interface TransactionManager {

	UUID createTransaction(Stream s, long timeout);

	void commitTransaction(UUID txId) throws TxFailedException;

	boolean dropTransaction(UUID txId);

	Transaction.Status checkTransactionStatus(UUID txId);

}
