package com.emc.nautilus.streaming.impl;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.emc.nautilus.logclient.LogOutputStream;
import com.emc.nautilus.logclient.LogSealedExcepetion;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.TxFailedException;

final class LogTransactionImpl<Type> implements LogTransaction<Type> {
	private final Serializer<Type> serializer;
	private final LogOutputStream out;
	private UUID txId;

	LogTransactionImpl(UUID txId, LogOutputStream out, Serializer<Type> serializer) {
		this.txId = txId;
		this.out = out;
		this.serializer = serializer;
	}

	@Override
	public void publish(Type event) throws TxFailedException {
		try {
			ByteBuffer buffer = serializer.serialize(event);
			buffer = DataParser.prependLength(buffer);
			out.write(buffer);
		} catch (LogSealedExcepetion e) {
			throw new TxFailedException();
		}
	}

	@Override
	public UUID getId() {
		return txId;
	}

	@Override
	public void flush() throws TxFailedException {
		try {
			out.flush();
		} catch (LogSealedExcepetion e) {
			throw new TxFailedException();
		}
	}

}