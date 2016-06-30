package com.emc.nautilus.streaming.impl;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.emc.nautilus.logclient.SegmentOutputStream;
import com.emc.nautilus.logclient.SegmentSealedExcepetion;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.TxFailedException;

final class SegmentTransactionImpl<Type> implements SegmentTransaction<Type> {
	private final Serializer<Type> serializer;
	private final SegmentOutputStream out;
	private final UUID txId;

	SegmentTransactionImpl(UUID txId, SegmentOutputStream out, Serializer<Type> serializer) {
		this.txId = txId;
		this.out = out;
		this.serializer = serializer;
	}

	@Override
	public void publish(Type event) throws TxFailedException {
		try {
			ByteBuffer buffer = serializer.serialize(event);
			out.write(buffer, null);
		} catch (SegmentSealedExcepetion e) {
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
		} catch (SegmentSealedExcepetion e) {
			throw new TxFailedException();
		}
	}

}