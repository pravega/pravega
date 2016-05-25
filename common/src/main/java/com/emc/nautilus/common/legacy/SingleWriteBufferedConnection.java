package com.emc.nautilus.common.legacy;

import java.nio.ByteBuffer;

import com.emc.nautilus.common.netty.ConnectionFailedException;

public class SingleWriteBufferedConnection implements OldConnection {

	private final Object lock = new Object();
	private final OldConnection connection;
	private CapactyAvailableCallback capacityCallback;
	private ByteBuffer ongoingWrite = null;

	private class CapacityListener implements CapactyAvailableCallback {
		@Override
		public void writePossible() {
			boolean invokeCallback = false;
			synchronized (lock) {
				if (ongoingWrite == null) {
					invokeCallback = true;
				} else {
					try {
						ByteBuffer leftover = writeAsMuchAsPossible(ongoingWrite);
						if (leftover.hasRemaining()) {
							ongoingWrite = leftover;
						} else {
							ongoingWrite = null;
							invokeCallback = true;
						}
					} catch (ConnectionFailedException e) {
						drop();
					}
				}
			}
			if (invokeCallback && capacityCallback != null) {
				capacityCallback.writePossible();
			}
		}
	}

	public SingleWriteBufferedConnection(OldConnection connection) {
		this.connection = connection;
		connection.setCapacityAvailableCallback(new CapacityListener());
	}

	@Override
	public int dataAvailable() {
		synchronized (lock) {
			return connection.dataAvailable();
		}
	}

	@Override
	public int capacityAvailable() {
		synchronized (lock) {
			return ongoingWrite == null ? Integer.MAX_VALUE : 0;
		}
	}

	@Override
	public void write(ByteBuffer buffer) throws ConnectionFailedException {
		synchronized (lock) {
			if (ongoingWrite == null) {
				ByteBuffer leftover = writeAsMuchAsPossible(buffer);
				if (leftover.hasRemaining()) {
					ongoingWrite = leftover;
				}
			} else {
				connection.write(ongoingWrite);
				ongoingWrite = buffer;
			}
		}
	}

	private ByteBuffer writeAsMuchAsPossible(ByteBuffer buffer) throws ConnectionFailedException {
		int bytesToWrite = Math.min(buffer.remaining(), connection.capacityAvailable());
		ByteBuffer writeBuffer = buffer.slice();
		writeBuffer.limit(bytesToWrite);
		connection.write(writeBuffer);
		writeBuffer.position(bytesToWrite);
		writeBuffer.limit(writeBuffer.capacity());
		return writeBuffer;
	}

	@Override
	public void read(ByteBuffer buffer) throws ConnectionFailedException {
		synchronized (lock) {
			connection.read(buffer);
		}
	}

	@Override
	public void drop() {
		synchronized (lock) {
			connection.drop();
			ongoingWrite = null;
		}
	}

	@Override
	public void setCapacityAvailableCallback(CapactyAvailableCallback cb) {
		if (capacityCallback != null) {
			throw new IllegalStateException("Callback already set");
		}
		capacityCallback = cb;
	}

	@Override
	public void setDataAvailableCallback(DataAvailableCallback cb) {
		connection.setDataAvailableCallback(cb);
	}

}
