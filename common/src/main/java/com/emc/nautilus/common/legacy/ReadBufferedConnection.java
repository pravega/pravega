package com.emc.nautilus.common.legacy;

import java.nio.ByteBuffer;

import com.emc.nautilus.common.netty.ConnectionFailedException;

public class ReadBufferedConnection implements OldConnection {

	/**
	 * Convenience class wrapping byteBuffer to provide a circular buffer. This
	 * works by maintaining a two byte buffers backed by the same array. The
	 * position of the readBuffer corresponds to the point up to which has been
	 * read. The position of the writeBuffer corresponds to the point up to
	 * which data has been written. Each buffer's limit is either the end or the
	 * buffer or the position of the other, depending on where the data has
	 * wrapped.
	 */
	private static class CircularBuffer {

		private final ByteBuffer readBuffer;
		private final ByteBuffer fillBuffer;

		CircularBuffer(int capacity) {
			byte[] buffer = new byte[capacity];
			readBuffer = ByteBuffer.wrap(buffer);
			fillBuffer = ByteBuffer.wrap(buffer);
			clear();
		}

		void clear() {
			readBuffer.position(0).limit(0);
			fillBuffer.position(0).limit(fillBuffer.capacity());
		}

		void read(ByteBuffer toFill) {
			readHelper(toFill);
			if (readBuffer.hasRemaining() && toFill.hasRemaining()) {
				readHelper(toFill);
			}
		}

		private void readHelper(ByteBuffer toFill) {
			int toRead = Math.min(toFill.remaining(), readBuffer.remaining());
			readBuffer.limit(toRead);
			toFill.put(readBuffer);
			if (readBuffer.position() <= fillBuffer.position()) {
				readBuffer.limit(fillBuffer.position());
			} else {
				readBuffer.limit(readBuffer.capacity());
			}
		}

		void fill(OldConnection fillFrom) throws ConnectionFailedException {
			fillHelper(fillFrom);
			if (fillBuffer.hasRemaining() && fillFrom.dataAvailable() > 0) {
				fillHelper(fillFrom);
			}
		}

		private void fillHelper(OldConnection fillFrom) throws ConnectionFailedException {
			int toAdd = Math.min(fillFrom.dataAvailable(), fillBuffer.remaining());
			fillBuffer.limit(toAdd);
			fillFrom.read(fillBuffer);
			if (fillBuffer.position() <= readBuffer.position()) {
				fillBuffer.limit(readBuffer.position());
			} else {
				fillBuffer.limit(fillBuffer.capacity());
			}
		}

		/**
		 * @return the number of bytes that can be read
		 */
		int remaining() {
			if (readBuffer.position() <= fillBuffer.position()) {
				return fillBuffer.position() - readBuffer.position();
			} else {
				return readBuffer.capacity() - readBuffer.position() + fillBuffer.position();
			}
		}
	}

	private final Object lock = new Object();
	private final OldConnection connection;
	private final CircularBuffer buffer;
	private DataAvailableCallback dataCallback;

	private class DataListener implements DataAvailableCallback {
		@Override
		public void readPossible() {
			synchronized (lock) {

				try {
					buffer.fill(connection);
				} catch (ConnectionFailedException e) {
					drop();
				}
			}
			if (dataCallback != null) {
				dataCallback.readPossible();
			}
		}
	}

	public ReadBufferedConnection(OldConnection connection, int buffersize) {
		this.connection = connection;
		this.buffer = new CircularBuffer(buffersize);
		connection.setDataAvailableCallback(new DataListener());
	}

	@Override
	public int dataAvailable() {
		synchronized (lock) {
			return buffer.remaining() + connection.dataAvailable();
		}
	}

	@Override
	public int capacityAvailable() {
		synchronized (lock) {
			return connection.capacityAvailable();
		}
	}

	@Override
	public void write(ByteBuffer writeBuffer) throws ConnectionFailedException {
		synchronized (lock) {
			connection.write(writeBuffer);
		}
	}

	@Override
	public void read(ByteBuffer readBuffer) throws ConnectionFailedException {
		synchronized (lock) {
			ByteBuffer rb = readBuffer.slice();
			buffer.read(rb);
			if (rb.hasRemaining()) {
				connection.read(rb);
			}
		}
	}

	@Override
	public void drop() {
		synchronized (lock) {
			connection.drop();
		}
	}

	@Override
	public void setCapacityAvailableCallback(CapactyAvailableCallback cb) {
		connection.setCapacityAvailableCallback(cb);
	}

	@Override
	public void setDataAvailableCallback(DataAvailableCallback cb) {
		if (dataCallback != null) {
			throw new IllegalArgumentException("Callback already set");
		}
		dataCallback = cb;
	}

}
