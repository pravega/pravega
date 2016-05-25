package com.emc.nautilus.common.legacy;

import java.nio.ByteBuffer;

import com.emc.nautilus.common.netty.ConnectionFailedException;

public interface OldConnection {

	@FunctionalInterface
	interface DataAvailableCallback {
		void readPossible();
	}

	@FunctionalInterface
	interface CapactyAvailableCallback {
		void writePossible();
	}

	/**
	 * @param cb callback to be invoked when there is capacity available to
	 *            write data. NOTE: the callback might be invoked unnecessarily,
	 *            so check the capacityAvailable. This method may only be called
	 *            once.
	 */
	void setCapacityAvailableCallback(CapactyAvailableCallback cb);

	/**
	 * @param cb callback to be invoked when there is data available to be read.
	 *            NOTE: the callback might be invoked unnecessarily, so check
	 *            the dataAvailable. This method may only be called once.
	 */
	void setDataAvailableCallback(DataAvailableCallback cb);

	/**
	 * @return the number of bytes that can be read without blocking
	 */
	int dataAvailable();

	/**
	 * @return The number of bytes that can be written without blocking
	 */
	int capacityAvailable();

	/**
	 * @param buffer the data to be written. All remaining data in the buffer
	 *            will be written. The position and the limit of the provided
	 *            buffer will be unchanged. If the buffer is larger than
	 *            capacityAvailable this operation will block.
	 * @throws ConnectionFailedException The connection has died, and can no
	 *             longer be used.
	 */
	void write(ByteBuffer buffer) throws ConnectionFailedException;

	/**
	 * @param buffer The buffer to read the data into. The buffer will be filled
	 *            from the current position to the current limit. The position
	 *            and the limit of the provided buffer will be unchanged. If the
	 *            buffer has more remaining than the dataAvailable this
	 *            operation will block.
	 * @throws ConnectionFailedException The connection has died, and can no
	 *             longer be used.
	 */
	void read(ByteBuffer buffer) throws ConnectionFailedException;

	/**
	 * Drop the connection. No further operations may be performed.
	 */
	void drop();
}
