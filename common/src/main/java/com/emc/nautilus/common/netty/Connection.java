package com.emc.nautilus.common.netty;

public interface Connection {

	/**
	 * Sends the provided command. This operation may block. (Though buffering
	 * is used to try to prevent it)
	 * 
	 * @throws ConnectionFailedException The connection has died, and can no
	 *             longer be used.
	 */
	void send(WireCommand cmd) throws ConnectionFailedException;

	/**
	 * Send the provided command. This operation is guarenteed not to block.
	 */
	void asyncSend(WireCommand cmd);

	/**
	 * @param cp Sets the command processor to receive incoming commands from
	 *            the other side of the connection. This method may only be
	 *            called once.
	 */
	void setCommandProcessor(CommandProcessor cp);

	void pauseReading();
	
	void resumeReading();
	
	/**
	 * Drop the connection. No further operations may be performed.
	 */
	void drop();

}
