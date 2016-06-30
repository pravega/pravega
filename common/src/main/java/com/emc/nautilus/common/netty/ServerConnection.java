package com.emc.nautilus.common.netty;

public interface ServerConnection {

	/**
	 * Sends the provided command. This operation may block. (Though buffering
	 * is used to try to prevent it)
	 */
	void send(WireCommand cmd);

	/**
	 * @param cp Sets the command processor to receive incoming commands from
	 *            the client. This method may only be called once.
	 */
	void setRequestProcessor(RequestProcessor cp);

	void pauseReading();

	void resumeReading();

	/**
	 * Drop the connection. No further operations may be performed.
	 */
	void drop();

}
