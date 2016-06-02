package com.emc.nautilus.common.netty;

public interface ClientConnection {

	/**
	 * Sends the provided command. This operation may block. (Though buffering
	 * is used to try to prevent it)
	 * 
	 * @throws ConnectionFailedException The connection has died, and can no
	 *             longer be used.
	 */
	void send(WireCommand cmd); //TODO: Should throw ConnectionFailedException

	/**
	 * @param cp Sets the command processor to receive incoming replies from
	 *            the server. This method may only be
	 *            called once.
	 */
	void setResponseProcessor(ReplyProcessor cp);
	
	/**
	 * Drop the connection. No further operations may be performed.
	 */
	void drop();

	boolean isConnected();

}
