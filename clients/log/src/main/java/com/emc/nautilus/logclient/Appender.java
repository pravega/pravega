/**
*/
package com.emc.nautilus.logclient;

//Defines methods for an Append-only entity.
public interface Appender extends AutoCloseable {
	// Gets a LogOutputStream that can be used to append to the log.
	LogOutputStream getOutputStream();
}
