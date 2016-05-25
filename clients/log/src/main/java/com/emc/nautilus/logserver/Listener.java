package com.emc.nautilus.logserver;

import java.nio.channels.Selector;
import java.util.concurrent.ThreadPoolExecutor;

public class Listener {

	ThreadPoolExecutor executor;
	Selector selector;
	RequestProcessor requestProcessor;
	
	void run() {
		Socket s = grabSocket(selector.select());
		
		if (s != null) {
			Request r = s.parseRequest();
			requestProcessor.process(r);
		}
	}
}
//TODO: Frame reader as a chunk requestor?
//TODO: Figure out how to modify selector, so that incomplete writes don't trigger it.
//TODO: Figure out a mode that allows for batching of writes.
//TODO: Can writes be batched on a cross client basis?

//Writes pass into a buffer (to assemble blocks).
//When there is a whole record available, it should be written setting up a callback to ack it.
//There can only be one write outstanding at a time, so this provides some opportunistic batching.
//If the buffer is full and there is a write outstanding, we can set a bit so that when the callback completes it pulls more.

//Sockets need to be acquired so that there is only one thing working on them at a time. This can be managed by adding and removing them from the selector.

//Reads pull from a buffer on available socket capacity. If the buffer is low a request is issued (which may be long lived) to request more. 
//Only one such request should exist at once.

//Other requests, execute and setup callback for reply.


//Incoming connection:
//Contains buffer for incoming requests
//Thread moves data to buffer from socket
//Thread wakes on:
//socket transitions to having data from not
//Buffer full to not full.
//Upon assembly of a full request, the request is invoked.
//Only one request may be invoked at a time.
//Callbacks are setup for replies.
//After invoking the callback the buffer should be checked for another request. (In case one is waiting)


//Outgoing connection
//Buffer for replies / data to be returned
//Thread moves data from buffer to wire.
//Thread wakes on:
//Buffer transitions to non-empty
//socket transitions to having capacity
