package com.emc.nautilus.demo;

import com.emc.logservice.serverhost.handler.LogSerivceConnectionListener;

import lombok.Cleanup;

public class StartServer {
	
	public static final int PORT = 12345;
	

	public static void main(String[] args) throws InterruptedException {
		@Cleanup("shutdown")
		LogSerivceConnectionListener server = new LogSerivceConnectionListener(false, PORT, "My ContainerId");
		server.startListening();
		Thread.sleep(60000);
	}

}
