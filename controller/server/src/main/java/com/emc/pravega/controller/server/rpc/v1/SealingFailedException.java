package com.emc.pravega.controller.server.rpc.v1;

/**
 * Created by bhargav on 10/16/16.
 */
public class SealingFailedException extends RuntimeException {

    public SealingFailedException(Throwable cause) {
        super(cause);
    }
}
