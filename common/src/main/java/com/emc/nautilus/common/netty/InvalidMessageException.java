package com.emc.nautilus.common.netty;

public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException() {
        super();
    }

    public InvalidMessageException(String string) {
        super(string);
    }

    public InvalidMessageException(Throwable throwable) {
        super(throwable);
    }
}
