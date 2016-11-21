package com.emc.pravega.state.impl;

public class CorruptedStateException extends RuntimeException {

    public CorruptedStateException(String message) {
        super(message);
    }

    public CorruptedStateException(String string, Exception e) {
        super(string, e);
    }

}
