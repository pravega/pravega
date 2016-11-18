package com.emc.pravega.state.impl;

public class CorruptedStateException extends RuntimeException {

    public CorruptedStateException(String message) {
        super(message);
    }

}
