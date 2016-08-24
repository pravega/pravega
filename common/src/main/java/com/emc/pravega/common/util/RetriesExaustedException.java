package com.emc.pravega.common.util;

public class RetriesExaustedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RetriesExaustedException(Exception last) {
        super(last);
    }
}
